package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	sourceDB, err := sql.Open("mysql", os.Getenv("SOURCE_DB_URL"))
	if err != nil {
		log.Fatalf("Could not connect to source database: %v", err)
	}
	defer sourceDB.Close()

	destDB, err := sql.Open("mysql", os.Getenv("DEST_DB_URL"))
	if err != nil {
		log.Fatalf("Could not connect to destination database: %v", err)
	}
	defer destDB.Close()

	tables := []string{"timezones", "admins", "billing_account", "team", "users", "roles", "master_encryption_keys", "license_table", "tenant_encryption_keys", "master_plan_table", "tenant_plan_table", "license_store_table", "app_groups", "apps", "audit_logs"}
	for _, table := range tables {
		log.Printf("Starting migration for table: %s", table) //add logs for each table row, check source and destination rows count pre and post migration
		err := migrateTable(sourceDB, destDB, table)
		if err != nil {
			log.Fatalf("Failed to migrate table %s: %v", table, err)
		}
		log.Printf("Successfully migrated table: %s", table)
		log.Println(" ")
	}

	log.Println("Starting to insert specific roles for each team...")
	if err := insertRolesForTeams(destDB); err != nil {
		log.Fatalf("Failed to insert roles for teams: %v", err)
	}
	log.Println("Successfully inserted roles for all teams.")
	log.Println(" ")

	// if err := fetchAndDisplayUserRoles(sourceDB); err != nil {
	// 	log.Fatalf("Failed to fetch user roles information: %v", err)
	// }

	if err := ensureUserRolesMappingTableExists(destDB); err != nil {
		log.Fatalf("Failed to ensure user_roles_mapping table exists: %v", err)
	}

	log.Println("Fetching user roles information from source database...")
	if err := fetchAndInsertUserRoles(sourceDB, destDB); err != nil {
		log.Fatalf("Failed to fetch and insert user roles information: %v", err)
	}
}

func migrateTable(sourceDB, destDB *sql.DB, tableName string) error {
	log.Printf("Retrieving schema for table: %s", tableName)
	destinationTableName := tableName

	schema, err := getTableSchema(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("error getting schema for table %s: %v", tableName, err)
	}

	if tableName == "apps" || tableName == "app_groups" {
		additionalColumns := ", `created_by` varchar(255), `updated_by` varchar(255)"
		schema += additionalColumns
	}

	if tableName == "app_groups" {
		schema += ", `team_id` CHAR(36)"
	}

	if tableName == "apps" {
		// Rename 'key' to 'key_value' and 'label' to 'label_value'
		schema = strings.Replace(schema, "`key`", "`key_value`", 1)
		schema = strings.Replace(schema, "`label`", "`label_value`", 1)
	}

	if tableName == "audit_logs" {
		destinationTableName = "audit_log"
		schema = "`id` bigint NOT NULL AUTO_INCREMENT," +
			"`entity_id` varchar(255) NOT NULL," +
			"`modified_date` datetime(6) NOT NULL," +
			"`new_value` longtext," +
			"`old_value` longtext," +
			"`actor` varchar(255) NOT NULL," +
			"`actor_type` varchar(255) NOT NULL," +
			"`entity_info` varchar(255) DEFAULT NULL," +
			"`entity_type` varchar(255) NOT NULL," +
			"`operation` enum('ADD','DELETE','UPDATE') NOT NULL," +
			"PRIMARY KEY (`id`)"
	}

	_, err = destDB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", destinationTableName, schema))
	if err != nil {
		return fmt.Errorf("error creating table %s in destination database: %v", tableName, err)
	}

	log.Printf("Migrating data for table: %s", tableName)
	var query string
	if tableName == "apps" {
		query = "SELECT id, `key` AS key_value, label AS label_value, group_id, created_at, updated_at FROM apps"
	} else if tableName == "audit_logs" {
		query = "SELECT a.email_id AS actor, al.action AS operation, al.target AS entity_type, 'ADMIN' AS actor_type, al.target_id AS entity_id, al.created_at AS modified_date, al.target_info AS entity_info FROM audit_logs al LEFT JOIN admins a ON a.id = al.admin_id"
	} else if tableName == "app_groups" {
		query = `SELECT ag.id, ag.name, ag.user_id, ag.created_at, ag.updated_at, utm.team_id FROM app_groups ag LEFT JOIN user_team_mapping utm ON ag.user_id = utm.user_id`
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", tableName)
	}
	rows, err := sourceDB.Query(query)
	if err != nil {
		return fmt.Errorf("error querying data from table %s: %v", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error retrieving columns from table %s: %v", tableName, err)
	}

	if tableName == "apps" {
		for i, col := range columns {
			if col == "key" {
				columns[i] = "key_value"
			} else if col == "label" {
				columns[i] = "label_value"
			}
		}
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", destinationTableName, joinColumns(columns), placeholders(len(columns)))
	sourceCount := 0
	insertCount := 0
	for rows.Next() {
		sourceCount++
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("error scanning data from table %s: %v", tableName, err)
		}

		_, err = destDB.Exec(insertStmt, values...)
		if err != nil {
			return fmt.Errorf("error inserting data into table %s: %v", tableName, err)
		}
		insertCount++
	}

	log.Printf("Migrated %d records from source table %s.", sourceCount, tableName)
	log.Printf("Inserted %d records into destination table %s.", insertCount, tableName)

	return nil
}

func insertRolesForTeams(db *sql.DB) error {
	if _, err := db.Exec("DELETE FROM roles"); err != nil {
		return fmt.Errorf("error clearing roles table: %v", err)
	}

	log.Println("Fetching all team IDs from the team table...")
	rows, err := db.Query("SELECT id, billing_id FROM team")
	if err != nil {
		return fmt.Errorf("error fetching team ids: %v", err)
	}
	defer rows.Close()

	log.Println("Preparing statement for inserting roles...")
	stmt, err := db.Prepare(`INSERT INTO roles (id, name, type, team_id, billing_id) VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("error preparing insert statement: %v", err)
	}
	defer stmt.Close()

	var count int
	for rows.Next() {
		var teamId string
		var billingId string
		if err := rows.Scan(&teamId, &billingId); err != nil {
			return fmt.Errorf("error scanning team id: %v", err)
		}

		log.Printf("Inserting roles for team ID: %s\n", teamId)
		if err := insertRole(stmt, "BI_ADMIN", "BILLING", nil, &billingId); err != nil {
			return err
		}
		if err := insertRole(stmt, "PLATFORM_ADMIN", "STANDARD", &teamId, &billingId); err != nil {
			return err
		}
		if err := insertRole(stmt, "PLATFORM_READ_ONLY", "STANDARD", &teamId, &billingId); err != nil {
			return err
		}
		count += 3
	}

	log.Printf("Inserted a total of %d roles for all teams.\n", count)
	return nil
}

func insertRole(stmt *sql.Stmt, name string, roleType string, teamId *string, billingId *string) error {
	newUUID, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("error generating UUID: %v", err)
	}

	var result sql.Result
	if teamId == nil {
		result, err = stmt.Exec(newUUID.String(), name, roleType, nil, *billingId)
	} else {
		result, err = stmt.Exec(newUUID.String(), name, roleType, *teamId, *billingId)
	}

	if err != nil {
		return fmt.Errorf("error inserting role: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	teamIdValue := "<nil>"
	if teamId != nil {
		teamIdValue = *teamId
	}
	log.Printf("Inserted role: %s, Type: %s, Team ID: %v, Rows affected: %d\n", name, roleType, teamIdValue, rowsAffected)
	return nil
}

func ensureUserRolesMappingTableExists(db *sql.DB) error {
	createTableQuery := `
    CREATE TABLE IF NOT EXISTS user_roles_mapping (
        user_id CHAR(36) NOT NULL,
        role_id CHAR(36) NOT NULL,
        PRIMARY KEY (user_id, role_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (role_id) REFERENCES roles(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
	_, err := db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("error creating user_roles_mapping table: %v", err)
	}
	log.Println("Ensured user_roles_mapping table exists.")
	return nil
}

func fetchAndDisplayUserRoles(db *sql.DB) error {
	rows, err := db.Query("SELECT id FROM billing_account")
	if err != nil {
		return fmt.Errorf("error fetching billing_account ids: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var billingId string
		if err := rows.Scan(&billingId); err != nil {
			return fmt.Errorf("error scanning billing_account id: %v", err)
		}

		// For each billing_id, get all associated user_ids from users table
		userRows, err := db.Query("SELECT id FROM users WHERE billing_id = ?", billingId)
		if err != nil {
			return fmt.Errorf("error fetching user ids for billing_id %s: %v", billingId, err)
		}
		defer userRows.Close()

		for userRows.Next() {
			var userId string
			if err := userRows.Scan(&userId); err != nil {
				return fmt.Errorf("error scanning user id: %v", err)
			}

			// For each user_id, get role_id and team_id from users_role table
			roleRows, err := db.Query("SELECT role_id, team_id FROM users_role WHERE user_id = ?", userId)
			if err != nil {
				return fmt.Errorf("error fetching role and team ids for user_id %s: %v", userId, err)
			}
			defer roleRows.Close()

			for roleRows.Next() {
				var roleId, teamId string
				if err := roleRows.Scan(&roleId, &teamId); err != nil {
					return fmt.Errorf("error scanning role and team ids: %v", err)
				}

				// Get the name associated with that role_id from roles table
				var roleName string
				err = db.QueryRow("SELECT name FROM roles WHERE id = ?", roleId).Scan(&roleName)
				if err != nil {
					return fmt.Errorf("error fetching role name for role_id %s: %v", roleId, err)
				}

				// Print the fetched information
				fmt.Printf("Billing ID: %s, User ID: %s, Role ID: %s, Role Name: %s, Team ID: %s\n", billingId, userId, roleId, roleName, teamId)
			}
		}
	}

	return nil
}

func fetchAndInsertUserRoles(sourceDB, destDB *sql.DB) error {
	query := `SELECT u.id AS user_id, ba.id AS billing_id, utm.team_id, r.name AS role_name
              FROM users u
              JOIN billing_account ba ON u.billing_id = ba.id
              JOIN users_role ur ON u.id = ur.user_id
              JOIN roles r ON ur.role_id = r.id
              JOIN user_team_mapping utm ON u.id = utm.user_id`
	rows, err := sourceDB.Query(query)
	if err != nil {
		return fmt.Errorf("error fetching user roles data: %v", err)
	}
	defer rows.Close()

	// log.Println("Preparing statement for inserting into user_roles_mapping in destDB...")
	insertStmt, err := destDB.Prepare(`INSERT IGNORE INTO user_roles_mapping (user_id, role_id) VALUES (?, ?)`)
	if err != nil {
		return fmt.Errorf("error preparing insert statement: %v", err)
	}
	defer insertStmt.Close()

	for rows.Next() {
		var userId, billingId, teamId, roleName string
		if err := rows.Scan(&userId, &billingId, &teamId, &roleName); err != nil {
			return fmt.Errorf("error scanning user roles data: %v", err)
		}

		roleName = transformRoleName(roleName)

		var roleId string
		roleQuery := `SELECT id FROM roles WHERE name = ? AND billing_id = ?`
		if roleName == "BI_ADMIN" {
			err = destDB.QueryRow(roleQuery, roleName, billingId).Scan(&roleId)
		} else {
			roleQuery += " AND team_id = ?"
			err = destDB.QueryRow(roleQuery, roleName, billingId, teamId).Scan(&roleId)
		}
		if err != nil {
			if err == sql.ErrNoRows {
				log.Printf("No role found for Role Name: %s, Billing ID: %s, Team ID: %s. Skipping insertion.", roleName, billingId, teamId)
				continue
			} else {
				return fmt.Errorf("error fetching role id for role name %s with billing_id %s and team_id %s: %v", roleName, billingId, teamId, err)
			}
		}

		log.Printf("Inserting into user_roles_mapping: UserID: %s, RoleID: %s", userId, roleId)
		if _, err := insertStmt.Exec(userId, roleId); err != nil {
			return fmt.Errorf("error inserting into user_roles_mapping: %v", err)
		}
	}

	log.Println("Successfully fetched and inserted user roles information.")
	return nil
}

func transformRoleName(sourceRoleName string) string {
	switch sourceRoleName {
	case "USER":
		return "PLATFORM_READ_ONLY"
	case "TEAM_ADMIN":
		return "PLATFORM_ADMIN"
	default:
		return sourceRoleName
	}
}
func getTableSchema(db *sql.DB, tableName string) (string, error) {
	// Retrieve column definitions
	columns, err := getColumnDefinitions(db, tableName)
	if err != nil {
		return "", err
	}

	// Check if the table is 'roles' and modify the schema accordingly
	if tableName == "roles" {
		additionalColumns := ", `created_by` varchar(255), `updated_by` varchar(255), `type` enum('BILLING', 'STANDARD', 'CUSTOM') NOT NULL DEFAULT 'STANDARD', `team_id` char(36), `billing_id` char(36)"
		columns += additionalColumns
	}

	// Retrieve primary key
	primaryKey, err := getPrimaryKey(db, tableName)
	if err != nil {
		return "", err
	}

	// Retrieve unique keys
	uniqueKeys, err := getUniqueKeys(db, tableName)
	if err != nil {
		return "", err
	}

	// Retrieve indexes
	indexes, err := getIndexes(db, tableName)
	if err != nil {
		return "", err
	}

	// Retrieve foreign keys
	foreignKeys, err := getForeignKeys(db, tableName)
	if err != nil {
		return "", err
	}

	// Add a foreign key for 'roles' table specifically
	if tableName == "roles" {
		foreignKeys = append(foreignKeys, "CONSTRAINT `fk_roles_team_id` FOREIGN KEY (`team_id`) REFERENCES `team` (`id`)")
	}

	if tableName == "app_groups" {
		foreignKeys = append(foreignKeys, "CONSTRAINT `fk_app_groups_team_id` FOREIGN KEY (`team_id`) REFERENCES `team` (`id`)")
	}

	// Construct the full table schema
	schemaParts := []string{columns}
	if primaryKey != "" {
		schemaParts = append(schemaParts, fmt.Sprintf("PRIMARY KEY (%s)", primaryKey))
	}
	schemaParts = append(schemaParts, uniqueKeys...)
	schemaParts = append(schemaParts, indexes...)
	schemaParts = append(schemaParts, foreignKeys...)

	return strings.Join(schemaParts, ", "), nil
}

func getColumnDefinitions(db *sql.DB, tableName string) (string, error) {
	query := `SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
              FROM INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var (
			name, dataType, isNullable string
			columnDefault, extra       sql.NullString
		)
		if err := rows.Scan(&name, &dataType, &isNullable, &columnDefault, &extra); err != nil {
			return "", err
		}

		column := fmt.Sprintf("`%s` %s", name, dataType)
		if isNullable == "NO" {
			column += " NOT NULL"
		}

		if name == "id" && dataType == "char(36)" {
			// Skip default value for UUID columns to avoid syntax error
			// UUID should be generated by the application or a trigger
		} else if columnDefault.Valid {
			if strings.HasSuffix(columnDefault.String, "()") || columnDefault.String == "CURRENT_TIMESTAMP" {
				column += fmt.Sprintf(" DEFAULT %s", columnDefault.String)
			} else {
				column += fmt.Sprintf(" DEFAULT '%s'", columnDefault.String)
			}
		}

		if extra.Valid && !strings.Contains(extra.String, "GENERATED") {
			column += " " + extra.String
		}

		columns = append(columns, column)
	}

	return strings.Join(columns, ", "), nil
}

func getPrimaryKey(db *sql.DB, tableName string) (string, error) {
	query := `SELECT COLUMN_NAME
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var primaryKeyColumns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return "", err
		}
		primaryKeyColumns = append(primaryKeyColumns, fmt.Sprintf("`%s`", columnName))
	}

	return strings.Join(primaryKeyColumns, ", "), nil
}

func getUniqueKeys(db *sql.DB, tableName string) ([]string, error) {
	query := `SELECT tc.CONSTRAINT_NAME, GROUP_CONCAT(kcu.COLUMN_NAME ORDER BY kcu.ORDINAL_POSITION)
              FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
              JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
              WHERE tc.TABLE_SCHEMA = DATABASE() AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'UNIQUE'
              GROUP BY tc.CONSTRAINT_NAME`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uniqueKeys []string
	for rows.Next() {
		var constraintName, columnNames string
		if err := rows.Scan(&constraintName, &columnNames); err != nil {
			return nil, err
		}
		uniqueKeys = append(uniqueKeys, fmt.Sprintf("UNIQUE KEY `%s` (%s)", constraintName, columnNames))
	}

	return uniqueKeys, nil
}

func getIndexes(db *sql.DB, tableName string) ([]string, error) {
	query := `SELECT s.INDEX_NAME, GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX)
              FROM INFORMATION_SCHEMA.STATISTICS AS s
              WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 1
              GROUP BY s.INDEX_NAME`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []string
	for rows.Next() {
		var indexName, columnNames string
		if err := rows.Scan(&indexName, &columnNames); err != nil {
			return nil, err
		}
		if indexName != "PRIMARY" { // Exclude the primary key index
			indexes = append(indexes, fmt.Sprintf("KEY `%s` (%s)", indexName, columnNames))
		}
	}

	return indexes, nil
}

func getForeignKeys(db *sql.DB, tableName string) ([]string, error) {
	query := `SELECT kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
              WHERE kcu.TABLE_SCHEMA = DATABASE() AND kcu.TABLE_NAME = ? AND kcu.REFERENCED_TABLE_NAME IS NOT NULL`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var foreignKeys []string
	for rows.Next() {
		var constraintName, columnName, referencedTableName, referencedColumnName string
		if err := rows.Scan(&constraintName, &columnName, &referencedTableName, &referencedColumnName); err != nil {
			return nil, err
		}
		foreignKey := fmt.Sprintf("CONSTRAINT `%s` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)", constraintName, columnName, referencedTableName, referencedColumnName)
		foreignKeys = append(foreignKeys, foreignKey)
	}

	return foreignKeys, nil
}

func joinColumns(columns []string) string {
	return strings.Join(columns, ", ")
}

func placeholders(count int) string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = "?"
	}
	return strings.Join(placeholders, ", ")
}
