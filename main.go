package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
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

	tables := []string{"timezones", "billing_account", "team", "users", "roles", "master_encryption_keys", "license_table", "tenant_encryption_keys", "master_plan_table", "tenant_plan_table", "license_store_table"}
	for _, table := range tables {
		log.Printf("Starting migration for table: %s", table)
		err := migrateTable(sourceDB, destDB, table)
		if err != nil {
			log.Fatalf("Failed to migrate table %s: %v", table, err)
		}
		log.Printf("Successfully migrated table: %s", table)
	}
}

func migrateTable(sourceDB, destDB *sql.DB, tableName string) error {
	log.Printf("Retrieving schema for table: %s", tableName)

	schema, err := getTableSchema(sourceDB, tableName)
	if err != nil {
		return fmt.Errorf("error getting schema for table %s: %v", tableName, err)
	}

	log.Printf("Creating table %s in destination database with schema: %s", tableName, schema)
	_, err = destDB.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, schema))
	if err != nil {
		return fmt.Errorf("error creating table %s in destination database: %v", tableName, err)
	}

	log.Printf("Migrating data for table: %s", tableName)
	rows, err := sourceDB.Query(fmt.Sprintf("SELECT * FROM %s", tableName))
	if err != nil {
		return fmt.Errorf("error querying data from table %s: %v", tableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("error retrieving columns from table %s: %v", tableName, err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, joinColumns(columns), placeholders(len(columns)))
	for rows.Next() {
		err = rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("error scanning data from table %s: %v", tableName, err)
		}

		_, err = destDB.Exec(insertStmt, values...)
		if err != nil {
			return fmt.Errorf("error inserting data into table %s: %v", tableName, err)
		}
	}

	return nil
}

func getTableSchema(db *sql.DB, tableName string) (string, error) {
	// Retrieve column definitions
	columns, err := getColumnDefinitions(db, tableName)
	if err != nil {
		return "", err
	}

	// Check if the table is 'roles' and modify the schema accordingly
	if tableName == "roles" {
		additionalColumns := ", `created_by` varchar(255), `updated_by` varchar(255), `type` enum('BILLING', 'STANDARD', 'CUSTOM') NOT NULL DEFAULT 'STANDARD', `team_id` char(36)"
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
