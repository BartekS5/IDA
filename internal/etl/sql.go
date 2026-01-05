package etl

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/BartekS5/IDA/pkg/models"
)

// SQLToMongoExtractor reads from SQL and builds hierarchical structures.
type SQLToMongoExtractor struct {
	DB     *sql.DB
	Config *models.MappingSchema
}

func (s *SQLToMongoExtractor) Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error) {
	// 1. Fetch main entities
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %v ROWS FETCH NEXT %d ROWS ONLY",
		s.Config.SQLTable, s.Config.IDStrategy.SQLField, getIntOffset(offset), batchSize)
	
	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	var results []map[string]interface{}
	var ids []interface{}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, nil, err
		}

		m := make(map[string]interface{})
		for i, colName := range cols {
			val := columns[i]
			b, ok := val.([]byte)
			if ok {
				m[colName] = string(b)
			} else {
				m[colName] = val
			}
		}
		results = append(results, m)
		
		idVal := m[s.Config.IDStrategy.SQLField]
		ids = append(ids, idVal)
	}

	if len(results) == 0 {
		return results, offset, nil
	}

	// 2. Fetch Relations
	if err := s.enrichRelations(results, ids); err != nil {
		return nil, nil, err
	}

	// 3. Transform
	transformed := s.transformToMongoSchema(results)

	nextOffset := getIntOffset(offset) + len(results)
	return transformed, nextOffset, nil
}

func (s *SQLToMongoExtractor) enrichRelations(users []map[string]interface{}, ids []interface{}) error {
	if len(ids) == 0 { return nil }
	inClause := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids)), ","), "[]")

	for key, rel := range s.Config.Relations {
		var relRows *sql.Rows
		var err error
		var query string

		if rel.Type == "many-to-many" {
			cols := strings.Join(rel.Fields, ", ")
			query = fmt.Sprintf("SELECT ur.%s as parent_id, r.%s FROM %s ur JOIN roles r ON ur.role_id = r.id WHERE ur.%s IN (%s)",
				rel.SQLForeignKey, cols, rel.SQLJoinTable, rel.SQLForeignKey, inClause)
		} else {
			query = fmt.Sprintf("SELECT %s as parent_id, * FROM %s WHERE %s IN (%s)",
				rel.SQLForeignKey, rel.SQLTable, rel.SQLForeignKey, inClause)
		}

		relRows, err = s.DB.Query(query)
		if err != nil {
			return fmt.Errorf("failed to fetch relation %s: %w", key, err)
		}
		defer relRows.Close()

		relMap := make(map[string][]map[string]interface{})
		relCols, _ := relRows.Columns()
		for relRows.Next() {
			vals := make([]interface{}, len(relCols))
			ptrs := make([]interface{}, len(relCols))
			for i := range vals { ptrs[i] = &vals[i] }
			relRows.Scan(ptrs...)
			
			rowMap := make(map[string]interface{})
			var parentID string
			for i, col := range relCols {
				val := vals[i]
				b, ok := val.([]byte)
				v := val
				if ok { v = string(b) }
				
				if col == "parent_id" {
					parentID = fmt.Sprintf("%v", v)
				} else {
					rowMap[col] = v
				}
			}
			relMap[parentID] = append(relMap[parentID], rowMap)
		}

		for _, user := range users {
			uid := fmt.Sprintf("%v", user[s.Config.IDStrategy.SQLField])
			if childData, found := relMap[uid]; found {
				user[key] = childData
			} else {
				user[key] = []map[string]interface{}{}
			}
		}
	}
	return nil
}

func (s *SQLToMongoExtractor) transformToMongoSchema(raw []map[string]interface{}) []map[string]interface{} {
	var out []map[string]interface{}
	for _, r := range raw {
		doc := make(map[string]interface{})
		doc[s.Config.IDStrategy.MongoField] = r[s.Config.IDStrategy.SQLField]

		for _, f := range s.Config.Fields {
			if val, ok := r[f.SQLColumn]; ok {
				if f.Type == "datetime" && f.Format == "ISO8601" {
					if tStr, ok := val.(string); ok {
						val, _ = time.Parse(time.RFC3339, tStr)
					}
				}
				doc[f.MongoField] = val
			}
		}

		for key, rel := range s.Config.Relations {
			if val, ok := r[key]; ok {
				doc[rel.MongoField] = val
			}
		}
		out = append(out, doc)
	}
	return out
}

// SQLLoader inserts/updates data into SQL.
type SQLLoader struct {
	DB     *sql.DB
	Config *models.MappingSchema
}

func (l *SQLLoader) Load(data []map[string]interface{}) error {
	fmt.Printf("SQL Loader: Processing %d records...\n", len(data))

	for _, doc := range data {
		// 1. Identify PK
		// The Config.IDStrategy.MongoField (e.g., "_id") holds the value for Config.IDStrategy.SQLField (e.g., "id")
		idVal, ok := doc[l.Config.IDStrategy.MongoField]
		if !ok {
			fmt.Println("Warning: Skipping document missing ID")
			continue
		}

		// 2. Map fields back to SQL columns
		colValues := make(map[string]interface{})
		for _, f := range l.Config.Fields {
			if val, exists := doc[f.MongoField]; exists {
				// Simple type handling (dates need formatting in real apps)
				colValues[f.SQLColumn] = val
			}
		}

		// 3. Check if Row Exists
		var exists int
		checkQuery := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = @p1", l.Config.SQLTable, l.Config.IDStrategy.SQLField)
		err := l.DB.QueryRow(checkQuery, idVal).Scan(&exists)

		if err == sql.ErrNoRows {
			// INSERT
			l.insertRow(colValues, idVal)
		} else if err == nil {
			// UPDATE
			l.updateRow(colValues, idVal)
		} else {
			return fmt.Errorf("error checking row existence: %w", err)
		}
	}
	return nil
}

func (l *SQLLoader) insertRow(cols map[string]interface{}, idVal interface{}) {
	var colNames []string
	var placeholders []string
	var args []interface{}

	// Add Identity Insert logic if needed, but usually we let DB handle ID or force it if Identity Insert is ON.
	// For simplicity, we assume we insert other fields and let DB generate ID or update existing.
	// NOTE: If we want to restore the EXACT ID from Mongo, we need IDENTITY_INSERT ON.
	// Here we try to update existing rows mainly.
	
	// Add PK to cols if we want to force it (requires SET IDENTITY_INSERT ON in MSSQL)
	// For this demo, let's focus on updating the existing 'john_doe'.
	
	for col, val := range cols {
		colNames = append(colNames, col)
		placeholders = append(placeholders, fmt.Sprintf("@p%d", len(args)+1))
		args = append(args, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", 
		l.Config.SQLTable, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	if _, err := l.DB.Exec(query, args...); err != nil {
		fmt.Printf("Error inserting: %v\n", err)
	}
}

func (l *SQLLoader) updateRow(cols map[string]interface{}, idVal interface{}) {
	var setClauses []string
	var args []interface{}

	for col, val := range cols {
		setClauses = append(setClauses, fmt.Sprintf("%s = @p%d", col, len(args)+1))
		args = append(args, val)
	}

	// Add ID as the last argument for WHERE clause
	args = append(args, idVal)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = @p%d",
		l.Config.SQLTable, strings.Join(setClauses, ", "), l.Config.IDStrategy.SQLField, len(args))

	if _, err := l.DB.Exec(query, args...); err != nil {
		fmt.Printf("Error updating: %v\n", err)
	} else {
		fmt.Printf("Updated record ID: %v\n", idVal)
	}
}

func getIntOffset(o interface{}) int {
	if o == nil { return 0 }
	switch v := o.(type) {
	case int: return v
	case float64: return int(v)
	default: return 0
	}
}
