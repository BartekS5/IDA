package etl

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/BartekS5/IDA/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type SQLToMongoExtractor struct {
	DB     *sql.DB
	Config *models.MappingSchema
}

func (s *SQLToMongoExtractor) Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error) {
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

	if err := s.enrichRelations(results, ids); err != nil {
		return nil, nil, err
	}

	transformed := s.transformToMongoSchema(results)

	nextOffset := getIntOffset(offset) + len(results)
	return transformed, nextOffset, nil
}

func (s *SQLToMongoExtractor) enrichRelations(users []map[string]interface{}, ids []interface{}) error {
	if len(ids) == 0 {
		return nil
	}
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
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			relRows.Scan(ptrs...)

			rowMap := make(map[string]interface{})
			var parentID string
			for i, col := range relCols {
				val := vals[i]
				b, ok := val.([]byte)
				v := val
				if ok {
					v = string(b)
				}

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

type SQLLoader struct {
	DB     *sql.DB
	Config *models.MappingSchema
}

func (l *SQLLoader) Load(data []map[string]interface{}) error {
	fmt.Printf("SQL Loader: Processing %d records...\n", len(data))

	for _, doc := range data {
		idVal, ok := doc[l.Config.IDStrategy.MongoField]
		if !ok {
			fmt.Println("Warning: Skipping document missing ID")
			continue
		}

		colValues := make(map[string]interface{})
		for _, f := range l.Config.Fields {
			if val, exists := doc[f.MongoField]; exists {
				if f.Type == "datetime" {
					if t, ok := val.(primitive.DateTime); ok {
						val = t.Time()
					}
				}
				colValues[f.SQLColumn] = val
			}
		}

		var exists int
		checkQuery := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = @p1", l.Config.SQLTable, l.Config.IDStrategy.SQLField)
		err := l.DB.QueryRow(checkQuery, idVal).Scan(&exists)

		if err == sql.ErrNoRows {
			l.insertRow(colValues, idVal)
		} else {
			l.updateRow(colValues, idVal)
		}

		if err := l.syncRelations(doc, idVal); err != nil {
			fmt.Printf("Error syncing relations for ID %v: %v\n", idVal, err)
		}
	}
	return nil
}

func (l *SQLLoader) insertRow(cols map[string]interface{}, idVal interface{}) {
	tx, err := l.DB.Begin()
	if err != nil {
		fmt.Printf("Error starting transaction: %v\n", err)
		return
	}
	defer tx.Rollback()

	var colNames []string
	var placeholders []string
	var args []interface{}
	for col, val := range cols {
		colNames = append(colNames, col)
		placeholders = append(placeholders, fmt.Sprintf("@p%d", len(args)+1))
		args = append(args, val)
	}

	colNames = append(colNames, l.Config.IDStrategy.SQLField)
	placeholders = append(placeholders, fmt.Sprintf("@p%d", len(args)+1))
	args = append(args, idVal)

	_, err = tx.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s ON", l.Config.SQLTable))
	if err != nil {
		fmt.Printf("Error enabling identity insert: %v\n", err)
		return
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		l.Config.SQLTable, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	if _, err := tx.Exec(query, args...); err != nil {
		fmt.Printf("Error inserting row: %v\n", err)
		return
	}

	_, _ = tx.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s OFF", l.Config.SQLTable))
	if err := tx.Commit(); err != nil {
		fmt.Printf("Error committing transaction: %v\n", err)
	}
}

func (l *SQLLoader) updateRow(cols map[string]interface{}, idVal interface{}) {
	var setClauses []string
	var args []interface{}

	for col, val := range cols {
		setClauses = append(setClauses, fmt.Sprintf("%s = @p%d", col, len(args)+1))
		args = append(args, val)
	}

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
	if o == nil {
		return 0
	}
	switch v := o.(type) {
	case int:
		return v
	case float64:
		return int(v)
	default:
		return 0
	}
}

func (l *SQLLoader) syncRelations(doc map[string]interface{}, parentID interface{}) error {
	for _, relConfig := range l.Config.Relations {
		rawData, ok := doc[relConfig.MongoField]
		if !ok || rawData == nil {
			continue
		}

		var items []map[string]interface{}

		extractItems := func(slice []interface{}) {
			for _, item := range slice {
				if m, ok := item.(map[string]interface{}); ok {
					items = append(items, m)
				} else if m, ok := item.(primitive.M); ok {
					items = append(items, map[string]interface{}(m))
				} else if d, ok := item.(primitive.D); ok {
					items = append(items, d.Map())
				}
			}
		}

		switch v := rawData.(type) {
		case primitive.A:
			extractItems([]interface{}(v))
		case []interface{}:
			extractItems(v)
		}

		if relConfig.Type == "many-to-many" {
			l.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = @p1", relConfig.SQLJoinTable, relConfig.SQLForeignKey), parentID)

			for _, item := range items {
				var roleID int64
				nameVal := item["name"]
				err := l.DB.QueryRow("SELECT id FROM roles WHERE name = @p1", nameVal).Scan(&roleID)
				if err == nil {
					l.DB.Exec(fmt.Sprintf("INSERT INTO %s (%s, role_id) VALUES (@p1, @p2)", relConfig.SQLJoinTable, relConfig.SQLForeignKey), parentID, roleID)
				}
			}
		}

		if relConfig.Type == "one-to-many" {
			l.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = @p1", relConfig.SQLTable, relConfig.SQLForeignKey), parentID)

			for _, item := range items {
				var cols []string
				var vals []interface{}
				var placeholders []string

				cols = append(cols, relConfig.SQLForeignKey)
				vals = append(vals, parentID)
				placeholders = append(placeholders, fmt.Sprintf("@p%d", len(vals)))

				for k, v := range item {
					if k == "_id" || k == "id" || k == relConfig.SQLForeignKey {
						continue
					}

					if t, ok := v.(primitive.DateTime); ok {
						v = t.Time()
					}

					cols = append(cols, k)
					vals = append(vals, v)
					placeholders = append(placeholders, fmt.Sprintf("@p%d", len(vals)))
				}

				query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
					relConfig.SQLTable, strings.Join(cols, ", "), strings.Join(placeholders, ", "))

				if _, err := l.DB.Exec(query, vals...); err != nil {
					fmt.Printf("Warning: Failed to insert child record: %v\n", err)
				}
			}
		}
	}
	return nil
}
