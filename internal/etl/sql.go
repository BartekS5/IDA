package etl

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/BartekS5/IDA/pkg/logger"
	"github.com/BartekS5/IDA/pkg/models"
	"github.com/BartekS5/IDA/pkg/utils" // Imported utils
)

type SQLToMongoExtractor struct {
	DB     *sql.DB
	Config *models.MappingSchema
}

func (s *SQLToMongoExtractor) Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error) {
	// Use shared utility function
	currentOffset := utils.GetIntOffset(offset)
	
	// Ensure safe ordering
	orderBy := s.Config.IDStrategy.SQLField
	if orderBy == "" {
		orderBy = "(SELECT NULL)" 
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", 
		s.Config.SQLTable, orderBy, currentOffset, batchSize)
	
	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("SQL query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var result []map[string]interface{}

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
			if b, ok := val.([]byte); ok {
				m[colName] = string(b)
			} else {
				m[colName] = val
			}
		}
		
		s.enrichWithRelations(m)
		result = append(result, m)
	}

	return result, currentOffset + len(result), nil
}

func (s *SQLToMongoExtractor) enrichWithRelations(row map[string]interface{}) {
	idVal, ok := row[s.Config.IDStrategy.SQLField]
	if !ok {
		return
	}

	for relName, relCfg := range s.Config.Relations {
		if relCfg.Type == "one-to-many" && relCfg.SQLTable != "" {
			q := fmt.Sprintf("SELECT * FROM %s WHERE %s = @p1", relCfg.SQLTable, relCfg.SQLForeignKey)
			rows, err := s.DB.Query(q, idVal)
			if err == nil {
				cols, _ := rows.Columns()
				var children []map[string]interface{}
				for rows.Next() {
					vals := make([]interface{}, len(cols))
					ptrs := make([]interface{}, len(cols))
					for i := range vals { ptrs[i] = &vals[i] }
					rows.Scan(ptrs...)
					child := make(map[string]interface{})
					for i, c := range cols { 
						if b, ok := vals[i].([]byte); ok { child[c] = string(b) } else { child[c] = vals[i] }
					}
					children = append(children, child)
				}
				rows.Close()
				row[relName] = children
			}
		}
	}
}

type SQLLoader struct {
	DB          *sql.DB
	Config      *models.MappingSchema
	Transformer *Transformer
}

func NewSQLLoader(db *sql.DB, config *models.MappingSchema) *SQLLoader {
	return &SQLLoader{
		DB:          db,
		Config:      config,
		Transformer: NewTransformer(config),
	}
}

func (l *SQLLoader) Load(data []map[string]interface{}) error {
	logger.Infof("SQL Loader: Processing %d records...", len(data))

	tx, err := l.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	hasIdentity, err := l.hasIdentityColumn(tx, l.Config.SQLTable)
	if err != nil {
		logger.Warnf("Failed to check identity column: %v", err)
	}

	for _, doc := range data {
		sqlRow, err := l.Transformer.TransformMongoToSQL(doc)
		if err != nil {
			logger.Errorf("Transform error: %v", err)
			continue
		}

		idVal := sqlRow[l.Config.IDStrategy.SQLField]
		if idVal == nil {
			continue
		}

		if err := l.upsertRow(tx, l.Config.SQLTable, l.Config.IDStrategy.SQLField, idVal, sqlRow, hasIdentity); err != nil {
			logger.Errorf("Failed to upsert main entity %v: %v", idVal, err)
			continue
		}

		relations := l.Transformer.ExtractRelationData(doc, idVal)
		for relName, rows := range relations {
			relCfg := l.Config.Relations[relName]
			if relCfg.Type == "many-to-many" {
				if err := l.syncJoinTable(tx, relCfg, idVal, rows); err != nil {
					logger.Errorf("Failed to sync M2M relation %s: %v", relName, err)
				}
			} else {
				if err := l.replaceChildren(tx, relCfg, idVal, rows); err != nil {
					logger.Errorf("Failed to replace children for %s: %v", relName, err)
				}
			}
		}
	}

	return tx.Commit()
}

func (l *SQLLoader) hasIdentityColumn(tx *sql.Tx, table string) (bool, error) {
	var exists int
	query := "SELECT 1 FROM sys.identity_columns WHERE object_id = OBJECT_ID(@p1)"
	err := tx.QueryRow(query, table).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (l *SQLLoader) upsertRow(tx *sql.Tx, table string, idCol string, idVal interface{}, cols map[string]interface{}, hasIdentity bool) error {
	var exists int
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE %s = @p1", table, idCol)
	err := tx.QueryRow(query, idVal).Scan(&exists)

	if err == sql.ErrNoRows {
		var colNames, params []string
		var args []interface{}
		i := 1
		for k, v := range cols {
			colNames = append(colNames, k)
			params = append(params, fmt.Sprintf("@p%d", i))
			args = append(args, v)
			i++
		}
		
		insQ := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(colNames, ","), strings.Join(params, ","))
		
		if hasIdentity {
			if _, ok := cols[idCol]; ok {
				_, err = tx.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s ON", table))
				if err != nil { return fmt.Errorf("failed to set identity_insert on: %w", err) }
				
				_, err = tx.Exec(insQ, args...)
				
				_, _ = tx.Exec(fmt.Sprintf("SET IDENTITY_INSERT %s OFF", table))
				
				return err
			}
		}

		_, err = tx.Exec(insQ, args...)
		return err
	} else if err != nil {
		return err
	}

	var sets []string
	var args []interface{}
	i := 1
	for k, v := range cols {
		if k == idCol { continue }
		sets = append(sets, fmt.Sprintf("%s = @p%d", k, i))
		args = append(args, v)
		i++
	}
	args = append(args, idVal)
	updQ := fmt.Sprintf("UPDATE %s SET %s WHERE %s = @p%d", table, strings.Join(sets, ","), idCol, i)
	_, err = tx.Exec(updQ, args...)
	return err
}

func (l *SQLLoader) replaceChildren(tx *sql.Tx, config models.RelationConfig, parentID interface{}, rows []map[string]interface{}) error {
	delQ := fmt.Sprintf("DELETE FROM %s WHERE %s = @p1", config.SQLTable, config.SQLForeignKey)
	if _, err := tx.Exec(delQ, parentID); err != nil {
		return err
	}

	for _, row := range rows {
		row[config.SQLForeignKey] = parentID
		var colNames, params []string
		var args []interface{}
		i := 1
		for k, v := range row {
			colNames = append(colNames, k)
			params = append(params, fmt.Sprintf("@p%d", i))
			args = append(args, v)
			i++
		}
		insQ := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", config.SQLTable, strings.Join(colNames, ","), strings.Join(params, ","))
		if _, err := tx.Exec(insQ, args...); err != nil {
			return err
		}
	}
	return nil
}

func (l *SQLLoader) syncJoinTable(tx *sql.Tx, config models.RelationConfig, parentID interface{}, rows []map[string]interface{}) error {
	delQ := fmt.Sprintf("DELETE FROM %s WHERE %s = @p1", config.SQLJoinTable, config.SQLForeignKey)
	if _, err := tx.Exec(delQ, parentID); err != nil {
		return err
	}

	for _, row := range rows {
		var childID interface{}
		if val, ok := row["id"]; ok { childID = val } else 
		if val, ok := row["_id"]; ok { childID = val }
		
		if childID != nil {
			otherCol := "role_id" 
			insQ := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (@p1, @p2)", 
				config.SQLJoinTable, config.SQLForeignKey, otherCol)
			
			if _, err := tx.Exec(insQ, parentID, childID); err != nil {
				return err
			}
		}
	}
	return nil
}
