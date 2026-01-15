package etl

import (
	"fmt"
	"github.com/BartekS5/IDA/pkg/models"
	"github.com/BartekS5/IDA/pkg/utils"
)

type Transformer struct {
	Config *models.MappingSchema
}

func NewTransformer(config *models.MappingSchema) *Transformer {
	return &Transformer{Config: config}
}

func (t *Transformer) TransformSQLToMongo(sqlRow map[string]interface{}) (map[string]interface{}, error) {
	doc := make(map[string]interface{})
	
	// ID
	if idVal, ok := sqlRow[t.Config.IDStrategy.SQLField]; ok {
		doc[t.Config.IDStrategy.MongoField] = idVal
	}
	
	// Fields
	for _, fieldCfg := range t.Config.Fields {
		if val, exists := sqlRow[fieldCfg.SQLColumn]; exists {
			converted, err := utils.ConvertToMongoType(val, fieldCfg)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", fieldCfg.SQLColumn, err)
			}
			doc[fieldCfg.MongoField] = converted
		}
	}
	
	// Relations (already enriched by Extractor)
	for relKey, relCfg := range t.Config.Relations {
		if relData, exists := sqlRow[relKey]; exists {
			doc[relCfg.MongoField] = t.transformRelation(relData, relCfg)
		}
	}
	
	return doc, nil
}

func (t *Transformer) TransformMongoToSQL(mongoDoc map[string]interface{}) (map[string]interface{}, error) {
	row := make(map[string]interface{})
	
	if idVal, ok := mongoDoc[t.Config.IDStrategy.MongoField]; ok {
		row[t.Config.IDStrategy.SQLField] = idVal
	}
	
	for _, fieldCfg := range t.Config.Fields {
		if val, exists := mongoDoc[fieldCfg.MongoField]; exists {
			converted, err := utils.ConvertToSQLType(val, fieldCfg)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", fieldCfg.MongoField, err)
			}
			row[fieldCfg.SQLColumn] = converted
		}
	}
	return row, nil
}

func (t *Transformer) transformRelation(relData interface{}, cfg models.RelationConfig) interface{} {
	switch cfg.Embedding {
	case "reference":
		if items, ok := relData.([]map[string]interface{}); ok {
			refs := make([]interface{}, 0, len(items))
			for _, item := range items {
				if refVal, exists := item[cfg.ReferenceKey]; exists {
					refs = append(refs, map[string]interface{}{cfg.ReferenceKey: refVal})
				}
			}
			return refs
		}
	default:
		return relData
	}
	return relData
}

// ExtractRelationData pulls child documents out of the Mongo doc to be inserted into SQL tables
func (t *Transformer) ExtractRelationData(mongoDoc map[string]interface{}, parentID interface{}) map[string][]map[string]interface{} {
	relations := make(map[string][]map[string]interface{})
	
	for relKey, relCfg := range t.Config.Relations {
		val, exists := mongoDoc[relCfg.MongoField]
		if !exists || val == nil {
			continue
		}

		// Handle array of children
		if items, ok := val.(interface{}); ok { // bson.A is essentially []interface{}
			if itemSlice, ok := items.([]interface{}); ok {
				converted := make([]map[string]interface{}, 0, len(itemSlice))
				for _, item := range itemSlice {
					if itemMap, ok := item.(map[string]interface{}); ok {
						// Clone map to avoid mutating original
						childRow := make(map[string]interface{})
						for k, v := range itemMap {
							childRow[k] = v
						}
						// Enforce Foreign Key
						childRow[relCfg.SQLForeignKey] = parentID
						converted = append(converted, childRow)
					}
				}
				relations[relKey] = converted
			}
		}
	}
	return relations
}
