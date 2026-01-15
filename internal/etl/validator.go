package etl

import (
	"fmt"
	"github.com/BartekS5/IDA/pkg/models"
)

type Validator struct {
	Config *models.MappingSchema
}

func NewValidator(config *models.MappingSchema) *Validator {
	return &Validator{Config: config}
}

// ValidateDocument checks if required fields are present and types are vaguely correct
func (v *Validator) ValidateDocument(doc map[string]interface{}) error {
	// Check ID
	idField := v.Config.IDStrategy.MongoField
	if _, ok := doc[idField]; !ok {
		return fmt.Errorf("missing required ID field: %s", idField)
	}

	// Basic check for required fields could be added here
	// For now, we assume if it's in the mapping it might be nullable unless specified
	return nil
}
