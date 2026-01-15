package utils

import (
	"fmt"
	"strconv"
	"time"

	"github.com/BartekS5/IDA/pkg/models"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ConvertToMongoType handles type conversion from SQL/Generic to MongoDB
func ConvertToMongoType(val interface{}, cfg models.FieldConfig) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	switch cfg.Type {
	case "datetime":
		return ConvertDateTime(val, cfg.Format)
	case "enum":
		return fmt.Sprintf("%v", val), nil
	case "int":
		return ConvertToInt(val)
	case "string":
		return fmt.Sprintf("%v", val), nil
	default:
		return val, nil
	}
}

// ConvertToSQLType handles type conversion from MongoDB back to SQL
func ConvertToSQLType(val interface{}, cfg models.FieldConfig) (interface{}, error) {
	if val == nil {
		return nil, nil
	}
	switch cfg.Type {
	case "datetime":
		return ConvertDateTime(val, cfg.Format)
	case "int":
		return ConvertToInt(val)
	case "string", "enum":
		return fmt.Sprintf("%v", val), nil
	default:
		return val, nil
	}
}

// GetIntOffset safely converts an interface to int, defaulting to 0.
// Useful for pagination offsets.
func GetIntOffset(v interface{}) int {
	if v == nil {
		return 0
	}
	// Try using the existing ConvertToInt logic, ignoring errors
	val, err := ConvertToInt(v)
	if err != nil {
		return 0
	}
	return val
}

func ConvertDateTime(val interface{}, format string) (interface{}, error) {
	switch v := val.(type) {
	case time.Time:
		return v, nil
	case primitive.DateTime:
		return v.Time(), nil
	case string:
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, f := range formats {
			if t, err := time.Parse(f, v); err == nil {
				return t, nil
			}
		}
		return nil, fmt.Errorf("unable to parse datetime: %s", v)
	case []byte:
		return ConvertDateTime(string(v), format)
	default:
		return val, nil
	}
}

func ConvertToInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case float64:
		return int(v), nil
	case primitive.DateTime:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	case []byte:
		return strconv.Atoi(string(v))
	default:
		return 0, fmt.Errorf("cannot convert %T to int", val)
	}
}
