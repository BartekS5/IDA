package etl

import (
	"context"
	"time"

	"github.com/BartekS5/IDA/pkg/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoLoader struct {
	Client *mongo.Client
	Config *models.MappingSchema
}

func (m *MongoLoader) Load(data []map[string]interface{}) error {
	coll := m.Client.Database("mydb").Collection(m.Config.MongoCollection)
	var writes []mongo.WriteModel

	for _, doc := range data {
		idVal := doc[m.Config.IDStrategy.MongoField]
		filter := bson.M{m.Config.IDStrategy.MongoField: idVal}
		update := bson.M{"$set": doc}
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		writes = append(writes, model)
	}

	if len(writes) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := coll.BulkWrite(ctx, writes)
		if err != nil {
			return err
		}
	}
	return nil
}

type MongoToSQLExtractor struct {
	Client *mongo.Client
	Config *models.MappingSchema
}

func (m *MongoToSQLExtractor) Extract(batchSize int, offset interface{}) ([]map[string]interface{}, interface{}, error) {
	ctx := context.Background()
	coll := m.Client.Database("mydb").Collection(m.Config.MongoCollection)
	
	findOpts := options.Find().SetLimit(int64(batchSize))
	skip := int64(getIntOffset(offset))
	findOpts.SetSkip(skip)

	cursor, err := coll.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		cursor.Decode(&doc)
		results = append(results, doc)
	}

	return results, getIntOffset(offset) + len(results), nil
}
