package etl

import (
	"context"
	"time"

	"github.com/BartekS5/IDA/pkg/logger"
	"github.com/BartekS5/IDA/pkg/models"
	"github.com/BartekS5/IDA/pkg/utils" // Imported utils
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoLoader struct {
	Client      *mongo.Client
	Config      *models.MappingSchema
	Transformer *Transformer
}

func NewMongoLoader(client *mongo.Client, config *models.MappingSchema) *MongoLoader {
	return &MongoLoader{
		Client:      client,
		Config:      config,
		Transformer: NewTransformer(config),
	}
}

func (m *MongoLoader) Load(data []map[string]interface{}) error {
	coll := m.Client.Database("mydb").Collection(m.Config.MongoCollection)
	var writes []mongo.WriteModel

	for _, sqlRow := range data {
		// 1. Transform SQL -> Mongo
		doc, err := m.Transformer.TransformSQLToMongo(sqlRow)
		if err != nil {
			logger.Errorf("Skipping row due to transform error: %v", err)
			continue
		}

		// 2. Prepare Upsert
		idVal := doc[m.Config.IDStrategy.MongoField]
		if idVal == nil {
			logger.Errorf("Missing ID for document")
			continue
		}

		filter := bson.M{m.Config.IDStrategy.MongoField: idVal}
		update := bson.M{"$set": doc}
		model := mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true)
		writes = append(writes, model)
	}

	if len(writes) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		res, err := coll.BulkWrite(ctx, writes)
		if err != nil {
			return err
		}
		logger.Infof("Mongo BulkWrite: Match %d, Mod %d, Upsert %d", res.MatchedCount, res.ModifiedCount, res.UpsertedCount)
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
	// Use shared utility function
	skip := int64(utils.GetIntOffset(offset))
	findOpts.SetSkip(skip)

	// Sort by ID to ensure consistent paging
	findOpts.SetSort(bson.M{m.Config.IDStrategy.MongoField: 1})

	cursor, err := coll.Find(ctx, bson.M{}, findOpts)
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(ctx)

	var results []map[string]interface{}
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			logger.Errorf("Error decoding mongo doc: %v", err)
			continue
		}
		results = append(results, doc)
	}

	return results, utils.GetIntOffset(offset) + len(results), nil
}
