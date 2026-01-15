package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/BartekS5/IDA/internal/config"
	"github.com/BartekS5/IDA/internal/etl"
	"github.com/BartekS5/IDA/pkg/database"
	"github.com/BartekS5/IDA/pkg/models"
	_ "github.com/microsoft/go-mssqldb"
	"go.mongodb.org/mongo-driver/bson"
)

func TestSQLToMongoMigration(t *testing.T) {
	// 1. Load config
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 2. Connect to databases
	sqlDB, err := database.ConnectSQL(cfg.SQLConnString)
	if err != nil {
		t.Fatalf("Failed to connect to SQL: %v", err)
	}
	defer sqlDB.Close()

	mongoClient, err := database.ConnectMongo(cfg.MongoConnString)
	if err != nil {
		t.Fatalf("Failed to connect to Mongo: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	// 3. Insert test data into SQL
	// Cleanup first to ensure clean state
	cleanupTestData(t, sqlDB, mongoClient)
	insertTestUser(t, sqlDB)

	// 4. Setup Mapping (Mock or load file)
	// We'll construct the schema manually to avoid file path issues in tests
	mappingSchema := &models.MappingSchema{
		Entity:          "AppUser",
		SQLTable:        "users",
		MongoCollection: "users",
		IDStrategy: models.IDStrategy{
			SQLField:   "id",
			MongoField: "_id",
			Type:       "long",
		},
		Fields: map[string]models.FieldConfig{
			"userName": {SQLColumn: "user_name", MongoField: "username", Type: "string"},
			"email":    {SQLColumn: "email", MongoField: "email", Type: "string"},
			"points":   {SQLColumn: "points", MongoField: "points", Type: "int"},
		},
		Relations: map[string]models.RelationConfig{},
	}

	// 5. Run Migration Pipeline directly
	extractor := &etl.SQLToMongoExtractor{DB: sqlDB, Config: mappingSchema}
	loader := etl.NewMongoLoader(mongoClient, mappingSchema)
	pipeline := etl.NewEnhancedPipeline(extractor, loader, 10, false)

	if err := pipeline.Run(); err != nil {
		t.Fatalf("Pipeline execution failed: %v", err)
	}

	// 6. Verify data in MongoDB
	verifyMongoData(t, mongoClient)

	// 7. Cleanup
	cleanupTestData(t, sqlDB, mongoClient)
}

func insertTestUser(t *testing.T, db *sql.DB) {
	// Assuming table 'users' exists (created by setup script)
	query := `
		INSERT INTO users (user_name, password, email, name, points, status, registered_at)
		VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7)
	`
	_, err := db.Exec(query,
		"test_user",
		"password123",
		"test@example.com",
		"Test User",
		100,
		"ACTIVE",
		time.Now(),
	)
	if err != nil {
		t.Fatalf("Failed to insert test user: %v", err)
	}
}

func verifyMongoData(t *testing.T, client *mongo.Client) {
	coll := client.Database("mydb").Collection("users")
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var result bson.M
	err := coll.FindOne(ctx, bson.M{"email": "test@example.com"}).Decode(&result)
	if err != nil {
		t.Fatalf("Failed to find user in MongoDB: %v", err)
	}

	if result["username"] != "test_user" {
		t.Errorf("Expected username test_user, got %v", result["username"])
	}

	// Points comes as int32 or int64 from Mongo
	points := result["points"]
	if val, ok := points.(int32); ok && val != 100 {
		t.Errorf("Expected points 100, got %v", val)
	} else if val, ok := points.(int64); ok && val != 100 {
		t.Errorf("Expected points 100, got %v", val)
	}
}

func cleanupTestData(t *testing.T, sqlDB *sql.DB, mongoClient *mongo.Client) {
	// Clean SQL
	sqlDB.Exec("DELETE FROM users WHERE email = @p1", "test@example.com")

	// Clean MongoDB
	coll := mongoClient.Database("mydb").Collection("users")
	ctx := context.Background()
	coll.DeleteMany(ctx, bson.M{"email": "test@example.com"})
}
