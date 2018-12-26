package mongoClient

import (
	"context"
	"errors"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

var mongoClient *mongo.Client
var mongoDb *mongo.Database
var mongoCollectionsList = make(map[string]bool)

// initialiseMongo
func Connect(uri string, db string) (bool, error) {
	if uri == "" {
		return false, errors.New("uri not provided")
	}
	var err error
	// CREATE CLIENT
	mongoClient, err = mongo.Connect(context.Background(), uri, nil)
	if err != nil {
		return false, errors.New("Mongo db client creation failed: " + err.Error())
	}
    defer closeMongo()

	// SELECT DATABASE
	dbExists, err := databaseExists(db)
	if err != nil {
		return false, errors.New("Error while retrieving databases list 💥: " + err.Error())
	}
	if !dbExists {
		return false, errors.New("provided mongo db does not exists")
	}
	mongoDb = mongoClient.Database(db)

	// RETRIEVE COLLECTIONS LIST
	err = retrieveCollectionsList()
	if err != nil {
		return false, errors.New("Mongo db collections list not retrieved 💥:" + err.Error())
	}

	return true, nil
}

func retrieveCollectionsList() error {
	var cur mongo.Cursor
	var err error
	cnt := context.Background()

	cur, err = mongoDb.ListCollections(context.Background(), nil)
	if err != nil {
		return errors.New("Mongo db collections list not retrieved: " + err.Error())
	}

	for cur.Next(cnt) {
		elem := bson.NewDocument()
		if err := cur.Decode(elem); err != nil {
			return errors.New("Unable to decode element while reading collections list: " + err.Error())
		}
		name := elem.Lookup("name").StringValue()
		mongoCollectionsList[name] = true
	}

	if err := cur.Err(); err != nil {
		return errors.New("Cursor error while reading collections list: " + err.Error())
	}

	return nil
}

func databaseExists(databaseName string) (bool, error) {
	var databasesList []string
	var err error

	databasesList, err = mongoClient.ListDatabaseNames(context.Background(), nil)
	if err != nil {
		return false, err
	}
	for _, v := range databasesList {
		if v == databaseName {
			return true, nil
		}
	}
	return false, nil
}

func collectionExists(collectionName string) bool {
	_, exists := mongoCollectionsList[collectionName]
	return exists
}

func closeMongo() {
	_ = mongoClient.Disconnect(context.Background())
	mongoClient = nil
	mongoDb = nil

}
