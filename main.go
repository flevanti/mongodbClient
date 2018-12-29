package mongodbClient

import (
	"context"
	"errors"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

type ConnType struct {
	Client      *mongo.Client
	Db          *mongo.Database
	Collections []string
}

// initialiseMongo
func Connect(uri string, db string) (*ConnType, error) {
	var newConn = new(ConnType);
	var err error

	if uri == "" {
		return newConn, errors.New("uri not provided")
	}

	// CREATE CLIENT
	newConn.Client, err = mongo.Connect(context.Background(), uri, nil)
	if err != nil {
		return newConn, errors.New("Mongo db client creation failed: " + err.Error())
	}

	// SELECT DATABASE
	newConn.Db = newConn.Client.Database(db)

	//COLLECTIONS LIST
	newConn.Collections, err = newConn.RetrieveCollectionsList()
	if err != nil {
		return newConn, errors.New("Mongo db failed to retrieve collections list: " + err.Error())
	}
	return newConn, nil
}

func (conn ConnType) RetrieveCollectionsList() ([]string, error) {
	var cur mongo.Cursor
	var err error
	var mongoCollectionsList []string

	cnt := context.Background()

	cur, err = conn.Db.ListCollections(context.Background(), nil)
	if err != nil {
		return nil, errors.New("Mongo db collections list not retrieved: " + err.Error())
	}

	for cur.Next(cnt) {
		elem := &bsonx.Doc{}
		err := cur.Decode(elem)
		if err != nil {
			return nil, errors.New("Unable to decode element while reading collections list: " + err.Error())
		}
		name := elem.Lookup("name").StringValue()
		mongoCollectionsList = append(mongoCollectionsList, name)
	}

	if err := cur.Err(); err != nil {
		return nil, errors.New("Cursor error while reading collections list: " + err.Error())
	}

	return mongoCollectionsList, nil
}

//func databaseExists(databaseName string) (bool, error) {
//	var databasesList []string
//	//var databasesList []string
//	var err error
//	databasesList, err = mongoClient.ListDatabaseNames(context.Background(), nil)
//	databasesList = databasesList
//	if err != nil {
//		return false, err
//	}
//	//for _, v := range databasesList2.Databases {
//	//	if v == databaseName {
//	//		return true, nil
//	//	}
//	//}
//	return false, nil
//}

//func collectionExists(collectionName string) bool {
//	_, exists := mongoCollectionsList[collectionName]
//	return exists
//}

func (conn ConnType) Close() {
	_ = conn.Client.Disconnect(context.Background())
	conn.Client = nil
	conn.Db = nil

}
