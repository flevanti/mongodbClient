package mongodbClient

import (
	"context"
	"errors"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/x/bsonx"
)

type connType struct {
	Client *mongo.Client
	Db     *mongo.Database
}

// initialiseMongo
func Connect(uri string, db string) (*connType, error) {
	//var mongoClient *mongo.Client
	//var mongoDb *mongo.Database
	var newConn = new(connType);
	if uri == "" {
		return newConn, errors.New("uri not provided")
	}
	var err error
	// CREATE CLIENT
	newConn.Client, err = mongo.Connect(context.Background(), uri, nil)
	if err != nil {
		return newConn, errors.New("Mongo db client creation failed: " + err.Error())
	}

	// SELECT DATABASE
	//dbExists, err := databaseExists(db)
	//if err != nil {
	//	return false, errors.New("Error while retrieving databases list 💥: " + err.Error())
	//}
	//if !dbExists {
	//	return false, errors.New("provided mongo db does not exists")
	//}
	//newConn.client.Connect(context.Background());
	newConn.Db = newConn.Client.Database(db)
	cur, err := newConn.Db.ListCollections(context.Background(), nil)

	cur = cur
	// RETRIEVE COLLECTIONS LIST
	// TODO fix the issue around the bson newdocument .... driver has changed since last time....FFS
	//err = retrieveCollectionsList()
	//if err != nil {
	//	return false, errors.New("Mongo db collections list not retrieved 💥:" + err.Error())
	//}

	return newConn, nil
}

func (conn connType) RetrieveCollectionsList() ([]string, error) {
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

func (conn connType) Close() {
	_ = conn.Client.Disconnect(context.Background())
	conn.Client = nil
	conn.Db = nil

}
