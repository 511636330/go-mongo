package mongo

import (
	"context"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Pipeline = mongo.Pipeline

type Model interface {
	GetConnection() string
	GetCollection() string
}

type Filter struct {
	SortBy     string
	SortMode   int8
	Limit      *int64
	Skip       *int64
	Filter     map[string]interface{}
	RegexFiler map[string]string
}

type Collection struct {
	Model         Model
	PkStructField string
	PkBson        string
	Ctx           context.Context
	Client        *mongo.Client
	Collection    *mongo.Collection
}

func GetCollection(ctx context.Context, model Model) *Collection {
	collection := Collection{Model: model}

	connectionName := model.GetConnection()
	collectionName := model.GetCollection()
	client := GetClient(ctx, connectionName)
	collection.Ctx = ctx
	collection.Client = client
	collection.Collection = client.Database(GetMongoDatabase(connectionName)).Collection(collectionName)
	return &collection
}

func (collection *Collection) SetPk(struckField, bsonTag string) {
	collection.PkStructField = struckField
	collection.PkBson = bsonTag
}

func (collection *Collection) GetPK() (pkStructField, pkBson string) {
	pkStructField = collection.PkStructField
	pkBson = collection.PkBson

	if len(pkStructField) == 0 {
		pkStructField = "Id"
	}
	if len(pkBson) == 0 {
		pkBson = "_id"
	}

	return
}

func (collection *Collection) GetPkValue() (pkValue interface{}) {
	if len(collection.PkStructField) == 0 {
		collection.PkBson = "_id"
		pkValue = reflect.ValueOf(collection.Model).Elem().FieldByName("Id").Interface().(primitive.ObjectID)
	} else {
		pkValue = reflect.ValueOf(collection.Model).Elem().FieldByName(collection.PkBson).Interface().(string)
	}
	return
}

func trackTimer(model interface{}, isNew bool) {
	now := time.Now()
	rval := reflect.ValueOf(model).Elem()
	for rval.Kind() == reflect.Ptr {
		if rval.IsNil() && rval.CanAddr() {
			rval.Set(reflect.New(rval.Type().Elem()))
			break
		}

		rval = rval.Elem()
	}
	rtype := rval.Type()
	for i := 0; i < rval.NumField(); i++ {
		if rtype.Field(i).Name == "Document" {
			docValue := rval.Field(i).Addr()
			createdFunc := docValue.MethodByName("SetCreatedAt")
			if isNew && createdFunc.Kind() != reflect.Invalid {
				createdFunc.Call([]reflect.Value{reflect.ValueOf(now)})
			}
			updatedFunc := docValue.MethodByName("SetUpdatedAt")
			if updatedFunc.Kind() != reflect.Invalid {
				updatedFunc.Call([]reflect.Value{reflect.ValueOf(now)})
			}
		}
	}

}

func (collection *Collection) SetPKValue(model interface{}, objId interface{}) {
	rval := reflect.ValueOf(model).Elem()
	for rval.Kind() == reflect.Ptr {
		if rval.IsNil() && rval.CanAddr() {
			rval.Set(reflect.New(rval.Type().Elem()))
			break
		}

		rval = rval.Elem()
	}
	rtype := rval.Type()
	for i := 0; i < rval.NumField(); i++ {
		if rtype.Field(i).Name == "Document" {
			docValue := rval.Field(i).Addr()
			createdFunc := docValue.MethodByName("SetId")
			if createdFunc.Kind() != reflect.Invalid {
				createdFunc.Call([]reflect.Value{reflect.ValueOf(objId)})
			}
			break
		}
	}
}

func (collection *Collection) Insert(model interface{}) (id string, err error) {
	trackTimer(model, true)
	cid, err := collection.Collection.InsertOne(collection.Ctx, model)
	if err == nil {
		objId := cid.InsertedID.(primitive.ObjectID)
		collection.SetPKValue(model, objId)
		id = objId.Hex()
	}
	return
}

func (collection *Collection) InsertMany(models []interface{}) (ids []interface{}, err error) {
	for _, model := range models {
		trackTimer(model, true)
	}
	cid, err := collection.Collection.InsertMany(collection.Ctx, models)
	if err == nil {
		objIds := cid.InsertedIDs
		for idx, objId := range objIds {
			collection.SetPKValue(models[idx], objId)
			ids = append(ids, objId)
		}
	}
	return
}

func (collection *Collection) Save(model interface{}) (bool, error) {
	trackTimer(model, false)

	result, err := collection.Collection.UpdateByID(collection.Ctx, collection.GetPkValue(), bson.D{
		primitive.E{Key: "$set", Value: model},
	})
	if err != nil {
		return false, err
	}
	if result.ModifiedCount == 0 {
		return false, nil
	}
	return true, nil
}

func (collection *Collection) Update(id string, model interface{}) (modifiedCount int64, err error) {
	objId, _ := primitive.ObjectIDFromHex(id)
	trackTimer(model, false)

	result, err := collection.Collection.UpdateByID(collection.Ctx, objId, bson.D{
		primitive.E{Key: "$set", Value: model},
	})
	if err == nil {
		modifiedCount = result.ModifiedCount
	}
	return
}

func (collection *Collection) UpdateOne(filter Filter, model interface{}) (modifiedCount int64, err error) {
	filter = MergeFilter(filter)
	trackTimer(model, false)

	result, err := collection.Collection.UpdateOne(collection.Ctx, filter.Filter, bson.D{
		primitive.E{Key: "$set", Value: model},
	})
	if err == nil {
		modifiedCount = result.ModifiedCount
	}
	return
}

func (collection *Collection) UpdateMany(filter Filter, model interface{}) (modifiedCount int64, err error) {
	filter = MergeFilter(filter)
	trackTimer(model, false)

	result, err := collection.Collection.UpdateMany(collection.Ctx, filter.Filter, bson.D{
		primitive.E{Key: "$set", Value: model},
	})
	if err == nil {
		modifiedCount = result.ModifiedCount
	}
	return
}

func (collection *Collection) ReplaceOne(filter Filter, model interface{}) (modifiedCount int64, err error) {
	filter = MergeFilter(filter)
	trackTimer(model, false)

	result, err := collection.Collection.ReplaceOne(collection.Ctx, filter.Filter, model)
	if err == nil {
		modifiedCount = result.ModifiedCount
	}
	return
}

func (collection *Collection) Find(model interface{}, id string) (err error) {
	_, pkBson := collection.GetPK()
	objID, _ := primitive.ObjectIDFromHex(id)
	result := collection.Collection.FindOne(collection.Ctx, bson.M{
		pkBson:       objID,
		"deleted_at": nil,
	})
	result.Decode(model)
	return
}
func (collection *Collection) FindOne(model interface{}, filter Filter) (err error) {
	filter = MergeFilter(filter)
	opts := options.FindOneOptions{
		Skip: filter.Skip,
	}

	if len(filter.SortBy) > 0 {
		var sortMode int8 = 1
		if filter.SortMode != 0 {
			sortMode = filter.SortMode
		}
		opts.Sort = bson.M{filter.SortBy: sortMode}
	}
	result := collection.Collection.FindOne(collection.Ctx, filter.Filter, &opts)
	result.Decode(model)
	return
}

func (collection *Collection) Count(filter Filter) (c int64, err error) {

	filter = MergeFilter(filter)
	c, err = collection.Collection.CountDocuments(collection.Ctx, filter.Filter)

	return
}

func (collection *Collection) Get(models interface{}, filter Filter) (err error) {
	filter = MergeFilter(filter)
	opts := options.FindOptions{
		Skip:  filter.Skip,
		Limit: filter.Limit,
	}

	if len(filter.SortBy) > 0 {
		var sortMode int8 = 1
		if filter.SortMode != 0 {
			sortMode = filter.SortMode
		}
		opts.Sort = bson.M{filter.SortBy: sortMode}
	}

	cur, err := collection.Collection.Find(collection.Ctx, filter.Filter, &opts)

	if err != nil {
		return
	}

	rval := reflect.ValueOf(models)
	for rval.Kind() == reflect.Ptr {
		if rval.IsNil() && rval.CanAddr() {
			rval.Set(reflect.New(rval.Type().Elem()))
			break
		}

		rval = rval.Elem()
	}

	kind := rval.Kind()

	if kind != reflect.Array && kind != reflect.Slice {
		return
	}

	rvalType := rval.Type().Elem()
	isPtr := rvalType.Kind() == reflect.Ptr

	if isPtr {
		rvalType = rvalType.Elem()
	}

	for cur.Next(collection.Ctx) {
		elem := reflect.New(rvalType).Interface()
		cur.Decode(elem)

		if isPtr {
			rval.Set(reflect.Append(rval, reflect.ValueOf(elem)))
		} else {
			rval.Set(reflect.Append(rval, reflect.ValueOf(elem).Elem()))
		}
	}
	defer cur.Close(collection.Ctx)

	return
}

func (collection *Collection) Delete(id string) (deletedCount int, err error) {
	_, pkBson := collection.GetPK()
	objID, _ := primitive.ObjectIDFromHex(id)
	result, err := collection.Collection.UpdateOne(collection.Ctx, bson.M{
		pkBson:       objID,
		"deleted_at": nil,
	}, bson.D{
		primitive.E{Key: "$set", Value: bson.M{"deleted_at": time.Now()}},
	})

	if err != nil {
		deletedCount = int(result.ModifiedCount)
	}
	return
}

func (collection *Collection) ForceDelete(id string) (deletedCount int, err error) {
	_, pkBson := collection.GetPK()
	objID, _ := primitive.ObjectIDFromHex(id)
	result, err := collection.Collection.DeleteOne(collection.Ctx, bson.M{pkBson: objID})

	if err != nil {
		deletedCount = int(result.DeletedCount)
	}

	return
}

func (collection *Collection) DeleteOne(filter Filter) (deletedCount int, err error) {
	filter = MergeFilter(filter)
	result, err := collection.Collection.UpdateOne(collection.Ctx, filter.Filter, bson.D{
		primitive.E{Key: "$set", Value: bson.M{"deleted_at": time.Now()}},
	})

	if err == nil {
		deletedCount = int(result.ModifiedCount)
	}
	return
}

func (collection *Collection) FormceDeleteOne(filter Filter) (deletedCount int, err error) {
	filter = MergeFilter(filter)
	result, err := collection.Collection.DeleteOne(collection.Ctx, filter.Filter)

	if err == nil {
		deletedCount = int(result.DeletedCount)
	}
	return
}

func (collection *Collection) DeleteMany(filter Filter) (deletedCount int64, err error) {
	filter = MergeFilter(filter)
	result, err := collection.Collection.UpdateMany(collection.Ctx, filter.Filter, bson.D{
		primitive.E{Key: "$set", Value: bson.M{"deleted_at": time.Now()}},
	})

	if err == nil {
		deletedCount = result.ModifiedCount
	}
	return
}

func (collection *Collection) ForceDeleteMany(filter Filter) (deletedCount int64, err error) {
	filter = MergeFilter(filter)
	result, err := collection.Collection.DeleteMany(collection.Ctx, filter.Filter)

	if err == nil {
		deletedCount = result.DeletedCount
	}
	return
}

func MergeFilter(filter Filter) Filter {
	if filter.RegexFiler != nil {
		for k, v := range filter.RegexFiler {
			filter.Filter[k] = primitive.Regex{Pattern: v, Options: "i"}
		}
	}
	filter.Filter["deleted_at"] = nil

	return filter
}
