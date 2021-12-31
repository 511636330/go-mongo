package mongo

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	config "github.com/511636330/go-conf"
	"github.com/golang/glog"
	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var clients = make(map[string]*mongo.Client)

func GetClient(ctx context.Context, conn string) *mongo.Client {
	mongoClient, ok := clients[conn]
	if ok && mongoClient != nil {
		return mongoClient
	}
	return Connnect(ctx, conn)
}

func Connnect(ctx context.Context, conn string) *mongo.Client {
	uri := GetMongoDSN(conn)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))

	if err != nil {
		glog.Errorf("Connect mongo %s error: %v", conn, err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		glog.Errorf("Connect mongo %s error: %v", conn, err)
	}

	clients[conn] = client

	return client
}

func GetMongoDSN(conn string) (dsn string) {
	username := config.GetString(fmt.Sprintf("database.mongo.%s.username", conn))
	password := url.PathEscape(config.GetString(fmt.Sprintf("database.mongo.%s.password", conn)))
	hosts := config.GetString(fmt.Sprintf("database.mongo.%s.host", conn))
	port := config.GetString(fmt.Sprintf("database.mongo.%s.port", conn))
	database := GetMongoDatabase(conn)
	charset := config.GetString(fmt.Sprintf("database.mongo.%s.charset", conn), "utf8")
	options := config.GetStringMap(fmt.Sprintf("database.mongo.%s.options", conn))
	optionString := ""
	for key, value := range options {
		optionString += "&" + cast.ToString(key) + "=" + cast.ToString(value)
	}
	var hostArr []string
	for _, host := range strings.Split(hosts, ",") {
		hostArr = append(hostArr, fmt.Sprintf("%s:%s", host, port))
	}
	uri := strings.Join(hostArr, ",")
	if len(username) > 0 && len(password) > 0 {
		dsn = fmt.Sprintf("mongodb://%s:%s@%s/%s?charset=%s%s", username, password, uri, database, charset, optionString)
	} else {
		dsn = fmt.Sprintf("mongodb://%s/%s?charset=%s%s", uri, database, charset, optionString)
	}
	return
}

func GetMongoDatabase(conn string) string {
	database := config.GetString(fmt.Sprintf("database.mongo.%s.database", conn))
	return database
}
