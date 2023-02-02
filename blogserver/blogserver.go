/*
 *
 * Copyright 2023 puzzleblogserver authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package blogserver

import (
	"context"
	"errors"
	"log"
	"strings"

	pb "github.com/dvaumoron/puzzleblogservice"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const collectionName = "posts"

const idKey = "_id"
const blogIdKey = "blogId"
const postIdKey = "postId"
const userIdKey = "userId"
const titleKey = "title"
const textKey = "text"

const mongoCallMsg = "Failed during MongoDB call :"

var errInternal = errors.New("internal service error")
var errNoPost = errors.New("no blog post with requested ids")

var optsMaxPostId = options.FindOne().SetSort(bson.D{{Key: postIdKey, Value: -1}}).SetProjection(bson.D{{Key: postIdKey, Value: true}})

// server is used to implement puzzleblogservice.BlogServer
type server struct {
	pb.UnimplementedBlogServer
	clientOptions *options.ClientOptions
	databaseName  string
}

func New(clientOptions *options.ClientOptions, databaseName string) pb.BlogServer {
	return server{clientOptions: clientOptions, databaseName: databaseName}
}

func (s server) CreatePost(ctx context.Context, request *pb.CreateRequest) (*pb.Response, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)

	blogId := request.BlogId
	post := bson.M{
		blogIdKey: blogId, userIdKey: request.UserId,
		titleKey: request.Title, textKey: request.Text,
	}

	// rely on the mongo server to ensure there will be no duplicate
	newPostId := uint64(1)

GeneratePostIdStep:
	var result bson.D
	err = collection.FindOne(
		ctx, bson.D{{Key: blogIdKey, Value: blogId}}, optsMaxPostId,
	).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			goto CreatePostStep
		}

		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}

	// can call [0] because result has only one field
	newPostId = extractUint64(result[0].Value) + 1

CreatePostStep:
	post[postIdKey] = newPostId
	_, err = collection.InsertOne(ctx, post)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// retry
			goto GeneratePostIdStep
		}

		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func (s server) GetPost(ctx context.Context, request *pb.IdRequest) (*pb.Content, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)

	var result bson.M
	err = collection.FindOne(
		ctx, bson.D{{Key: blogIdKey, Value: request.BlogId}, {Key: postIdKey, Value: request.PostId}},
	).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errNoPost
		}

		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return convertToContent(result), nil
}

func (s server) GetPosts(ctx context.Context, request *pb.SearchRequest) (*pb.Contents, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)
	filters := bson.D{{Key: blogIdKey, Value: request.BlogId}}

	total, err := collection.CountDocuments(ctx, filters)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}

	paginate := options.Find().SetSort(bson.D{{Key: postIdKey, Value: -1}})
	start := int64(request.Start)
	paginate.SetSkip(start)
	paginate.SetLimit(int64(request.End) - start)

	if filter := request.Filter; filter != "" {
		filters = append(filters, bson.E{Key: titleKey, Value: buildFilterRegex(filter)})
	}

	cursor, err := collection.Find(ctx, filters)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Contents{List: convertToContents(results), Total: uint64(total)}, nil
}

func (s server) DeletePost(ctx context.Context, request *pb.IdRequest) (*pb.Response, error) {
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	defer disconnect(client, ctx)

	collection := client.Database(s.databaseName).Collection(collectionName)

	_, err = collection.DeleteMany(
		ctx, bson.D{{Key: blogIdKey, Value: request.BlogId}, {Key: postIdKey, Value: request.PostId}},
	)
	if err != nil && err != mongo.ErrNoDocuments {
		log.Println(mongoCallMsg, err)
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func disconnect(client *mongo.Client, ctx context.Context) {
	if err := client.Disconnect(ctx); err != nil {
		log.Print("Error during MongoDB disconnect :", err)
	}
}

func convertToContents(posts []bson.M) []*pb.Content {
	contents := make([]*pb.Content, 0, len(posts))
	return contents
}

func convertToContent(post bson.M) *pb.Content {
	title, _ := post[titleKey].(string)
	text, _ := post[textKey].(string)
	id, _ := post[idKey].(primitive.ObjectID)
	return &pb.Content{
		PostId: extractUint64(post[postIdKey]), UserId: extractUint64(post[userIdKey]),
		Title: title, Text: text, CreatedAt: id.Timestamp().Unix(),
	}
}

func buildFilterRegex(filter string) bson.D {
	var regexBuilder strings.Builder
	if strings.Index(filter, ".*") != 0 {
		regexBuilder.WriteString(".*")
	}
	regexBuilder.WriteString(filter)
	if strings.LastIndex(filter, ".*") != len(filter)-2 {
		regexBuilder.WriteString(".*")
	}
	return bson.D{{Key: "$regex", Value: regexBuilder.String()}}
}

func extractUint64(v any) uint64 {
	switch casted := v.(type) {
	case int32:
		return uint64(casted)
	case int64:
		return uint64(casted)
	}
	return 0
}