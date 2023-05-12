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
	"strings"

	pb "github.com/dvaumoron/puzzleblogservice"
	mongoclient "github.com/dvaumoron/puzzlemongoclient"
	"github.com/uptrace/opentelemetry-go-extra/otelzap"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

const BlogKey = "puzzleBlog"

const collectionName = "posts"

const blogIdKey = "blogId"
const postIdKey = "postId"
const userIdKey = "userId"
const titleKey = "title"
const textKey = "text"

const mongoCallMsg = "Failed during MongoDB call"

var errInternal = errors.New("internal service error")
var errNoPost = errors.New("no blog post with requested ids")

var optsMaxPostId = options.FindOne().SetSort(bson.D{{Key: postIdKey, Value: -1}}).SetProjection(bson.D{{Key: postIdKey, Value: true}})

// server is used to implement puzzleblogservice.BlogServer
type server struct {
	pb.UnimplementedBlogServer
	clientOptions *options.ClientOptions
	databaseName  string
	logger        *otelzap.Logger
}

func New(clientOptions *options.ClientOptions, databaseName string, logger *otelzap.Logger) pb.BlogServer {
	return server{clientOptions: clientOptions, databaseName: databaseName, logger: logger}
}

func (s server) CreatePost(ctx context.Context, request *pb.CreateRequest) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	blogId := request.BlogId
	filter := bson.D{{Key: blogIdKey, Value: blogId}}
	post := bson.M{
		blogIdKey: blogId, userIdKey: request.UserId,
		titleKey: request.Title, textKey: request.Text,
	}

	// rely on the mongo server to ensure there will be no duplicate
	newPostId := uint64(1)

GeneratePostIdStep:
	var result bson.D
	err = collection.FindOne(ctx, filter, optsMaxPostId).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			goto CreatePostStep
		}

		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}

	// call [1] to get picture because result has only the id and one field
	newPostId = mongoclient.ExtractUint64(result[1].Value) + 1

CreatePostStep:
	post[postIdKey] = newPostId
	_, err = collection.InsertOne(ctx, post)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			// retry
			goto GeneratePostIdStep
		}

		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Response{Success: true, Id: newPostId}, nil
}

func (s server) GetPost(ctx context.Context, request *pb.IdRequest) (*pb.Content, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	var result bson.M
	err = collection.FindOne(
		ctx, bson.D{{Key: blogIdKey, Value: request.BlogId}, {Key: postIdKey, Value: request.PostId}},
	).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errNoPost
		}

		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return convertToContent(result), nil
}

func (s server) GetPosts(ctx context.Context, request *pb.SearchRequest) (*pb.Contents, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)
	filters := bson.D{{Key: blogIdKey, Value: request.BlogId}}
	if filter := request.Filter; filter != "" {
		filters = append(filters, bson.E{Key: titleKey, Value: buildRegexFilter(filter)})
	}

	total, err := collection.CountDocuments(ctx, filters)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}

	paginate := options.Find().SetSort(bson.D{{Key: postIdKey, Value: -1}})
	start := int64(request.Start)
	paginate.SetSkip(start).SetLimit(int64(request.End) - start)

	cursor, err := collection.Find(ctx, filters, paginate)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}

	var results []bson.M
	if err = cursor.All(ctx, &results); err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Contents{List: mongoclient.ConvertSlice(results, convertToContent), Total: uint64(total)}, nil
}

func (s server) DeletePost(ctx context.Context, request *pb.IdRequest) (*pb.Response, error) {
	logger := s.logger.Ctx(ctx)
	client, err := mongo.Connect(ctx, s.clientOptions)
	if err != nil {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	defer mongoclient.Disconnect(client, logger)

	collection := client.Database(s.databaseName).Collection(collectionName)

	_, err = collection.DeleteMany(
		ctx, bson.D{{Key: blogIdKey, Value: request.BlogId}, {Key: postIdKey, Value: request.PostId}},
	)
	if err != nil && err != mongo.ErrNoDocuments {
		logger.Error(mongoCallMsg, zap.Error(err))
		return nil, errInternal
	}
	return &pb.Response{Success: true}, nil
}

func convertToContent(post bson.M) *pb.Content {
	title, _ := post[titleKey].(string)
	text, _ := post[textKey].(string)
	return &pb.Content{
		PostId: mongoclient.ExtractUint64(post[postIdKey]),
		UserId: mongoclient.ExtractUint64(post[userIdKey]),
		Title:  title, Text: text, CreatedAt: mongoclient.ExtractCreateDate(post).Unix(),
	}
}

func buildRegexFilter(filter string) bson.D {
	filter = strings.ReplaceAll(filter, "%", ".*")
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
