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

package main

import (
	_ "embed"

	"github.com/dvaumoron/puzzleblogserver/blogserver"
	pb "github.com/dvaumoron/puzzleblogservice"
	grpcserver "github.com/dvaumoron/puzzlegrpcserver"
	mongoclient "github.com/dvaumoron/puzzlemongoclient"
)

//go:embed version.txt
var version string

func main() {
	s := grpcserver.Make(blogserver.BlogKey, version)
	clientOptions, databaseName := mongoclient.Create()
	pb.RegisterBlogServer(s, blogserver.New(clientOptions, databaseName, s.Logger))
	s.Start()
}
