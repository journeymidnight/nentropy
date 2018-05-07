#!/usr/local/bin/bash
REPO_ROOT=`git rev-parse --show-toplevel`
PROTO_DIR=${REPO_ROOT}/osd/multiraftbase
rm -rf *.pb.go

GOGO_PROTOBUF_PACKAGE=github.com/gogo/protobuf
GOGO_PROTOBUF_PATH=${REPO_ROOT}/../../../${GOGO_PROTOBUF_PACKAGE}

GITHUB_PARENT=${REPO_ROOT}/../../..

IMPORT_PREFIX=github.com/journeymidnight/

protoc  -I.:${GOGO_PROTOBUF_PATH}:${GITHUB_PARENT}:${REPO_ROOT} --gofast_out=plugins=grpc:.    *.proto
sed -i '' '/import _/d'  *.pb.go
sed -i '' '/gogoproto/d'  *.pb.go

