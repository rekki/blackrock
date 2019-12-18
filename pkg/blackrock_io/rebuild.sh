protoc \
  --proto_path=$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf:. \
  -I/usr/include/google/protobuf -I. \
  -I$GOPATH/src \
  -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  --gogofaster_out=plugins=grpc:. \
  spec.proto

protoc \
  -I/usr/include -I. \
  -I$GOPATH/src \
  -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  --grpc-gateway_out=logtostderr=true,allow_patch_feature=false:. \
  spec.proto

protoc -I/usr/include -I. \
  -I$GOPATH/src \
  -I$GOPATH/src/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
  --swagger_out=logtostderr=true:. \
  spec.proto
