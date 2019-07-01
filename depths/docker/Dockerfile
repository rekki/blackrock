FROM golang:alpine

RUN apk update && apk add bash git

RUN go get -v github.com/jackdoe/blackrock/...
RUN for i in orgrim blackhand jubei khanzo; do (cd /go/src/github.com/jackdoe/blackrock/$i && echo $i && go build && mv $i /bin) ; done
RUN rm -rf /go/src



