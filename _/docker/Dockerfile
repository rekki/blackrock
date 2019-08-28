FROM jackdoe/blackrock-base:0.2

RUN go get -v github.com/jackdoe/blackrock/...
RUN for i in orgrim jubei khanzo; do (cd /go/src/github.com/jackdoe/blackrock/$i && echo $i && go build && mv $i /bin) ; done
RUN rm -rf /go/src



