###
# build image
###

FROM golang:alpine AS builder
ARG CMD

WORKDIR /

RUN apk add --no-cache --update ca-certificates make git bash less vim

RUN mkdir /geoip /tmp/geoip \
  && wget -O - https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz | tar -C /tmp/geoip -zxvf - \
  && mv /tmp/geoip/GeoLite2-City_*/* /geoip

COPY go.mod go.sum /
RUN go mod download

COPY . /
RUN GOOS=linux GOARCH=amd64 make ${CMD}

###
# runtime image
###

FROM alpine
ARG CMD

COPY --from=builder /${CMD} /entrypoint
COPY --from=builder /geoip /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/entrypoint"]
