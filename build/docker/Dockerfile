###
# build image
###

FROM golang:alpine AS builder
ARG COMMAND

WORKDIR /

RUN apk add --no-cache --update ca-certificates curl git make

RUN mkdir /geoip /tmp/geoip \
  && curl https://geolite.maxmind.com/download/geoip/database/GeoLite2-City.tar.gz | tar -C /tmp/geoip -zxvf - \
  && mv /tmp/geoip/GeoLite2-City_*/* /geoip

COPY go.mod go.sum /
RUN go mod download

COPY . /
RUN GOOS=linux GOARCH=amd64 make ${COMMAND}

###
# runtime image
###

FROM scratch
ARG COMMAND

COPY --from=builder /${COMMAND} /entrypoint
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /geoip /

ENTRYPOINT ["/entrypoint"]
