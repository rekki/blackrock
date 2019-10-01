VERSION ?= 0.100

CMDS ?= $(patsubst cmd/%,%,$(wildcard cmd/*))
GO_MOD ?= github.com/rekki/blackrock

all: build test

build: $(CMDS)

$(CMDS):
	CGO_ENABLED=0 go build -a -o ./$@ $(GO_MOD)/cmd/$@

test:
	go test -v ./...

clean: $(patsubst %,clean-%,$(CMDS))

$(patsubst %,clean-%,$(CMDS)):
	rm -f $(patsubst clean-%,%,$@)

docker-build: $(patsubst %,docker-build-%,$(CMDS))

$(patsubst %,docker-build-%,$(CMDS)):
	docker build -t rekki/$(patsubst docker-build-%,%,$@):$(VERSION) --build-arg CMD=$(patsubst docker-build-%,%,$@) -f ./build/docker/Dockerfile .

docker-push: $(patsubst %,docker-push-%,$(CMDS))

$(patsubst %,docker-push-%,$(CMDS)):
	docker push rekki/$(patsubst docker-push-%,%,$@):$(VERSION)

docker-clean: $(patsubst %,docker-clean-%,$(CMDS))

$(patsubst %,docker-clean-%,$(CMDS)):
	docker rmi --force $(shell docker images --format '{{.Repository}}:{{.Tag}}' | grep '^rekki/$(patsubst docker-push-%,%,$@):') || true

docker-compose-up:
	docker-compose -f ./deployments/docker-compose.yml -f ./deployments/docker-compose.kafka.yml up -d

docker-compose-up-kafka:
	docker-compose -f ./deployments/docker-compose.kafka.yml up -d

docker-compose-ps:
	docker-compose -f ./deployments/docker-compose.yml -f ./deployments/docker-compose.kafka.yml ps

docker-compose-logs:
	docker-compose -f ./deployments/docker-compose.yml -f ./deployments/docker-compose.kafka.yml logs --timestamps --tail=all

docker-compose-down:
	docker-compose -f ./deployments/docker-compose.yml -f ./deployments/docker-compose.kafka.yml down

clean-all: clean docker-compose-down docker-clean

# Do not forget to update the .PHONY target with the command below:
#   echo ".PHONY: $(cat Makefile | grep -v '^.PHONY:' | grep -oE '^[a-z$][^:]+' | tr '\n' ' ')"
.PHONY: all build $(CMDS) test clean $(patsubst %,clean-%,$(CMDS)) docker-build $(patsubst %,docker-build-%,$(CMDS)) docker-push $(patsubst %,docker-push-%,$(CMDS)) docker-clean $(patsubst %,docker-clean-%,$(CMDS)) docker-compose-up docker-compose-up-kafka docker-compose-ps docker-compose-logs docker-compose-down clean-all
