VERSION ?= 0.100
DOCKER_REGISTRY ?= rekki

GO_MOD := $(shell head -1 < go.mod | cut -d' ' -f2)
CMDS := $(patsubst cmd/%,%,$(wildcard cmd/*))
EXAMPLES := $(patsubst examples/%,example-%,$(wildcard examples/*))

all: build test

build: $(CMDS) $(EXAMPLES)

$(CMDS):
	CGO_ENABLED=0 go build -o ./$@ $(GO_MOD)/cmd/$@

$(EXAMPLES):
	CGO_ENABLED=0 go build -o ./$@ $(GO_MOD)/examples/$(patsubst example-%,%,$@)

test:
	go vet ./...
	go test -vet=off ./...

clean: $(patsubst %,clean-%,$(CMDS)) $(patsubst %,clean-%,$(EXAMPLES))

$(patsubst %,clean-%,$(CMDS)) $(patsubst %,clean-%,$(EXAMPLES)):
	rm -f $(patsubst clean-%,%,$@)

docker-build: $(patsubst %,docker-build-%,$(CMDS))

$(patsubst %,docker-build-%,$(CMDS)):
	docker build -t rekki/$(patsubst docker-build-%,%,$@):$(VERSION) --build-arg CMD=$(patsubst docker-build-%,%,$@) -f ./build/docker/Dockerfile .

docker-push: $(patsubst %,docker-push-%,$(CMDS))

$(patsubst %,docker-push-%,$(CMDS)):
	docker tag rekki/$(patsubst docker-push-%,%,$@):$(VERSION) $(DOCKER_REGISTRY)/$(patsubst docker-push-%,%,$@):$(VERSION)
	docker push $(DOCKER_REGISTRY)/$(patsubst docker-push-%,%,$@):$(VERSION)

docker-clean: $(patsubst %,docker-clean-%,$(CMDS))

$(patsubst %,docker-clean-%,$(CMDS)):
	docker rmi --force $(shell docker images --format '{{.Repository}}:{{.Tag}}' | grep '^rekki/$(patsubst docker-clean-%,%,$@):') || true

docker-compose-up:
	docker-compose -f ./deployments/docker-compose/main.yml -f ./deployments/docker-compose/dependencies.yml up -d

docker-compose-up-main:
	docker-compose -f ./deployments/docker-compose/main.yml up -d

docker-compose-up-dependencies:
	docker-compose -f ./deployments/docker-compose/dependencies.yml up -d

docker-compose-ps:
	docker-compose -f ./deployments/docker-compose/main.yml -f ./deployments/docker-compose/dependencies.yml ps

docker-compose-logs:
	docker-compose -f ./deployments/docker-compose/main.yml -f ./deployments/docker-compose/dependencies.yml logs --timestamps

docker-compose-down:
	docker-compose -f ./deployments/docker-compose/main.yml -f ./deployments/docker-compose/dependencies.yml down

clean-all: clean docker-compose-down docker-clean

# Do not forget to update the .PHONY target with the command below:
#   echo ".PHONY: $(cat Makefile | grep -v '^.PHONY:' | grep -oE '^[a-z$][^:]+' | tr '\n' ' ')"
.PHONY: all build $(CMDS) $(EXAMPLES) test clean $(patsubst %,clean-%,$(CMDS)) docker-build $(patsubst %,docker-build-%,$(CMDS)) docker-push $(patsubst %,docker-push-%,$(CMDS)) docker-clean $(patsubst %,docker-clean-%,$(CMDS)) docker-compose-up docker-compose-up-kafka docker-compose-ps docker-compose-logs docker-compose-down clean-all
