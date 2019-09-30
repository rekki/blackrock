VERSION ?= 0.100

COMMANDS = $(patsubst cmd/%,%,$(wildcard cmd/*))
GO_MOD ?= github.com/rekki/blackrock

all: build test

build: $(COMMANDS)

$(COMMANDS):
	CGO_ENABLED=0 go build -a -o ./$@ $(GO_MOD)/cmd/$@

test:
	go test -v ./...

clean: $(patsubst %,clean-%,$(COMMANDS))

$(patsubst %,clean-%,$(COMMANDS)):
	rm -f $(patsubst clean-%,%,$@)

docker-build: $(patsubst %,docker-build-%,$(COMMANDS))

$(patsubst %,docker-build-%,$(COMMANDS)):
	docker build -t rekki/$(patsubst docker-build-%,%,$@):$(VERSION) --build-arg COMMAND=$(patsubst docker-build-%,%,$@) -f ./build/docker/Dockerfile .

docker-push: $(patsubst %,docker-push-%,$(COMMANDS))

$(patsubst %,docker-push-%,$(COMMANDS)):
	docker push rekki/$(patsubst docker-push-%,%,$@):$(VERSION)

docker-clean: $(patsubst %,docker-clean-%,$(COMMANDS))

$(patsubst %,docker-clean-%,$(COMMANDS)):
	docker rmi --force $(shell docker images --format '{{.Repository}}:{{.Tag}}' | grep '^rekki/$(patsubst docker-push-%,%,$@):') || true

docker-compose:
	@echo "docker-compose -f ./deployments/docker-compose.yml"

docker-compose-up:
	$(shell make docker-compose) up -d

docker-compose-logs:
	$(shell make docker-compose) logs --tail=all

docker-compose-down:
	$(shell make docker-compose) down

clean-all: clean docker-compose-down docker-clean

# cat Makefile | grep -v '^.PHONY:' | grep -oE '^[a-z$][^:]+' | tr '\n' ' '
.PHONY: all build $(COMMANDS) test clean $(patsubst %,clean-%,$(COMMANDS)) docker-build $(patsubst %,docker-build-%,$(COMMANDS)) docker-push $(patsubst %,docker-push-%,$(COMMANDS)) docker-clean $(patsubst %,docker-clean-%,$(COMMANDS)) docker-compose docker-compose-up docker-compose-logs docker-compose-down clean-all
