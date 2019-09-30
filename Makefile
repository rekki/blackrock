VERSION ?= 0.100

COMMANDS = $(patsubst cmd/%,%,$(wildcard cmd/*))
GO_MOD ?= github.com/rekki/blackrock

all:

build: $(COMMANDS)

$(COMMANDS):
	CGO_ENABLED=0 go build -a -o ./$@ $(GO_MOD)/cmd/$@

test: $(patsubst %,test-%,$(COMMANDS))

$(patsubst %,test-%,$(COMMANDS)):
	go test $(GO_MOD)/cmd/$(patsubst test-%,%,$@)

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

.PHONY: # TODO
