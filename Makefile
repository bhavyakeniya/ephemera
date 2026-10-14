CONTROLLER_GEN := go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
IMG ?= ephemera:latest

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: generate
generate: ## Regenerate DeepCopy methods
	$(CONTROLLER_GEN) object:headerFile=hack-boilerplate.txt paths=./api/...

.PHONY: manifests
manifests: ## Regenerate CRD + RBAC manifests
	$(CONTROLLER_GEN) rbac:roleName=ephemera-manager crd \
		paths="./api/...;./internal/controller/..." \
		output:crd:artifacts:config=config/crd \
		output:rbac:artifacts:config=config/rbac

.PHONY: vet
vet: ## go vet
	go vet ./...

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: build
build: ## Build the manager binary
	go build -o bin/manager ./cmd/manager

.PHONY: run
run: ## Run the controller locally against your current kubeconfig
	go run ./cmd/manager

.PHONY: docker-build
docker-build: ## Build the container image
	docker build -t $(IMG) .

.PHONY: install
install: ## Install the CRD into the current cluster
	kubectl apply -f config/crd/

.PHONY: deploy
deploy: install ## Deploy the controller (RBAC + manager) into the current cluster
	kubectl apply -f config/rbac/role.yaml
	kubectl apply -f config/manager.yaml

.PHONY: sample
sample: ## Apply the example TTLPolicy
	kubectl apply -f config/samples/
