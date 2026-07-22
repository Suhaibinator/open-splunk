.PHONY: build build-ui build-server build-collector build-loggen test clean

build: build-server build-collector

build-ui:
	npm run build
	test -f out/index.html

build-server: build-ui
	mkdir -p build
	go build -trimpath -o build/open-splunk-server ./cmd/open-splunk-server

build-collector:
	mkdir -p build
	go build -trimpath -o build/open-splunk-collector ./cmd/open-splunk-collector

build-loggen:
	mkdir -p build
	go build -trimpath -o build/open-splunk-loggen ./cmd/open-splunk-loggen

test:
	go test ./...
	npm run typecheck

clean:
	go clean
