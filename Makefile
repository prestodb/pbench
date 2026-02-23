OS=$(shell uname | tr '[:upper:]' '[:lower:]')
ARCH=$(shell uname -m)
BINARY=pbench
PLATFORMS=darwin linux
ARCHITECTURES=amd64 arm64
TAGS=
GO=go build -tags=$(TAGS)

.PHONY: $(BINARY)
$(BINARY): pre clean
	$(GO) -o $(BINARY)_$(OS)_$(ARCH)

.PHONY: all
all: pre clean
	$(foreach GOOS, $(PLATFORMS),\
    	$(foreach GOARCH, $(ARCHITECTURES),\
    		$(shell export GOOS=$(GOOS); export GOARCH=$(GOARCH); $(GO) -o $(BINARY)_$(GOOS)_$(GOARCH))))

.PHONY: test
test:
	gofmt -l . | (! grep .) || (echo "gofmt: above files are not formatted" && exit 1)
	go vet ./...
	staticcheck ./...
	go test ./... -race -count=1 -timeout 120s

pre:
ifeq "$(shell which go)" ""
	$(error No go in $$PATH)
endif

clean:
	rm -rf $(BINARY)_* release

uninstall:
	rm -f /usr/local/bin/$(BINARY)

install: pbench uninstall
	ln -s $(CURDIR)/$(BINARY) /usr/local/bin/$(BINARY)

tar: clean all
	mkdir -p release/$(BINARY)
	cp -r $(BINARY) $(BINARY)_* benchmarks clusters/params.json *.template.json clusters/templates release/$(BINARY)
	cd release $(foreach GOOS, $(PLATFORMS),\
		$(foreach GOARCH, $(ARCHITECTURES),\
			&& tar -czf ../$(BINARY)_$(GOOS)_$(GOARCH).tar.gz \
				$(BINARY)/$(BINARY) \
				$(BINARY)/$(BINARY)_$(GOOS)_$(GOARCH) \
				$(BINARY)/benchmarks \
				$(BINARY)/templates \
				$(BINARY)/*.template.json \
				$(BINARY)/params.json))
	rm -rf release

upload:
	$(shell export GOOS=linux; export GOARCH=amd64; $(GO) -tags=influx -o $(BINARY)_linux_amd64)
	aws s3 cp $(BINARY)_linux_amd64 s3://presto-deploy-infra-and-cluster-a9d5d14

sync:
	cp -r clusters/* ../presto-performance/presto-deploy-cluster/clusters
	rm -f ../presto-performance/presto-deploy-cluster/clusters/*.go \
		../presto-performance/presto-deploy-cluster/clusters/large/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/large-ssd/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/medium-ssd/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/medium/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/medium-spill/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/xlarge/docker-stack-spark.yaml \
		../presto-performance/presto-deploy-cluster/clusters/2xlarge/docker-stack-spark.yaml

.PHONY: clusters
clusters:
	@echo "Cleaning cluster directories..."
	@find clusters -name config.json -type f | sed 's/\/config.json$$//' | while read dir; do \
		echo "Cleaning $$dir..."; \
		find "$$dir" -type f ! -name config.json -delete; \
	done
	@echo "Generating cluster configurations..."
	./pbench genconfig -t clusters/templates -p clusters/params.json clusters
