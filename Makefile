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
	$(shell export GOOS=linux; export GOARCH=amd64; $(GO) -o $(BINARY)_linux_amd64)
	aws s3 cp $(BINARY)_linux_amd64 s3://presto-deploy-infra-and-cluster-a9d5d14

sync:
	cp -r clusters/* ../presto-performance/presto-deploy-cluster/clusters
	rm -f ../presto-performance/presto-deploy-cluster/clusters/*.go

.PHONY: clusters
clusters:
	./pbench genconfig -t clusters/templates -p clusters/params.json clusters
