OS=$(shell uname | tr '[:upper:]' '[:lower:]')
ARCH=$(shell uname -m)
BINARY=pbench
PLATFORMS=darwin linux
ARCHITECTURES=amd64 arm64

.PHONY: $(BINARY)
$(BINARY): pre clean
	go build -o $(BINARY)_$(OS)_$(ARCH)

.PHONY: all
all: pre clean
	$(foreach GOOS, $(PLATFORMS),\
    	$(foreach GOARCH, $(ARCHITECTURES),\
    		$(shell export GOOS=$(GOOS); export GOARCH=$(GOARCH); go build -v -o $(BINARY)_$(GOOS)_$(GOARCH))))

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
	$(shell export GOOS=linux; export GOARCH=amd64; go build -v -o $(BINARY)_amd64_linux)
	aws s3 cp $(BINARY)_amd64_linux s3://presto-deploy-infra-and-cluster-a9d5d14

sync:
	cp -r clusters/* ../presto-performance/presto-deploy-cluster/clusters
	rm -f ../presto-performance/presto-deploy-cluster/clusters/*.go

clusters: pbench
	./pbench genconfig -t clusters/templates -p clusters/params.json clusters