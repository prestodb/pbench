all: clean pbench

clean:
	rm -rf pbench_* release

install: pbench
	rm -f /usr/local/bin/pbench
ifeq ($(shell uname -p),arm)
	ln -s $(CURDIR)/pbench_arm64 /usr/local/bin/pbench
else
	ln -s $(CURDIR)/pbench_x86_64 /usr/local/bin/pbench
endif

.PHONY: pbench
pbench:
	./pbench > /dev/null

tar: clean pbench
	mkdir -p release/pbench
	cp -r pbench pbench_* benchmarks cmd/genconfig/templates release/pbench
	cd release && \
		tar -czf ../pbench_arm64.tar.gz pbench/pbench pbench/pbench_arm64 pbench/benchmarks pbench/templates && \
		tar -czf ../pbench_x86_64.tar.gz pbench/pbench pbench/pbench_x86_64 pbench/benchmarks pbench/templates
	rm -rf release

upload: pbench
	aws s3 cp pbench_x86_64 s3://presto-deploy-infra-and-cluster-a9d5d14

sync:
	cp -r clusters/* ../presto-performance/presto-deploy-cluster/clusters
	rm -f ../presto-performance/presto-deploy-cluster/clusters/*.go

clusters: pbench
	./pbench genconfig -t clusters/templates -p clusters/params.json clusters
