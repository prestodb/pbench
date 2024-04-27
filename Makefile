.PHONY: all
all: clean pbench

.PHONY: clean
clean:
	rm -rf pbench_* release

# ------------------------------------------------------------------------------
#  pbench
.PHONY: pbench
pbench:
	pbench

# ------------------------------------------------------------------------------
#  tar
.PHONY: tar
tar: clean pbench
	mkdir -p release/pbench
	cp -r pbench pbench_* benchmarks genconfig/templates release/pbench
	cd release && \
		tar -czf ../pbench_arm64.tar.gz pbench/pbench pbench/pbench_arm64 pbench/benchmarks pbench/templates && \
		tar -czf ../pbench_x86_64.tar.gz pbench/pbench pbench/pbench_x86_64 pbench/benchmarks pbench/templates
	rm -rf release
