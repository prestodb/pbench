.PHONY: all
all: clean pbench

.PHONY: clean
clean:
	rm -rf pbench_* pbench.* release

# ------------------------------------------------------------------------------
#  pbench
.PHONY: pbench
pbench:
	pbench

.PHONY: tar
tar: clean pbench
	mkdir -p release/pbench
	cp -r pbench pbench_* benchmarks gen-config/templates release/pbench
	cd release && tar -czf ../pbench.tar.gz *
	rm -rf release
