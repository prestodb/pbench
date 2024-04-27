.PHONY: all
all: clean pbench

.PHONY: clean
clean:
	rm pbench_*

# ------------------------------------------------------------------------------
#  pbench
.PHONY: pbench
pbench:
	pbench

.PHONY: release
release: clean pbench

