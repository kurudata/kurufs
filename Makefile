export GO111MODULE=on

all: mount

REVISION := $(shell git rev-parse --short HEAD || unknown)
REVISIONDATE := $(shell git log -1 --pretty=format:'%ad' --date short)
LDFLAGS = -s -w -X main.REVISION=$(REVISION) \
		        -X main.REVISIONDATE=$(REVISIONDATE)
SHELL = /bin/sh

mount: Makefile *.go */*.go
	go build -ldflags="$(LDFLAGS)" .

mount.ceph: Makefile *.go */*.go
	go build -tags ceph -o mount.ceph -ldflags="$(LDFLAGS)" .

mount.tikv: Makefile *.go */*.go
	go build -tags tikv -o mount.tikv -ldflags="$(LDFLAGS)" .

