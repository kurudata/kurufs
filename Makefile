export GO111MODULE=on

all: juicefs

REVISION := $(shell git rev-parse --short HEAD || unknown)
REVISIONDATE := $(shell git log -1 --pretty=format:'%ad' --date short)
LDFLAGS = -s -w -X cmd/main.REVISION=$(REVISION) \
		        -X cmd/main.REVISIONDATE=$(REVISIONDATE)
SHELL = /bin/sh

juicefs: Makefile cmd/*.go pkg/*/*.go
	go build -ldflags="$(LDFLAGS)"  -o juicefs ./cmd

