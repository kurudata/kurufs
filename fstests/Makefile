DURATION ?= 10

all: fsracer fsx

fsracer: healthcheck secfs.test/tools/bin/fsracer
	secfs.test/tools/bin/fsracer $(DURATION) /jfs >fsracer.log
	make healthcheck

fsx: healthcheck secfs.test/tools/bin/fsx
	secfs.test/tools/bin/fsx -d $(DURATION) -p 10000 -F 10000000 /tmp/fsx.out
	make healthcheck

setup:
	redis-server &
	mkdir -p /jfs
	../juicefs format -storage mem unittest
	../juicefs mount -o allow_other &

healthcheck:
	pgrep juicefs

secfs.test/tools/bin/fsx: secfs.test

secfs.test/tools/bin/fsracer: secfs.test

secfs.test:
	git clone https://github.com/billziss-gh/secfs.test.git
	make -C secfs.test >secfs.test-build.log 2>&1
