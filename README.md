# JuiceFS

[![Build Status](https://travis-ci.com/juicedata/juicefs.svg?token=jKSPwswpc2ph4uMtwpHa&branch=main)](https://travis-ci.org/juicedata/juicefs)

## How to Build

```bash
go get github.com/juicedata/juicefs.git
```

## Developing on macOS

1. Install [FUSE for macOS 3.x](https://osxfuse.github.io/2020/10/05/OSXFUSE-3.11.2.html)
1. Install redis-server `brew install redis`
1. Run `make`
1. Start redis server:

    ```bash
    brew services run redis
    ```
1. Format

    ```bash
    ./juicefs format dev
    ```
	
1. Mount:

    ```bash
    ./juicefs mount
    ```

### Troubleshooting

```sh
2020/12/15 20:32:24.963958 juicefs[33844] <ERROR>: fuse: exit status 255
```

Missing FUSE for macOS 3.x

```sh
2020/12/15 20:59:16.368006 juicefs[3022] <ERROR>: fuse: fork/exec /Library/Filesystems/osxfusefs.fs/Support/load_osxfusefs: no such file or directory
```

macFUSE 4.x is not supported at the moment.
