# JuiceFS

## How to Build

1. go get github.com/juicedata/juicefs.git

## Develping on MacOS

1. Install [FUSE for macOS](https://osxfuse.github.io/)
1. Install redis-server
1. Run `make`
1. Start the redis:
    ```bash
    redis-server
    ```
1. Mount:
    ```bash
    sudo ./jfs
    ```
