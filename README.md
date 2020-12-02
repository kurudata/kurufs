# JuiceFS

## How to Build

```bash
go get github.com/juicedata/juicefs.git
```

## Developing on macOS

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
