<p align="center"><a href="https://github.com/juicedata/juicefs/pkg"><img alt="JuiceFS Logo" src="https://github.com/juicedata/juicefs/pkg/raw/main/docs/images/juicefs-logo.png" width="50%" /></a></p>
<p align="center">
    <a href="https://travis-ci.com/juicedata/juicefs/pkg" target="_blank"><img alt="Build Status" src="https://travis-ci.com/juicedata/juicefs/pkg.svg?token=jKSPwswpc2ph4uMtwpHa&branch=main" /></a>
    <a href="https://goreportcard.com/report/github.com/juicedata/juicefs/pkg" target="_blank"><img alt="Go Report Card" src="https://goreportcard.com/badge/github.com/juicedata/juicefs/pkg" /></a>
    <a href="https://join.slack.com/t/juicefs/shared_invite/zt-kjbre7de-K8jeTMouDZE8nKEZVHLAMQ" target="_blank"><img alt="Join Slack" src="https://badgen.net/badge/Slack/Join%20JuiceFS/0abd59?icon=slack" /></a>
</p>

**JuiceFS** is an open-sourced POSIX file system built on top of [Redis](https://redis.io) and object storage (e.g. Amazon S3),
designed and optimized for cloud-native enviroment. By using the widely adopted Redis and S3 as the persistent storage,
JuiceFS serves as a stateless middleware to enable many application to share data easily.

The highlighted features are:

- **Fully POSIX compatible**:   pass all tests in pjdfstest
- **Global file locks**
- **Data compression**

---

[Architecture](#architecture) | [Getting Started](#getting-started) | [Benchmark](#benchmark) | [Supported Object Storage](#supported-object-storage) | [FAQ](#faq) | [Roadmap](#roadmap) | [Reporting Issues](#reporting-issues) | [Contributing](#contributing) | [Community](#community) | [Usage Tracking](#usage-tracking) | [License](#license) | [Credits](#credits)

---

## Architecture

Add a dialog here
## Getting Started

### Precompiled binaries

### Building from source

```
git clone github.com:juicedata/juicefs/pkg.git
make
```
## Dependency

A Redis server (>=2.2) or service is needed for metadata, please follow [Redis Quick Start](https://redis.io/topics/quickstart).

There are many options for object storage, local disk is the easist one to get started.

## Format

```
./juicefs format test
```

## Mount

```
./juicefs mount ~/jfs
```

## Benchmark

Added the chart here

## Supported Object Storage

- Amazon S3
- Google Cloud Storage
- Azure Blob Storage
- Alibaba Cloud Object Storage Service (OSS)
- Tencent Cloud Object Storage (COS)
- Local disk
- Ceph RGW
- Minio
- Redis

For the detailed list, see [juicesync](https://github.com/juicedata/juicesync).

## FAQ

### Why doesn't JuiceFS support XXX object storage?

JuiceFS already supported many object storage, please check [the list](#supported-object-storage) first. If you couldn't found it, try [reporting issue](#reporting-issues) to the community.

### Can I use Redis cluster?

The simple answer is no. JuiceFS use [transaction](https://redis.io/topics/transactions) to guarantee the atomicy of meta opearations, which is not well supported in cluster mode.

## Status

It's considered as beta quality, the storage format is not stablized yet, it's not recommended to deploy it into production.
Please test it with your use cases and give us feedback.

## Roadmap

- Kubernetes CSI plugin
- Stablize storage format
- Hadoop SDK
- S3 gateway
- Windows client
- Encryption at rest
- Other Databases for Meta


## Reporting Issues

We use [GitHub Issues](https://github.com/juicedata/juicefs/issues) to track community reported issues. You can also [contact](#community) the community for getting answers.

## Contributing

Thank you for your contribution! Please refer to the [CONTRIBUTING.md](https://github.com/juicedata/juicefs/pkg/blob/main/CONTRIBUTING.md) for more information.


## Community

Welcome to join our [Slack channel](https://join.slack.com/t/juicefs/shared_invite/zt-kjbre7de-K8jeTMouDZE8nKEZVHLAMQ) to connect with JuiceFS team members and other users.

## Usage Tracking

JuiceFS by default collects **anonymous** usage data. It only collects core metrics (e.g. inode number), no user or any sensitive data will be collected. You could review related code [here](https://github.com/juicedata/juicefs/blob/main/redis/xxx.go).

These data helps [JuiceFS team](https://github.com/orgs/juicedata/people) to understand how the community is using this project. You could disable this feature easily, just follow steps below.

## License

JuiceFS is open-sourced under GNU AGPL v3.0, see [LICENSE](https://github.com/juicedata/juicefs/blob/main/LICENSE).

## Credits

Design of JuiceFS was inspired from [Google File System](https://research.google.com/archive/gfs-sosp2003.pdf),
[HDFS](https://hadoop.apache.org/) and [MooseFS](https://moosefs.com/), thanks to their great work.