# wharf.proto

![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)
[![Build Status](https://ci.itch.ovh/buildStatus/icon?job=wharf.proto)](https://ci.itch.ovh/job/wharf.proto)

wharf is a part of the itch.io infrastructure that allows pushing incremental
updates with minimal network usage.

This is the golang code used by both butler (a wharf client), and the
closed-source wharf server.

butler is a command-line helper for used both by the itch.io app and directly
by developers who want a CLI interface to itch.io

  * <https://github.com/itchio/butler>
  * <https://github.com/itchio/itch>

## Regenerating protobuf code

```bash
protoc --go_out=. pwr/*.proto
```

protobuf v3 is required, as we use the 'proto3' syntax.

## License

Licensed under MIT License, see `LICENSE` for details.

