# wharf

![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)
[![Build Status](https://ci.itch.ovh/buildStatus/icon?job=wharf)](https://ci.itch.ovh/job/wharf)

wharf is a part of the itch.io infrastructure that allows pushing incremental
updates with minimal network usage. It includes a binary diff algorithm based
on rsync.

This is the golang code used by both butler (a wharf client), and the
(closed-source) wharf server.

butler is a command-line helper for used both by the itch.io app and directly
by developers who want a CLI interface to itch.io

  * <https://github.com/itchio/butler>
  * <https://github.com/itchio/itch>

## Concepts

There are three actors playing this out:

  * Creator, who pushes new builds
  * Storage, which stores all builds, patches, and signatures
  * User, who wants to get new builds

When Creator wants to push a new build:

  * If it's the first push, they have no choice but to upload everything
  * If there's a previous build available, they can download its signature,
    compute a patch, and upload that instead (along with a new signature)

When Storage receives a new build:

  * If it's a patch, it should keep track of old-version and new-version, to
    let User know which patches to download when upgrading from their local
    version to the latest version
  * It could rebuild new-version from `old-version + patch`, so that
    new-version becomes the new canonical first download for Users who aren't
    using a wharf-aware launcher such as itch.io/app
  * When rebuilding new-version, it should check against the signature uploaded
    by Creator, and mark the build as 'broken' instead.

When User is made aware that there's a new build available:

  * It should query Storage to know which patches it needs to download, then
    apply them one after the other until they reach the latest version
  * Whenever applying a patch, User should write changed files to a 'staging'
    folder so no data is overwritten and the apply process can be aborted if
    the final signature doesn't match (which could indicate a corrupt patch, or
    a corrupt local version).
  * Whenever applying a patch, User may build a reverse patch, that allows them
    to rollback at any point in the future.

Additionally:

  * User may check for integrity of their local version at any time by
    downloading the relevant signature and hashing local files block by block.
  * Storage may allow User to re-download corrupted data on a block by block
    basis. Alternatively, User might just re-download the canonical version of
    the build.

## Implementation

### Internals

Builds are directories - archives must be unpacked before wharf can work with them
(butler does that).

Directories are treated as a single TLC (`tar-like container`) unit - really
is a list of directories, files, and symbolic links along with their permissions.

There are two file formats used by wharf: patches and signatures. They're both
structured as follows:

  * magic number (uint32)
  * header (protobuf message), contains compression settings
  * compressed stream (if enabled) containing:
    * a bunch of protobuf messages, in a specific order

There is no explicit version number, since protobuf allows adding fields seamlessly.

A new feature would be indicated by a flag in the header set to true, which would
modify the behavior of the parser.

#### patches

A patch file contains two TLC trees (target=old, source=new), and a Brotli-compressed
stream of RSync operations.

The RSync algorithm is run against a hash library built from *all files* of the
old version, rather than going file-on-file, which means wharf handles renames
gracefully.

#### Signatures

A signature is a TLC tree along with a set of `rolling hash + md5 hash` for
each block of 64kb.

Signatures are used both to check the integrity of all files, and to compute
a patch from a version to the next.

## Hacking on wharf

wharf is a pretty typical golang project, all its dependencies are open-source,
it even has a few tests.

### Regenerating protobuf code

```bash
protoc --go_out=. pwr/*.proto
```

protobuf v3 is required, as we use the 'proto3' syntax.

## License

Licensed under MIT License, see `LICENSE` for details.

Contains modified code from the following projects:

  * [kardianos/rsync](https://bitbucket.org/kardianos/rsync/) (BSD) - golang implementation of the rsync algorithm
  * [rapidloop/rtop](https://github.com/rapidloop/rtop) (MIT) - SSH helpers for encrypted private keys
