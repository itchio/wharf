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
  * Storage, which stores all builds, recipes, and signatures
  * User, who wants to get new builds

When Creator wants to push a new build:

  * If it's the first push, they have no choice but to upload everything
  * If there's a previous build available, they can download its signature,
  compute a recipe, and upload that instead (along with a new signature)

When Storage receives a new build:

  * If it's a recipe, it should keep track of old-version and new-version, to
  let User know which recipes to download when upgrading from their local version
  to the latest version
  * It could rebuild new-version from `old-version + recipe`, so that new-version
  becomes the new canonical first download for Users who aren't using a wharf-aware launcher
  such as itch.io/app
  * When rebuilding new-version, it should check against the signature uploaded
  by Creator, and mark the build as 'broken' instead.

When User is made aware that there's a new build available:

  * It should query Storage to know which recipes it needs to download, then
  apply them one after the other until they reach the latest version
  * Whenever applying a recipe, User should write changed files to a 'staging'
  folder so no data is overwritten and the apply process can be aborted if the
  final signature doesn't match (which could indicate a corrupt recipe, or a
  corrupt local version).
  * Whenever applying a recipe, User may build a reverse recipe, that allows
  them to rollback at any point in the future.

Additionally:

  * User may check for integrity of their local version at any time by downloading
  the relevant signature and hashing local files block by block.
  * Storage may allow User to re-download corrupted data on a block by block
  basis. Alternatively, User might just re-download the canonical version of
  the build.

## Implementation

### Internals

Builds are directories - archives must be unpacked before wharf can work with them
(butler does that).

Directories are treated as a single TLC (`tar-like container`) unit - really
is a list of directories, files, and symbolic links along with their permissions.

There are two file formats used by wharf: recipes and signatures. They're both
structured as follows:

  * 

#### Recipes

A recipe file contains two TLC trees (target=old, source=new), and a Brotli-compressed
stream of RSync operations.

The RSync algorithm is run against a hash library built from *all files* of the
old version, rather than going file-on-file, which means wharf handles renames
gracefully.

#### Signatures

A signature is a TLC tree along with a set of `rolling hash + md5 hash` for each
block of each

## Hacking on wharf

wharf is a pretty typical golang project, all its dependencies are open-source,
it even has a few tests!

### Regenerating protobuf code

```bash
protoc --go_out=. pwr/*.proto
```

protobuf v3 is required, as we use the 'proto3' syntax.

## License

Licensed under MIT License, see `LICENSE` for details.
