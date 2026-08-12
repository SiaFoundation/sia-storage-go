## 0.2.0 (2026-08-12)

### Breaking Changes

- Added a version byte to the Slab type which breaks the Sia encoding of SealedObject.
- SDK: Adapt transfer concurrency to network conditions
- Download now returns io.ReadCloser instead of writing to an io.Writer

#### Packed uploads recoverable on reader error

If the reader passed to `Add` errors mid-stream, partial bytes become dead padding in the slab instead of killing the upload. Subsequent `Add` calls continue working.

Rename `SlabSize` to `OptimalDataSize` to distinguish the data portion from the full encoded slab size.

#### Removed SlabReader

The upload pipeline reads raw slabs directly and stripes them off the read loop, leaving SlabReader without callers. SlabReader, NewSlabReader, and ReadSlab are removed.

### Features

- SDK: Add per-shard progress callbacks for upload and download
- SDK: Encrypt object data per slab
- SDK: Allow accessing an object's data key and creating an object directly
- Migrate host selection to indexd's inflight reservation methods
- Periodically refresh hosts in the background and warmup their connections.
- Update go.sia.tech/indexd to v0.2.3.

#### Made racing adaptive

Upload and download racing now derive their timeout from the observed network
throughput and only race when it will not steal capacity from higher priority
work: uploads race once every shard has an attempt in flight, and downloads
race only chunks near the read head. Brings the Go SDK to parity with the Rust
SDK.

#### Ramp download chunk sizes and overprovision reads to match the Rust SDK

Downloads now start with a 32 KiB chunk for a fast first byte and double the
chunk size up to 1 MiB, amortizing the fixed cost of a read RPC over more
bytes. Each chunk also launches half again as many initial reads as needed and
takes the first successes, so a single slow host no longer stalls the chunk.

#### Race slow hosts during uploads

Slow hosts are now automatically raced by spawning additional upload attempts after a timeout. The first successful write wins and remaining attempts are cancelled.

### Fixes

- Added an example app that runs a benchmark.
- Remove io.Pipe when downloading
- Update benchmark example to match Rust SDK's.

#### Assemble and encode slabs off the upload read loop

Uploads read whole slabs and move encryption and erasure coding into per slab goroutines. This removes the 64 byte striped reads and the single threaded cipher from the upload hot path.

#### SDK: Batch slab pinning in PinObject

`PinObject` now sends slabs to the indexer in batches of 50 instead of a single request. This keeps the request size bounded for objects with many slabs.

#### SDK: Fix panic when a slab has fewer usable hosts than its minimum shards

`Download` panicked with `index out of range [0] with length 0` when fewer than a slab's `MinShards` hosts were still usable. The host count was checked before `Prioritize`, which removes unusable hosts, so the initial batch could consume more hosts than remained. It now returns `ErrNotEnoughShards`. The panic occurred on a background goroutine, so callers could not recover from it.

#### SDK: Document the prune cutoff on PruneSlabs

`PruneSlabs` now documents that the indexer only prunes slabs older than `api.DefaultSlabPruneCutoff` (72 hours), and that `api.WithBefore` overrides that cutoff. No behaviour change.

#### SDK: Fix upload retry logic and timeout

Retry all upload errors up to 3 attempts per host instead of only
retrying `context.DeadlineExceeded`. The previous check missed
`os.ErrDeadlineExceeded` returned by the network layer, causing
timed out hosts to be permanently removed from the upload queue.

The per-attempt timeout is now a flat 90s instead of a progressive
15s to 120s ramp, matching the Rust SDK.

#### SDK: Skip duplicate hosts when downloading a slab

Sectors are keyed by host when recovering a slab, so two sectors on the same host collapsed into a single entry while the host list kept both. The same shard could then be downloaded twice and counted twice towards the slab's minimum shards, returning with a shard still missing and failing with "too few shards given". Duplicate hosts are now skipped when the sector map is built, which also keeps the host list and the sector map the same length. The indexer rejects slabs with duplicate host keys at pin time, so this is defensive.

## 0.1.0 (2026-05-06)

### Breaking Changes

- Error instead of panic in builder

#### SDK: Add ObjectEvents

Add `ObjectEvents` method for raw event access and remove `ListObjects`.

### Features

- SDK: Add parallelism

## 0.0.3 (2026-04-09)

### Fixes

- Update indexd dependency
