# The C ABI against the native Go engine

This module replaces a native Go implementation of the same storage engine. The
question that decides whether that is a good trade is what crossing into Rust
costs, so both engines were measured on the same machine, on the same payload,
with the network removed.

**The short answer.** Upload through the C ABI is now **1.6x to 1.9x faster than
the native Go engine**, and the crossing itself costs about 15% on upload and
nothing measurable on download. The download rows where the native engine looks
ahead are an artefact of the two mocks, not a difference between the engines,
and the section on that is the most important part of this document.

These numbers depend on sia-sdk-rs PR #460, which applies the object keystream in
bulk rather than per 64-byte segment. Without it, upload through the C ABI is
378 MB/s rather than 721.

## What was measured

One upload, one pin and one download against a 90 host pool. Timings are wall
clock around each phase, so they include the boundary, the erasure coding, the
encryption and the framing. Only the host transport is faked. Every arm reads
and discards through a 1 MiB buffer, and correctness is checked on a separate
untimed pass.

Five samples per cell, each in a fresh process, every sample verified.

| | |
|---|---|
| machine | Apple M4 Pro, 14 cores (10P + 4E), 48 GiB, macOS 26.6.2 |
| Go | 1.27.0 |
| rustc | 1.96.1 |
| C ABI | sia-sdk-rs `matt/cabi` rebased onto PR #460, `sia_storage_cabi` with the `mock` feature |
| native | sia-storage-go `f9961f7`, the revision `s3d` currently depends on |

## The practical comparison

What a caller of this module gets, against what they have today. 256 MB payload.
A verdict is given only where the ranges do not overlap, because at this sample
size an overlap is not a result.

### Upload, by maximum buffered slabs

| buffered slabs | native median | native range | C ABI median | C ABI range | verdict |
|---|---|---|---|---|---|
| 1 | 364 | 351 to 376 | **707** | 699 to 725 | C ABI faster 1.94x |
| 5 | 516 | 488 to 517 | **822** | 813 to 838 | C ABI faster 1.59x |
| 10 | 492 | 463 to 499 | **829** | 815 to 838 | C ABI faster 1.69x |

### Download, by maximum buffered chunks

| buffered chunks | native median | native range | C ABI median | C ABI range | verdict |
|---|---|---|---|---|---|
| 1 | 734 | 588 to 777 | 362 | 358 to 390 | see below |
| 10 | 4044 | 3801 to 4607 | 3615 | 3521 to 3644 | see below |
| 30 | 5201 | 4085 to 5736 | 5541 | 5489 to 5777 | overlapping |

All figures MB/s.

The upload rows are a real result. **The download rows are not**, and the reason
is in the mock section below. Do not quote them as an engine comparison.

## Where the difference comes from

To separate the binding from the engine, the same work was run a third way: the
Rust engine on its own, through its own benchmark, against the **same**
`MockNetwork` the C ABI arm uses. Any difference between that arm and the C ABI
arm is the boundary and nothing else.

Matched at 120 MiB, the payload the Rust benchmark uses.

| phase | setting | native Go engine | Rust engine direct | Rust via C ABI + Go | boundary cost |
|---|---|---|---|---|---|
| upload | 10 buffered slabs | 525 | 846 | 721 | 15% |
| download | 10 buffered chunks | 3919 | 3389 | 3456 | none |
| download | 30 buffered chunks | 4913 | 4829 | 4918 | none |

All figures MB/s.

**Download through the boundary is free**, as it has been in every run of this
benchmark. The C ABI arm comes out marginally ahead of the Rust engine's own
harness in both cells, which is measurement noise between two different
harnesses rather than the boundary making anything faster.

**Upload through the boundary costs about 15%**, down from 21% before PR #460.
The C upload API is push-shaped while the engine is pull-shaped, so the binding
bridges them with a `tokio::io::duplex`. Before #460 the engine drained that pipe
through an 8 KiB `BufReader` in 64-byte reads, which meant roughly 15,000 refill
round trips per 120 MiB. The engine now reads in 1 MiB blocks, and tokio's
`BufReader` bypasses its buffer entirely for reads at or above its capacity, so
that ping-pong is gone. What remains is consistent with the duplex's extra copy
of the payload, which is a per-byte cost rather than a per-call one. That is why
the relative penalty fell while the absolute loss did not.

## The two mocks are not equivalent

This is the part that invalidates the download rows, and it invalidated an
earlier version of this entire document.

The Rust `MockNetwork` simulates a network. The Go `mockHostClient` does not.

| | Rust mock | Go mock |
|---|---|---|
| per sector write | `sleep(3ms)` | none |
| per sector read | `sleep(len x 0.8ns)`, so 3.4ms per 4 MiB sector | none |
| per sector CPU | Merkle root | 4 MiB copy plus Merkle root, under one global mutex |

Both distortions were measured directly rather than argued about:

- Deleting the Rust mock's two unconditional sleeps takes download at 10
  buffered chunks from 3342 to 5323 MB/s, a 59% jump. **The entire download
  deficit is simulated latency.**
- Deleting the Go mock's copy and Merkle root takes its upload from 525 to
  1151 MB/s. More than half of the native engine's own upload number is its
  mock, not its engine.

With both mocks reduced to a bare store, which is the only like-for-like
measurement available, the two engines are level:

| 120 MiB, equalised mocks | native Go | Rust engine |
|---|---|---|
| upload, 10 buffered slabs | 1151 | 1171 |
| download, 10 buffered chunks | 5059 | 5323 |
| download, 30 buffered chunks | 5783 | 5281 |

So the honest engine-to-engine conclusion is parity, and the C ABI's advantage
in the practical table above comes from the Go mock being expensive, while the
C ABI's apparent download deficit comes from the Rust mock being slow. Neither
tells you much about real hosts.

## Binary size

| | |
|---|---|
| native | 12.3 MiB |
| C ABI | 12.9 MiB |

Six hundred kilobytes, against a 15.9 MiB static archive. The linker keeps only
what is referenced, and the native binary carries its own erasure coding,
encryption and RHP4 that the C ABI binary does not. Whatever the cost of this
approach is, it is not distribution size.

## What this does not tell you

**It is not a 7 Gbps answer.** `BINDING_DECISION.md` set that threshold for
`s3d` and `CABI_PLAN.md` records that the only measurement so far was taken on a
102 Mbps uplink. These numbers are 0.4 to 5 GB/s with the network removed, which
bounds the engine rather than answering the question about `s3d`.

**Degraded conditions were not measured at all**, and they are the interesting
case. The native benchmark has a matrix over slow hosts and timed out hosts,
which is where host selection decides the outcome. The Rust mock supports it,
`MockNetwork::set_slow_hosts` and `reset_slow_hosts` both exist, but **the C ABI
does not expose them**, so the Go layer cannot reach that matrix. Adding two
mock only entry points would close it.

**Nothing here speaks to feature parity.** Two gaps found separately matter more
than any number above: `SealObject` returns opaque bytes with no slab accessor,
which `s3d`'s normalised schema needs, and the Rust engine never sets
`UploadedAt`, so slabs it pins skip an indexd freshness check.

## Three traps this benchmark fell into

All three produced published numbers that were wrong, so they are recorded here
rather than quietly fixed.

**Work inside the timed region.** The harness originally accumulated the
downloaded payload to hash it, putting a 256 MB copy inside the measurement while
the native benchmark discarded through a reusable buffer. Correctness is now
checked on a second untimed pass. The same asymmetry existed on upload, where
bridging into the writer shaped API with `io.Copy` used its 32 KiB default; it
now uses 1 MiB, matching the read side.

**Memory pressure masquerading as throughput.** Peak RSS grows by roughly one
encoded object per cycle and is never reclaimed, in **both** engines, because the
in memory mock host stores retain sectors. An earlier pass ran five cycles per
process and produced a table showing the native engine ahead in every cell by up
to 2.4x. That result was an artefact. Every figure here comes from a fresh
process per sample.

**Comparing two mocks and calling it two engines.** The first version of this
document reported native against C ABI and attributed the shape of the result to
boundary crossings. Measuring the Rust engine as its own arm, and then stripping
both mocks, showed that reasoning was wrong on download, where the boundary costs
nothing, and only partly right on upload.

If you extend this benchmark: one sample per process, no work in the timer, and
measure the Rust engine directly before attributing anything to cgo.

## Reproducing it

Both engines publish the same import path, `go.sia.tech/siastorage`, so they
cannot be linked into one binary. The control arm is therefore a module of its
own and builds with the workspace off.

Build the mock archive first, since the C ABI arm links it and a stale one fails
in ways that look like binding bugs. It must come from a tree containing #460.

    make testlib SIA_SDK_RS=../rs_sia_suite/sia-sdk-rs

Then the C ABI arm, one sample per process, repeated five times per cell.

    go run -tags siastorage_mock ./examples/benchmark \
        -size 256000000 -hosts 90 -buffered-slabs 10 -reps 1

It prints one JSON object per cycle with the upload, time to first byte and
download timings, the encoded size, peak RSS and the sha256 verdict.

The native arm's own benchmarks already exist in `sia-storage-go`. They default
to the same 256 MB payload and the same 90 hosts.

    go test -run XXX -bench 'BenchmarkUpload/slow_0_timeout_0_buffered_slabs_5$' \
        -benchtime 1x -count 5 .
    go test -run XXX -bench 'BenchmarkDownload/slow_0_buffered_chunks_10$' \
        -benchtime 1x -count 5 .

`-benchtime 1x -count 5` is the important part. Each count gets a fresh SDK and a
fresh mock, while `-benchtime 5x` would reuse one and measure the memory growth
described above. Pass `-bench.size` to match a different payload.

The Rust arm is the `sia_storage` crate's own benchmark, which uses 120 MiB and
the same 90 host `MockNetwork`. Run the C ABI arm at `-size 125829120` to compare
against it.

    cargo bench --bench upload --features mock -p sia_storage -- "120MiB"

Criterion reports MiB/s and the Go benchmarks report MB/s. The tables above are
MB/s throughout.

`examples/benchmark-native` is the same measurement loop written against the
native engine, for running the two arms through identical code against a real
indexer rather than a mock.

    GOWORK=off go run ./examples/benchmark-native \
        -indexer https://sia.storage -app-key <hex> -reps 5

## Conclusion

The boundary is cheap: free on download, about 15% on upload, against a mock host
on a path whose real world ceiling is the network. With PR #460 the C ABI arm is
comfortably faster than the native Go engine on upload in every cell measured.

The engines themselves are at parity once the mocks are equalised, so nothing
here argues for keeping two implementations on performance grounds. What is still
unanswered is the degraded host matrix, which the C ABI cannot reach until it
exposes the mock's slow host controls, and the throughput question that needs a
machine with an uplink above 7 Gbps. The case for or against this module rests on
those and on the feature parity gaps, not on the cost of the crossing.
