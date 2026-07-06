# Benchmark

A small program that measures upload and download performance of the
`siastorage` SDK against `sia.storage`. It generates pseudo-random data from a
seed, uploads it, pins the resulting object, downloads it back, and verifies the
bytes match. It then prints throughput, TTFB, and the maximum gap between
successive writes during the download.

## How it works

The benchmark has three subcommands: `login`, `run`, and `profiles`.

`login` authorizes the app once and stores a profile (app key + indexer):

1. Requests a connection to `sia.storage` and prints an authorization URL.
2. Waits for the user to approve the connection in the browser.
3. Prompts for the wallet recovery phrase on stdin (or `--new` to generate one)
   and registers the app.

`run` then uses that profile to:

1. Upload `-size` bytes of seeded random data.
2. Pin the uploaded object.
3. Download the object, verifying each byte against the seeded stream.
4. Delete the object and prune its slabs so nothing is left pinned.
5. Print upload/download elapsed time, throughput (raw and encoded), TTFB, and
   the maximum inter-write gap observed during the download.

## Flags

| Flag                           | Description                                              | Default               |
|--------------------------------|----------------------------------------------------------|-----------------------|
| `-size`                        | Size of the data to upload and download, in bytes.       | `125829120` (120 MiB) |
| `-upload-max-buffered-slabs`   | Maximum number of slabs buffered in memory during upload.| `0` (SDK default)     |
| `-download-max-buffered-chunks`| Maximum number of chunks buffered in memory during download. | `0` (SDK default) |

These match the `--upload-max-buffered-slabs` and `--download-max-buffered-chunks`
flags of the Rust benchmark in
[sia-sdk-rs](https://github.com/SiaFoundation/sia-sdk-rs), so the two can be
compared directly against the same account. Both are memory budgets that also
bound the SDK's adaptive transfer concurrency, not fixed concurrency counts.

## Example

First build the FFI library once (from the repo root) so cgo can link it:

```sh
./build.sh
```

Then, from the `examples` directory, authorize the app once and run the
benchmark. Follow the printed URL to authorize, then paste your recovery phrase
when prompted:

```sh
cd examples
go run ./benchmark login
```

Run the benchmark with 10 GiB of data, buffering up to 16 slabs during upload
and 32 chunks during download:

```sh
go run ./benchmark run -size $((10 * 1024 * 1024 * 1024)) -upload-max-buffered-slabs 16 -download-max-buffered-chunks 32
```
