# Benchmark

A small program that measures upload and download performance of the
`siastorage` SDK against `sia.storage`. It generates pseudo-random data from a
seed, uploads it, pins the resulting object, downloads it back, and verifies the
bytes match. It then prints throughput, TTFB, and the maximum gap between
successive writes during the download.

## How it works

1. Requests a connection to `sia.storage` and prints an authorization URL.
2. Waits for the user to approve the connection in the browser.
3. Prompts for the wallet recovery phrase on stdin and registers the app.
4. Uploads `-size` bytes of seeded random data.
5. Pins the uploaded object.
6. Downloads the object, verifying each byte against the seeded stream.
7. Prints upload/download elapsed time, throughput (raw and encoded), TTFB, and
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

Run the benchmark with 10 GiB of data, buffering up to 16 slabs during upload
and 32 chunks during download:

```sh
go run ./examples/benchmark -size $((10 * 1024 * 1024 * 1024)) -upload-max-buffered-slabs 16 -download-max-buffered-chunks 32
```

Follow the printed URL to authorize the app, then paste your recovery phrase
when prompted.
