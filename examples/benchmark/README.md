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

| Flag                      | Description                                         | Default               |
|---------------------------|-----------------------------------------------------|-----------------------|
| `-size`                   | Size of the data to upload and download, in bytes.  | `125829120` (120 MiB) |
| `-upload-max-inflight`    | Maximum number of concurrent shard uploads.         | `0` (SDK default)     |
| `-download-max-inflight`  | Maximum number of concurrent chunk downloads.       | `0` (SDK default)     |

## Example

Run the benchmark with 10 GiB of data, 16 concurrent uploaders and 32 concurrent
downloaders:

```sh
go run . -size $((10 * 1024 * 1024 * 1024)) -upload-max-inflight 16 -download-max-inflight 32
```

Follow the printed URL to authorize the app, then paste your recovery phrase
when prompted.
