# Benchmark

A small program that measures upload and download performance of the
`siastorage` SDK against `sia.storage`. It generates pseudo-random data from a
seed, uploads it, pins the resulting object, downloads it back, and verifies the
bytes match. It then prints throughput, TTFB, and the maximum gap between
successive writes during the download.

The `examples` directory is its own Go module; run the benchmark from inside
it. The SDK links against the Rust FFI library, so build it first if it hasn't
been already:

```sh
make lib        # from the repository root, requires a Rust toolchain
cd examples
```

## Commands

```
benchmark login    [--profile NAME] [--indexer URL] [--new]
benchmark run      [--profile NAME] [--size BYTES] [--max-buffered-slabs N]
                   [--max-buffered-chunks N] [--host-summary]
benchmark profiles
```

`login` walks through the connection approval flow once and stores the app key
in a profile; `run` reuses it. Profiles are stored in the same location and
format as the Rust benchmark (`sia-sdk-rs`), so profiles created by either tool
work in the other.

## Run flags

| Flag                    | Description                                          | Default               |
|-------------------------|------------------------------------------------------|-----------------------|
| `--profile`             | Profile to use.                                      | `default`             |
| `--size`                | Size of the data to upload and download, in bytes.   | `125829120` (120 MiB) |
| `--max-buffered-slabs`  | Maximum number of slabs buffered during upload.      | `0` (SDK default)     |
| `--max-buffered-chunks` | Maximum number of chunks buffered during download.   | `0` (SDK default)     |
| `--host-summary`        | Print a per-host breakdown after the run.            | off                   |

## Example

Authorize once, then run the benchmark with 1 GiB of data:

```sh
go run ./benchmark login
go run ./benchmark run --size $((1024 * 1024 * 1024)) --host-summary
```

Follow the printed URL to authorize the app, then paste your recovery phrase
when prompted.
