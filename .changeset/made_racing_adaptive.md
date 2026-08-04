---
default: minor
---

# Made racing adaptive

Upload and download racing now derive their timeout from the observed network
throughput and only race when it will not steal capacity from higher priority
work: uploads race once every shard has an attempt in flight, and downloads
race only chunks near the read head. Brings the Go SDK to parity with the Rust
SDK.
