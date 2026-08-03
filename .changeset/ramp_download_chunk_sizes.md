---
default: minor
---

# Ramp download chunk sizes and overprovision reads to match the Rust SDK

Downloads now start with a 32 KiB chunk for a fast first byte and double the
chunk size up to 1 MiB, amortizing the fixed cost of a read RPC over more
bytes. Each chunk also launches half again as many initial reads as needed and
takes the first successes, so a single slow host no longer stalls the chunk.
