---
default: patch
---

# Assemble and encode slabs off the stream loops

Downloads now assemble and decrypt each chunk inside the recovery workers and write whole plaintext buffers to the stream. Uploads read whole slabs and move encryption and erasure coding into per slab goroutines. This removes the 64 byte striped writes and the single threaded cipher from both hot paths.
