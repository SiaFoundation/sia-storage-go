---
default: patch
---

# Assemble and encode slabs off the upload read loop

Uploads read whole slabs and move encryption and erasure coding into per slab goroutines. This removes the 64 byte striped reads and the single threaded cipher from the upload hot path.
