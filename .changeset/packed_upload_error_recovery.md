---
default: major
---

# Packed uploads recoverable on reader error

If the reader passed to `Add` errors mid-stream, partial bytes become dead padding in the slab instead of killing the upload. Subsequent `Add` calls continue working.

Rename `SlabSize` to `OptimalDataSize` to distinguish the data portion from the full encoded slab size.
