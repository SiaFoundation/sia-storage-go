---
default: patch
---

# SDK: Batch slab pinning in PinObject

`PinObject` now sends slabs to the indexer in batches of 50 instead of a single request. This keeps the request size bounded for objects with many slabs.
