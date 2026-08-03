---
default: minor
---

# SDK: Add UnpinSlab

`UnpinSlab` releases a single slab immediately, regardless of how long ago it was pinned. Previously the only slab-releasing call on the SDK was `PruneSlabs`, which the indexer limits to slabs older than `api.DefaultSlabPruneCutoff` (72 hours).
