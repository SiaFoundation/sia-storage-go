---
default: patch
---

# SDK: Document the prune cutoff on PruneSlabs

`PruneSlabs` now documents that the indexer only prunes slabs older than `api.DefaultSlabPruneCutoff` (72 hours), and that `api.WithBefore` overrides that cutoff. No behaviour change.
