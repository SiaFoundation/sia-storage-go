---
default: patch
---

# SDK: Skip duplicate hosts when downloading a slab

Sectors are keyed by host when recovering a slab, so two sectors on the same host collapsed into a single entry while the host list kept both. The same shard could then be downloaded twice and counted twice towards the slab's minimum shards, returning with a shard still missing and failing with "too few shards given". Duplicate hosts are now skipped when the sector map is built, which also keeps the host list and the sector map the same length. The indexer rejects slabs with duplicate host keys at pin time, so this is defensive.
