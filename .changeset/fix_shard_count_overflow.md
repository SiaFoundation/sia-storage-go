---
default: patch
---

# SDK: Fix silent slab corruption at the maximum redundancy

`collectSlabs` summed `dataShards` and `parityShards` as `uint8` while the rest of the upload path widened them to `int`. A redundancy summing to exactly 256, such as `WithRedundancy(128, 128)`, wrapped the total to zero, so the shard results were never collected and the upload appended a slab with no sectors. The upload reported success and the object was undownloadable.
