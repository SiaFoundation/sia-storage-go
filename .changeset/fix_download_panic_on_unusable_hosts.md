---
default: patch
---

# SDK: Fix panic when a slab has fewer usable hosts than its minimum shards

`Download` panicked with `index out of range [0] with length 0` when fewer than a slab's `MinShards` hosts were still usable. The host count was checked before `Prioritize`, which removes unusable hosts, so the initial batch could consume more hosts than remained. It now returns `ErrNotEnoughShards`. The panic occurred on a background goroutine, so callers could not recover from it.
