---
default: minor
---

# Pin slabs implicitly on upload

`Upload` and `UploadPacked` now pin each slab to the indexer as soon as it finishes uploading, so its sectors are protected before the object itself is pinned. `PinObject` attempts to save the object first and only pins slabs when the indexer reports one is missing, which makes pinning an uploaded object a single request.
