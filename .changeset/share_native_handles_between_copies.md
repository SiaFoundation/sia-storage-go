---
default: patch
---

# Share native handles between copies of a handle type

Copying an `Object`, `SDK`, `Builder`, `SharedSDK`, `SharingKey`, `Download`, `PackedUpload` or `MockNetwork` by value no longer leaves the copy pointing at freed memory once the original is collected, and closing two copies no longer frees the handle twice. Copies now share one native handle.
