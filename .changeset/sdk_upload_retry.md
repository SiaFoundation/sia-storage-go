---
default: patch
---

# SDK: Fix upload retry logic and timeout

Retry all upload errors up to 3 attempts per host instead of only
retrying `context.DeadlineExceeded`. The previous check missed
`os.ErrDeadlineExceeded` returned by the network layer, causing
timed out hosts to be permanently removed from the upload queue.

The per-attempt timeout is now a flat 90s instead of a progressive
15s to 120s ramp, matching the Rust SDK.
