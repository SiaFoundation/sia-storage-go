---
default: patch
---

# Fix no more hosts issue (for real)

#65 by @chris124567

Issues fixed by this PR.  Some are also present in Rust and should probably be fixed there as well:

- Each upload had its own concurrency controller.  Because s3d uploads 8 objects simultaneously, all the controllers started at eight writes in flight and probed upward without knowing about each other. On slower connections that resulted in far more data in flight than could be finished inside the 90 second timeout, resulting in every write timing out.  This eventually resulted in all the hosts being removed from the pool because writes on them were repeatedly failing.  Solution: One limiter per SDK that every upload shares.  This is also present in the Rust SDK.  This was probably the largest contributor to the problem.
- ^ likewise with the memory budget.  This is also why the user reported very large RAM usage... the default budget is 10% of RAM per upload, so 8 uploads can take 80%.  Also present in Rust.
- A host's attempt count (which when it exceeds maxHostAttempts result in the host being removed from the pool) was incremented when the host was merely picked.  The Rust SDK only incremented it when the host failed an operation.  We are now consistent with the Rust SDK on this.
- After a window where every write failed, the controller halved the limit but also threw away its baseline, so the next window looked like a cold start and it doubled right back up to the level that had just timed everything out.  On a throttled test run it just bounced between 16 and 32 in flight forever.  Solution: settle at the last limit that actually worked (or half the current one if there isn't one) and start measuring again from there instead of probing back up.  The Rust code is slightly different here but a similar issue is present there I think too.

It also contains some miscellaneous improvements like regression tests, making the "no more hosts" error more detailed, and a few other things.

