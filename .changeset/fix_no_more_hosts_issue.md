---
default: patch
---

# Fix no more hosts issue

Issues fixed by this PR.  Some are also present in Rust and should probably be fixed there as well:

- A host's attempt count (which when it exceeds maxHostAttempts result in the host being removed from the pool) was incremented when the host was merely picked.  The Rust SDK only incremented it when the host failed an operation.  We are now consistent with the Rust SDK on this.
- After a window where every write failed, the controller halved the limit but also threw away its baseline, so the next window looked like a cold start and it doubled right back up to the level that had just timed everything out.  On a throttled test run it just bounced between 16 and 32 in flight forever.  Solution: settle at the last limit that actually worked (or half the current one if there isn't one) and start measuring again from there instead of probing back up.  The Rust code is slightly different here but a similar issue is present there I think too.

It also contains some miscellaneous improvements like regression tests, making the "no more hosts" error more detailed, and a few other things.

