---
default: patch
---

# Share one upload limiter across uploads

Issues fixed by this PR.  Both are also present in Rust and should probably be fixed there as well:

- Each upload had its own concurrency controller.  Because s3d uploads 8 objects simultaneously, all the controllers started at eight writes in flight and probed upward without knowing about each other. On slower connections that resulted in far more data in flight than could be finished inside the 90 second timeout, resulting in every write timing out.  This eventually resulted in all the hosts being removed from the pool because writes on them were repeatedly failing.  Solution: One limiter per SDK that every upload shares.  This is also present in the Rust SDK.  This was probably the largest contributor to the problem.
- ^ likewise with the memory budget.  This is also why the user reported very large RAM usage... the default budget is 10% of RAM per upload, so 8 uploads can take 80%.  Also present in Rust.
