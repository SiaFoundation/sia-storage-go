---
default: minor
---

# Race slow hosts during uploads

Slow hosts are now automatically raced by spawning additional upload attempts after a timeout. The first successful write wins and remaining attempts are cancelled.
