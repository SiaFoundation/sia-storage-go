---
default: patch
---

# Give host warmup enough room for distant hosts

A warmup probe had one second to complete a TCP handshake, the mux handshake and
a settings round trip, so hosts more than roughly 300ms away expired even when
they were perfectly healthy and never got their settings cached. The probe now
gets three seconds, and warmup runs more of them at once so warming the same set
of hosts still costs the same wall clock.
