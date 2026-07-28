---
default: major
---

# Removed SlabReader

The upload pipeline reads raw slabs directly and stripes them off the read loop, leaving SlabReader without callers. SlabReader, NewSlabReader, and ReadSlab are removed.
