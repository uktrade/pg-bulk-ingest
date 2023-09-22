---
layout: sub-navigation
order: 7
title: Utility functions
---

pg-bulk-ingest currently provides one utility function.

---

`to_file_like_object(iterable, base)`

Convert an iterable into a file-like object.

- `iterable` - An iterable of data

- `base` - The type of this data e.g. str or bytes

`.read(size=-1)`

Read a returned file-like object in a streaming way.

- `size`(optional) - An integer with a default of -1, the amount of bytes or characters to be read from the object. If "None" or any integer < 1, the whole object will be read.