preview-tabular-data
====================

Extract the first n lines of tabular dataset.

## API

```
import { head } from 'preview-tabular-data';

head(readable, contentType, opts, (err, head, columns) => {
 // do something with head and columns
});
```

`opts` is an object with:

- `nHead`, the number of rows to extract for the head (defaults to
  `Infinity`).
- `nSample`, the number of rows to use to comput the
  [columns](http://www.w3.org/TR/tabular-metadata/#columns)
  description. (defaults to `Infinity`)


## Tests

```npm test```
