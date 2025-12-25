# countwindow

`countwindow(count)` defines fixed windows measured by number of rows (tuples), not by time.

See also: `docs/window/syntax.md`.

## Semantics

- Let `count` be a positive integer.
- The operator groups the input stream into successive batches of `count` tuples.
- Emission is triggered by data arrival (every `count` tuples), not by watermarks.

## Example

```sql
SELECT avg(price)
FROM quotes
GROUP BY countwindow(500);
```
