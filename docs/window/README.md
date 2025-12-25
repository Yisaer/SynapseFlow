# StreamDialect Windows

StreamDialect expresses stream windows as window functions that appear in the `GROUP BY` clause.

This directory documents:
- Syntax rules and parser behavior: `docs/window/syntax.md`
- Window semantics: `docs/window/tumblingwindow.md`, `docs/window/slidingwindow.md`,
  `docs/window/countwindow.md`, `docs/window/statewindow.md`
- Watermark-driven execution model: `docs/window/watermarks.md`
- Sliding window RFC / implementation status: `docs/window/rfc_slidingwindow.md`

## Parser Contract

- Window functions are only allowed in `GROUP BY`.
- At most one window function is allowed per statement.
- When present, the parser extracts the window into `SelectStmt.window`.
- All other `GROUP BY` expressions remain in `SelectStmt.group_by_exprs`.

Example:

```sql
SELECT * FROM stream GROUP BY tumblingwindow('ss', 10), b, c
```

After parsing:
- `SelectStmt.window = Some(Window::Tumbling { ... })`
- `SelectStmt.group_by_exprs = [b, c]`
