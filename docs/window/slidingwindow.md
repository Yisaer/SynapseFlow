# slidingwindow

`slidingwindow(time_unit, lookback [, lookahead])` defines per-row triggered time ranges.

See also: `docs/window/syntax.md`, `docs/window/watermarks.md`, and `docs/window/rfc_slidingwindow.md`.

## Parameters

- `time_unit`: string literal (currently only `'ss'` is supported).
- `lookback`: unsigned integer literal (duration).
- `lookahead`: optional unsigned integer literal (duration).

## Semantics

Each incoming tuple is a trigger point with timestamp `t`:

- `slidingwindow('ss', lookback)`:
  - range: `[t - lookback, t]`
  - emission: immediate (on receiving the trigger tuple)
- `slidingwindow('ss', lookback, lookahead)`:
  - range: `[t - lookback, t + lookahead]`
  - emission: delayed until the operator observes a watermark `>= t + lookahead`

### Watermark contract for lookahead

Delayed emission is entirely watermark-driven. The upstream pipeline must eventually produce
watermarks that reach each trigger's deadline (`t + lookahead`):

- In processing-time mode, `SlidingWatermarkProcessor` emits periodic processing-time watermarks
  (tick interval is `1s`) and, when `lookahead` is present, generates per-tuple deadline
  processing-time watermarks.
- In event-time mode, the deadline watermark behavior is controlled by the upstream event-time
  watermark stream.
