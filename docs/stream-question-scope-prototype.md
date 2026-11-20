# Bounded streaming questions (hybrid mode only)

The experimental grammar adds four stream metric families, selecting existing
Python/SQL-computed values rather than asking the model to calculate:

- `מה סכום אירועי ההזמנה בחלון הפתוח האחרון?`
- `מה מספר אירועי ההזמנה בחלון הסגור האחרון שמופיע בנתונים?`
- `מה מספר הביקורות השליליות בזרם למוצר5 בחלון הפתוח האחרון?`
- `מה מספר הביקורות השליליות בעת ההתראה בזרם למוצר5 בחלון הפתוח האחרון?`

`מהו` is also supported. Open/closed may be exchanged; the phrase
`שמופיע בנתונים` is optional. Review and alert queries require an explicit product
ID. Order-window queries cannot be product-specific because their source rows
have no product-level amounts. Explicit product follow-ups retain the stream
metric and window-state context, never assume historical product-ID equivalence.

“Latest” means maximum event-time window start among the requested open/closed
windows represented in this frozen snapshot, not row order, wall-clock now, the
last hour, or all history. Establish that window across all retained stream
products/tables before filtering the requested metric/product. If its row is
absent, decline rather than using an older row or inventing zero. Empty/missing
closed windows are not evidence that no event ever occurred. No new sums or
absence claims are derived.

Rendered answers show both source coverage and the selected row's exact half-open
window bounds, checkpoint, watermark and open/provisional versus closed status.
They explicitly separate unpaid-status-unknown stream order amounts from paid
historical revenue and disclose snapshot-limited coverage. Review alert counts
are labeled as counts at trigger, not current negative-review counts. Stream
product IDs are not asserted to map to historical IDs. Actor can choose from
conditional explanations appropriate to the available facts; no unrestricted
interpretive prose is enabled.

Tests include shuffled windows, latest-window product/alert absence, trigger3
versus current4, unsupported paid/last-hour/product-order queries, stream-context
follow-ups, and a real uniquely accepted engine event while a MOCK language model
request is underway. That test proves frozen same-question evidence and updated
next-question open count; it is NOT proof of an actual model experiment. A fresh
live-model run must independently record the accepted unique event, its timing,
Actor/Critic shared evidence and next-question changed output before claiming the
assignment's concurrent-update experiment passed.

This expands a finite language but does not resolve the Actor wording/explanation
rubric concern. Standard mode is unchanged; no deployment/compliance claim follows.

## Actor-selected explanation emphasis

A factual stream answer requires the Actor to choose an applicable rationale.
For example, an open order window offers three distinct choices: explain why
the value may still change, explain the capture progress boundary, or explain
why order events cannot establish payment. The schema lists only applicable
choices, and the renderer rechecks applicability to the specific selected
facts before publication. Refusals keep explanation `none`. The code preserves
all mandatory scope, payment, window-state and product-identity caveats
independently of the model's choice. The selected rationale code is preserved
in the existing raw Actor output audit; the Hebrew rationale text is rendered
by code. Selecting a reasoning emphasis is not the same as the Actor composing
its own explanation, so the finite-language rubric tradeoff remains.
