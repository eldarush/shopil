# Narrow question-scope routing (hybrid mode only)

The default standard Actor path is unchanged. Experimental `--hybrid` now
recognizes a documented finite grammar, not arbitrary Hebrew or keywords.

Supported question forms start with `מה`, `מהי`, `מהו`, `מהן` or `מהם`, followed
by a configured metric alias and optionally `של מוצר N`, `עבור מוצר N` or
`מוצר N`. Whitespace and a single final question mark are normalized.

Metric families and representative aliases:
- Paid historical revenue: `ההכנסה מהזמנות ששולמו`, `הכנסה מהזמנות ששולמו`,
  `סך ההכנסות מהזמנות ששולמו`.
- Historical rating: `הדירוג הממוצע`, `דירוג ממוצע`, `ממוצע הדירוגים`,
  `הדירוג`, `דירוג`. A product ID is required for an answer because no global
  average is supplied by the fixed reader.
- Shipping charges: `עלויות המשלוח`, `עלויות משלוח`, `עלות המשלוח`,
  `עלות משלוח`.

`ומה לגבי מוצר N?`, `ומה עם מוצר N?` and `מה לגבי מוצר N?` inherit only the
metric from the preceding recognized approved user turn. Assistant prose cannot
change that metric. An intervening unrecognized user turn clears scope.

The whole question must match. Negations, comparisons, compound questions,
additional date/filter language, unknown metrics and ambiguous modifications are
not guessed. They receive the conservative unsupported-mode response, not an
assertion that the business lacks data. This is a substantial usability limit.

For recognized metrics, selectable references are limited to the requested metric
and explicit product ID. No product ID is inferred from row position. Overall
paid revenue uses the existing aggregate, never a sum invented from product rows.
The full frozen evidence remains available unchanged for the Critic and audit.
Known missing shipping charges require both a recognized shipping question and a
verified capture-scoped capability declaration. Only then is `missing_metric`
the allowed generation state. NULL/zero fields do not authorize absence, and an
unknown question cannot claim shipping absence even if the model attempts it.

No generated value or reference is silently replaced. Backend violations are
rejected and logged. Restricting references is not proof of general language
understanding, and the finite language may not satisfy the assignment's intended
Actor wording/explanation breadth. A real Actor/Critic remains involved, but
routing deliberately removes some semantic choices from the model. Do not
present the standard path as protected by these opt-in rules or claim rubric
compliance without independent assessment.
