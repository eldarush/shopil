# Experimental constrained Hebrew narrative

`--hybrid` (default off) is mutually exclusive with `--structured`. It retains
Actor-authored Hebrew sentence choices/order, a small explanation vocabulary,
and conversation context. Every factual sentence must match an exact supported
Hebrew pattern and contain a typed evidence placeholder. The metric in the
sentence must match the referenced field. Values, row identifiers and source
metadata are resolved from the frozen evidence; the model cannot write literal
values, dates, entity labels or additional prose. Critic sees the exact rendered
candidate and may reject it for up to three attempts. The audit preserves raw
Actor JSON, rendered draft and `constrained_hebrew_narrative` rendering identity.

Currently supported: twelve calculated metric fields, one or two Hebrew clause
forms per field, three optional sentence prefixes, at most six sentences, and
three conditional explanatory statements (ratings versus revenue, stream orders
versus paid orders, open windows provisional). Extra prose, literal numbers
(including numbers in words), forged entity labels, wrong metric/field bindings,
unknown references and inapplicable explanations fail closed before
Critic approval can publish them. Missing/unsupported information yields a
conservative inability-to-verify response, not an invented claim that the entire
warehouse lacks the data. Follow-up context is provided to the Actor, not used
as a source of numeric facts.

Important limits: this is a finite language, NOT arbitrary model-authored natural
language. It supports no review-text explanation, trend synthesis, mixed
available/unavailable narrative or freely worded alternatives. Selecting the
wrong row/metric or misinterpreting a follow-up remains a semantic error which a
weak Critic may approve. A product3 rating is explicitly rendered as product3;
this prevents relabeling it as product5, not selecting the wrong product in the
first place. Correct candidates may still be falsely rejected. The restricted
explanations might not satisfy the assignment's intended Actor wording and
explanation role. No claim of rubric compliance or reliable general answering is
made. Keep the original mode available, and independently evaluate before any
adoption, merge or final submission.

## Generation and presentation safeguards

The generation schema enumerates complete permitted sentences with actual
snapshot references and has disjoint answer/refusal branches. Reference aliases
expose original source identifiers, for example `s2:id_product_id_5:avg_rating`;
they do not infer product IDs from row positions. Rows without identifiers retain
zero-based row references. Duplicate source identifiers fail closed. The full
frozen evidence stays unchanged; the prompt repeats only a compact reference
index. Schema size is capped, but byte limits are not model-token guarantees.

Valid repeated references may be deduplicated as presentation-only normalization.
Every original clause is validated first. The first sentence's exact wording is
retained; no entity, field, reference or value is changed. The complete raw Actor
JSON and each removed sentence/index are recorded in attempt audit metadata.
Invalid clauses are never hidden by deduplication. Malformed backend outputs
remain errors and are retained in audits, not repaired into purported answers.
Wrong entity selection remains wrong after deduplication and still requires
semantic review; no guaranteed question-understanding claim is made.

## Explicit capture-supported missing metric

Fresh `build_capture` results include a bounded `metric_availability` manifest
for the known fixed readers. It describes only the supplied snapshot. The current
registry can explain absent shipping charges, because these readers do not query
shipping charges; it does not infer arbitrary absent business metrics. A supplied
shipping field, including NULL or zero, disables that absence claim. Unknown or
partial source collections, stale/forged manifests, and available metrics cannot
authorize it. NULL is a supplied but unknown value, not an absent metric.

The optional `missing_metric` state is generated only when this manifest explicitly
supports it. Its Hebrew explanation names the metric, limits absence to the current
snapshot, distinguishes it from zero, and offers an actually supplied non-NULL
alternative when one exists (zero is a legitimate alternative value). It does not
claim the entire warehouse lacks the data. Questions outside the bounded registry
still use `cannot_verify`; those refusals are not evidence of missing data. The
Actor/Critic must still determine whether the declared metric matches the user's
question. No universal question-understanding or rubric-compliance claim follows.
