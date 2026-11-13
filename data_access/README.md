# Evidence retrieval

SHOPIL-26–30 supply predefined SQL queries and a frozen evidence package. Each question needs one internally consistent SQLite read, including the checkpoint and watermark, and the completed historical batch output. Historical paid revenue and streaming order amounts must stay separate.

## Evidence payload

Build an `EvidenceSource` with row dictionaries for each metric or result set, then
create one `EvidencePackage(question=question, sources=(...))` per capture. The
async capture function passes `package.model_dump(mode="json")` to
`ChatService`. The service already reuses the serialized payload for every
attempt.

Each source supplies a relation, metric, units, identifier columns, and a
half-open time range. Use dates for historical calendar-day ranges and
timezone-aware datetimes for stream ranges. Streaming window sources include
the committed checkpoint, watermark (which can be null before the first
accepted event), and window state. Use separate sources for open and closed
windows. Encode exact decimal ILS values as strings and stream amounts as
integer agorot. An empty row list represents an empty result set. Zero is a
value in a returned row.

See the [shared contract](https://privaterelay-team-yb37vjhl.atlassian.net/browse/SHOPIL-9)
and [synthetic payload](../tests/fixtures/evidence_snapshot.json). The payload
does not require a confirmed cross-source product mapping. Keep historical and
stream values separate until the team validates the mapping and semantics.
