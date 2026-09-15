# Types Registry - Quickstart

Stores the platform's Global Type System (GTS) entities: Type Schemas and registered
Instances, versioned, dependency-aware, and materialized at admission so no consumer
recomputes an effective schema. Writes are asynchronous — a submission returns an
operation receipt and the per-entity outcome is polled.

**Features:**
- Submit-then-poll admission: `202` with an operation `Location`, one durable outcome per entity
- Optimistic concurrency through `expected_resource_version`; a stale version is an item outcome, never a `412`
- Deletion in two spellings (batch and single), blocked by live direct registered dependants
- Dry Run on every mutation: the whole check sequence against one snapshot, committing nothing
- `Idempotency-Key` on every mutation: a replay returns the same operation, a different body under the same key is a conflict
- Exact reads by canonical GTS identifier or by Registry Reference UUID; deleted entities stay readable as tombstones

Full API documentation: <http://127.0.0.1:8087/cf/docs>

## Surfaces

| Base path | What it is |
|---|---|
| `/types-registry/v1` | The pre-database contract, served from an in-memory repository. Unchanged from before the database landed, and removed once every consumer has moved |
| `/types-registry/v2` | The database-backed asynchronous surface described below. Interim by construction: it is promoted onto `/v1` when the in-memory path goes |

`/v2` is the **global platform-plane API**: every entity it manages is global, and
there is no tenant context to derive an owner from.

### The mutations are internal-only

No route in this gear is marked `exposed`, so none is registered with the gateway
as an externally published surface. For the mutations that is deliberate rather
than incidental: a platform-plane write needs an authenticated platform principal
and a policy decision before dispatch, and neither exists yet — an in-process gear
has no inbound platform-identity validator and the gateway has no platform
listener. They stay internal until a platform listener authenticates
`X-ToolKit-Internal-Token` / `PlatformIdentity` and a PDP decision precedes
dispatch.

The routes *are* in the served `OpenAPI` document, because gateway visibility and
spec inclusion are separate axes — so `/cf/docs` is where to read the full
contracts.

The examples below therefore point at whatever base the deployment gives this gear
internally, and deliberately do not spell a gateway path: treating these mutations
as an available external API is exactly what the missing identity gate forbids.

```bash
BASE="$TYPES_REGISTRY_INTERNAL"   # the base this gear is reachable at internally
```

## Examples

The registration policy is **closed by default**: only the platform vendor `cf` is
admitted, and other vendors need a region that lists them in `allowed_vendors`.
The examples use a `cf` identifier so they work against a stock configuration.

### Register a Type Schema, then poll the outcome

```bash
curl -s -X POST "$BASE/types-registry/v2/entities" \
  -H "Idempotency-Key: register-example-event-1" \
  -H "Content-Type: application/json" \
  -d '{
        "items": [{
          "gts_id": "gts.cf.core.example.event.v1~",
          "content": {
            "$id": "gts://gts.cf.core.example.event.v1~",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": { "email": { "type": "string" } }
          }
        }]
      }'
```

Response: **202 Accepted**, `Location: …/types-registry/v2/operations/{operation_id}`
and an advisory `Retry-After: 1`. Follow the `Location`:

```bash
curl -s "$BASE/types-registry/v2/operations/$OPERATION_ID" | python3 -m json.tool
```

```json
{
    "operation_id": "34af3e4e-4927-4a98-a028-d4c2fe9edc95",
    "kind": "registration",
    "dry_run": false,
    "status": "completed",
    "items": [
        {
            "gts_id": "gts.cf.core.example.event.v1~",
            "status": "succeeded",
            "resource_version": 1,
            "error": null
        }
    ]
}
```

`status` is progress only: `completed` means every item is terminal. The outcomes
are on the items.

Read it back — the effective artifacts were materialized at admission, so nothing
is recomputed here. The response also carries `gts_uuid`, the Registry Reference:

```bash
curl -s "$BASE/types-registry/v2/entities/gts.cf.core.example.event.v1~" \
  | python3 -m json.tool
```

### Rehearse a deletion, then perform it

Dry Run runs the whole check sequence — precondition, lifecycle, dependants — and
commits nothing. It still produces an operation you poll, so the prediction reads
exactly like the real outcome, minus the `resource_version` it did not assign:

```bash
curl -s -X DELETE \
  "$BASE/types-registry/v2/entities/gts.cf.core.example.event.v1~?expected_resource_version=1&dry_run=true" \
  -H "Idempotency-Key: rehearse-delete-1"
```

Drop `dry_run` to commit. The mode is part of the idempotency fingerprint, so the
commit needs its **own** key — reusing the dry run's is a `409`:

```bash
curl -s -X DELETE \
  "$BASE/types-registry/v2/entities/gts.cf.core.example.event.v1~?expected_resource_version=1" \
  -H "Idempotency-Key: delete-1"
```

The entity stays readable afterwards, now with `"lifecycle_status": "deleted"` and
its version advanced. Replaying that same key returns **200** with
`Idempotency-Replayed: true` instead of deleting twice.

`expected_resource_version` is **required and positive** on both deletion
spellings: deletion only targets an entity the caller has read. Absent, non-numeric
or zero is a synchronous `400`; a version that no longer matches is a `202`
followed by a terminal `precondition_failed` item, because the version can only be
checked authoritatively at admission. `If-Match` is refused rather than ignored —
the precondition is this field, not an entity validator.

To delete several entities, each with its own precondition, use the batch spelling.
An item's `key` takes either spelling — identifier or Registry Reference — and
outcomes come back keyed by GTS identifier in request order, which is how a caller
that deleted by reference matches results to requests:

```bash
curl -s -X POST "$BASE/types-registry/v2/entities:batchDelete" \
  -H "Idempotency-Key: delete-batch-1" \
  -H "Content-Type: application/json" \
  -d '{
        "items": [
          { "key": "gts.cf.core.example.event.v1~", "expected_resource_version": 1 },
          { "key": "d226dd5b-14c8-56da-a718-9cf29becaba1", "expected_resource_version": 2 }
        ]
      }'
```

A Registry Reference that resolves to no entity is a `404`, because the reference
is a one-way derivation of an identifier and there is no identifier to report an
outcome under. An *identifier* that names no entity is accepted and reported as a
terminal item failure instead.

`dry_run` is a body field here (`"dry_run": true`) and a query parameter on the
single spelling, and defaults to `false` on both.

For additional endpoints, see <http://127.0.0.1:8087/cf/docs>.
