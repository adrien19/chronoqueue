# API validation and error contract

ChronoQueue applies the same validation and error mapping to gRPC requests and requests received through the HTTP gateway. Invalid-argument responses include `google.rpc.BadRequest` field violations when a specific field caused the rejection.

## Error mapping

| Condition | gRPC code | HTTP status |
| --- | --- | --- |
| Invalid request or configuration | `InvalidArgument` | `400 Bad Request` |
| Resource does not exist | `NotFound` | `404 Not Found` |
| Resource identifier already exists | `AlreadyExists` | `409 Conflict` |
| Resource state or attempt ownership prevents the operation | `FailedPrecondition` | `400 Bad Request` |
| Message lease or request deadline expired | `DeadlineExceeded` | `504 Gateway Timeout` |
| Unclassified server or storage failure | `Internal` | `500 Internal Server Error` |

Internal errors use the public message `internal server error`. Database and encryption details are logged by the server but are not returned to clients.

## Queue creation

- `name` is required, is at most 255 characters, starts with an alphanumeric character, and otherwise contains only alphanumeric characters, `.`, `_`, or `-`.
- `type` must be `SIMPLE` or `EXCLUSIVE`. `EXCLUSIVE` requires a non-blank `exclusivity_key`.
- `default_max_attempts` accepts `-1` for unlimited attempts, `0` for the server default of 3, or a positive value.
- `lease_duration` and every duration in `lease_policy` must be valid protobuf durations. The base lease must be greater than zero and no longer than one hour. When both legacy `lease_duration` and `lease_policy.base_lease` are supplied, they must match.
- Lease extension durations cannot be negative. A heartbeat timeout requires a positive extension budget and extension step. `max_renewals` cannot be negative.
- `schema_required: true` requires a non-blank `schema_id`.
- `max_payload_size` cannot be negative. Every `allowed_content_types` entry must be a supported MIME type.
- Priority weight keys are the implemented low/medium/high buckets `0`, `2`, and `4`, and configured weights are positive. `STRICT` accepts neither weight nor age settings, `WEIGHTED` accepts weights, `AGING` accepts age settings, and `HYBRID` accepts both. Age-boost durations are positive and the multiplier cannot be negative.
- `RETAIN_DURATION` requires positive `retention_seconds`; other retention modes require it to be zero.

## Message posting

- `queue_name`, `message`, `message.message_id`, message metadata, and payload are required.
- Message IDs are 1–256 characters using alphanumeric characters, `_`, or `-`.
- Priority is inclusive from 0 through 4.
- `max_attempts` is `-1` for unlimited attempts or a positive value after queue defaults are applied.
- Message lease-policy fields follow the queue lease rules. Unset duration fields inherit the queue policy; `lease_duration` remains a compatible base-lease override.
- `scheduled_time` must be a valid protobuf timestamp.
- Runtime fields (`state`, `lease_expiry`, `lease_renewal_count`, `current_attempt`, and `priority_level`) are server-managed and cannot be supplied when posting.
- Payload size, metadata size, content type, and configured schema are validated before persistence.
- Bulk posting accepts 1–1000 messages and at most 1 MiB of serialized message data. Validation failure rejects `ALL_OR_NOTHING` batches with `FailedPrecondition`; `BEST_EFFORT` reports individual failures in its response. Schema-originated failures use `SCHEMA_MISMATCH`; queue lookup failure is an RPC-level `NotFound` because every item targets the request's single `queue_name`.

## Scheduling

- `calendar_schedule.timezone` is the canonical timezone for calendar validation, preview, and execution.
- The deprecated `schedule.metadata.timezone` may be omitted. If supplied for compatibility, it must equal `calendar_schedule.timezone`.
- `ValidateCalendarSchedule` is a validation-result endpoint: invalid calendar content returns an OK transport status with `valid: false` and structured `validation_issues`. Transport or server failures still use non-OK status codes.

## Dead-letter queue listing

- `dlq_name` is required.
- `limit` must be between 0 and 1000. A value of 0 uses the server default of 100.
- DLQ administration requires the named queue to be referenced by at least one source queue's `dead_letter_queue_name`; ordinary queues return `FailedPrecondition`.
- Posting and worker claims against a referenced DLQ return `FailedPrecondition`; DLQ messages are managed through the DLQ operations.

## Claim and lifecycle requests

- Claim lease overrides and lease-renewal durations must be valid and greater than zero.
- Acknowledgment, heartbeat, and renewal require the active `attempt_id` and `worker_id`.
- Missing messages return `NotFound`, stale ownership or invalid state returns `FailedPrecondition`, and expired leases return `DeadlineExceeded`.
