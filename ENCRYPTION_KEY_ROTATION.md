# Encryption key rotation

ChronoQueue encrypts message and schedule payloads with AES-GCM. Every newly encrypted payload records a SHA-256 fingerprint of the active key as `encryptionKeyId`; the key itself is never stored with the payload. Payloads written before key identifiers were introduced remain readable by trying the configured key set.

The active key encrypts new payloads. The active key and every historical key can decrypt existing payloads. All keys must be 16, 24, or 32 bytes.

## Key source configuration

For `LOCAL`, set the active key in `ENCRYPTION_KEY` and historical keys as a JSON array in `ENCRYPTION_PREVIOUS_KEYS`:

```text
ENCRYPTION_KEY=abcdef0123456789
ENCRYPTION_PREVIOUS_KEYS=["0123456789abcdef"]
```

For `VAULT`, the secret at `VAULT_SECRET_PATH` must contain the same logical key set:

```json
{
  "key": "abcdef0123456789",
  "previous_keys": '["0123456789abcdef"]'
}
```

Both KV v1 data and the nested `data` object returned by a KV v2 data path are supported. Vault is the recommended production source. `LOCAL` requires a coordinated restart because an existing process cannot observe environment changes made outside its environment.

## Rotation

1. Back up the database and the current key set independently. Confirm that both can be restored.
2. Generate a new key with a cryptographically secure generator. Do not derive it from a password.
3. Atomically publish the new key as `key` and append the old active key to `previous_keys`. Retain all older entries that may still encrypt stored payloads.
4. Wait for `KEY_REFRESH_DURATION_IN_MINUTES`, or restart instances one at a time. When a replica encounters an unknown key identifier, it performs one immediate provider refresh before returning an error.
5. Verify that records created before and after the change can both be consumed and that recurring schedules still execute.

During its lifetime, a process retains every valid key it has observed. Do not rely on that cache: publish the old key in `previous_keys`, or a process restart will lose the only available copy.

## Rollback

Publish the former active key as `key` and keep the unsuccessful new key plus every older required key in `previous_keys`. After refresh, new writes use the restored key while records written during the attempted rotation remain readable.

## Historical-key retirement and re-encryption

ChronoQueue does not currently provide a bulk re-encryption command. New records use the active key, but reading an existing record does not rewrite it. Keep a historical key while any queued, retained, dead-lettered, or scheduled record may reference it. In particular, elapsed time alone is not evidence that a key is unused when retention is unbounded or a schedule remains stored.

To retire a key with the current implementation, drain or delete every record encrypted by that key and recreate any long-lived schedules under the active key. Validate old and new payload reads from every storage backend before removing the historical key. Because live processes retain observed keys, restart one instance and repeat the read checks before completing the rollout.

## Missing-key recovery

An unavailable key produces an error containing its key identifier; ChronoQueue does not overwrite the affected ciphertext. Restore the exact missing key into `previous_keys` and allow the manager to refresh. AES-GCM ciphertext cannot be recovered if the key has been permanently lost, so key-set backups must be retained for at least as long as encrypted data.
