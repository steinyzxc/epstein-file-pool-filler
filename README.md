# epstein-file-pool-filler

Background worker that pre-renders Epstein document previews and keeps SQS queues topped up so the [bot](../epstein-mail-bot) can serve them instantly via Telegram `file_id`. Deployed as a Yandex Cloud Function.

## How it works

1. Picks a random document from `ids.txt`
2. Checks YDB cache for an existing Telegram `file_id`
3. On cache miss: downloads PDF from justice.gov → renders page 1 as JPEG → uploads to a private Telegram dump channel → caches the returned `file_id` in YDB
4. Enqueues a message to the appropriate SQS pool queue

Maintains two pools:
- `random` — documents from all datasets
- `random_photo` — documents from dataset 2 only

## Pool management

- `POOL_LOW_WATERMARK` (default 25) — filling starts only when queue size drops below this
- `POOL_TARGET_SIZE` (default 35) — target queue depth
- `BATCH_SIZE` (default 5) — max items to generate per invocation

## Triggers

- **SQS trigger** (refill-requests queue) — message body `{"pool": "random"}` or `{"pool": "random_photo"}`
- **Timer / HTTP** — fills all pools

## YDB cache

Table `file_cache`:

```sql
CREATE TABLE file_cache (
    doc_id Utf8,
    tg_file_id Utf8,
    pages Int32,
    PRIMARY KEY (doc_id)
);
```

To add the `pages` column to an existing table:

```sql
ALTER TABLE file_cache ADD COLUMN pages Int32;
```

To force re-caching (e.g. after schema change):

```sql
DELETE FROM file_cache;
```

## Monitoring

Pushes custom metrics to Yandex Monitoring (if `YC_FOLDER_ID` is set):
- `cache.hit_rate` — ratio of cache hits to total lookups
- `cache.hits` / `cache.misses` — counters

## Queue message format

```json
{
  "tg_file_id": "AgACAgIAA...",
  "original_url": "https://www.justice.gov/epstein/files/DataSet%202/EFTA00003200.pdf",
  "file_id": "EFTA00003200",
  "dataset": 2,
  "pages": 42
}
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `BOT_TOKEN` | yes | Telegram bot token (for uploading to dump channel) |
| `DUMP_CHAT_ID` | yes | Private channel ID for caching uploaded photos |
| `AWS_ACCESS_KEY_ID` | yes | YMQ / SQS credentials |
| `AWS_SECRET_ACCESS_KEY` | yes | YMQ / SQS credentials |
| `YDB_ENDPOINT` | yes | YDB Serverless endpoint |
| `YDB_DATABASE` | yes | YDB database path |
| `RANDOM_POOL_QUEUE_URL` | yes | SQS queue for random pool |
| `RANDOM_PHOTO_POOL_QUEUE_URL` | yes | SQS queue for random photo pool |
| `YMQ_ENDPOINT` | no | Defaults to `https://message-queue.api.cloud.yandex.net` |
| `YC_FOLDER_ID` | no | Enables Yandex Monitoring metrics |
| `POOL_LOW_WATERMARK` | no | Default 25 |
| `POOL_TARGET_SIZE` | no | Default 35 |
| `BATCH_SIZE` | no | Default 5 |

## Deployment

Automatic via GitHub Actions on push to `main`. Deploys to Yandex Cloud Functions (`epstein-file-pool-filler`, python314, 384MB, 300s timeout). Secrets pulled from Yandex Lockbox.

### GitHub Secrets required

- `YC_SA_KEY_JSON` — service account key JSON
- `YC_FOLDER_ID` — Yandex Cloud folder ID
- `LOCKBOX_SECRET_ID` — Lockbox secret containing all env vars

## Dependencies

- `PyMuPDF` — PDF rendering
- `boto3` — SQS (Yandex Message Queue)
- `ydb` — YDB Serverless cache
