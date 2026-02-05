import os
import json
import logging
import random
import urllib.request
from uuid import uuid4

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]

YMQ_ENDPOINT = os.environ.get(
    "YMQ_ENDPOINT", "https://message-queue.api.cloud.yandex.net"
)
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://storage.yandexcloud.net")
S3_BUCKET = os.environ["S3_BUCKET"]

RANDOM_POOL_QUEUE_URL = os.environ["RANDOM_POOL_QUEUE_URL"]
RANDOM_PHOTO_POOL_QUEUE_URL = os.environ["RANDOM_PHOTO_POOL_QUEUE_URL"]

POOL_LOW_WATERMARK = int(os.environ.get("POOL_LOW_WATERMARK", "15"))
POOL_TARGET_SIZE = int(os.environ.get("POOL_TARGET_SIZE", "25"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))

QUEUE_URL_BY_POOL = {
    "random": RANDOM_POOL_QUEUE_URL,
    "random_photo": RANDOM_PHOTO_POOL_QUEUE_URL,
}

# ---------------------------------------------------------------------------
# AWS-compatible clients (Yandex Cloud)
# ---------------------------------------------------------------------------
_sqs = boto3.client(
    "sqs",
    endpoint_url=YMQ_ENDPOINT,
    region_name="ru-central1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

_s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    region_name="ru-central1",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# ---------------------------------------------------------------------------
# Dataset loading (shared with the bot — same ids.txt format)
# ---------------------------------------------------------------------------

def load_ids_from_file(filepath: str | None = None) -> dict[int, tuple]:
    if filepath is None:
        filepath = os.path.join(os.path.dirname(__file__), "ids.txt")
    result: dict[int, tuple] = {}
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            dataset_str, ranges_str = line.split(":", 1)
            dataset_id = int(dataset_str)
            ids: list[int] = []
            for part in ranges_str.split(","):
                if "-" in part:
                    start, end = part.split("-", 1)
                    ids.extend(range(int(start), int(end) + 1))
                else:
                    ids.append(int(part))
            result[dataset_id] = tuple(ids)
    return result


IDS_BY_DATASET = load_ids_from_file()
AVAILABLE_DATASETS = list(IDS_BY_DATASET.keys())

# ---------------------------------------------------------------------------
# PDF fetching / conversion helpers (mirror of bot logic)
# ---------------------------------------------------------------------------

def get_random_epstein_doc_url(
    dataset: int | None = None,
) -> tuple[str, str, int]:
    """Return (url, file_id, dataset_number)."""
    if dataset is not None and dataset in IDS_BY_DATASET:
        chosen = dataset
    else:
        chosen = random.choice(AVAILABLE_DATASETS)
    num = random.choice(IDS_BY_DATASET[chosen])
    file_id = f"EFTA{num:08d}"
    url = (
        f"https://www.justice.gov/epstein/files/"
        f"DataSet%20{chosen}/{file_id}.pdf"
    )
    return url, file_id, chosen


def download_pdf(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36"
                ),
                "Cookie": "justiceGovAgeVerified=true",
            },
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read()
    except Exception as e:
        logger.error("download_pdf failed: %s", e)
        return None


def pdf_first_page_to_jpeg(pdf_bytes: bytes) -> bytes | None:
    import fitz  # PyMuPDF — lazy import to keep cold start fast

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page = doc[0]
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
        jpeg_bytes = pix.tobytes("jpeg", jpg_quality=75)
        doc.close()
        return jpeg_bytes
    except Exception as e:
        logger.error("pdf_first_page_to_jpeg failed: %s", e)
        return None

# ---------------------------------------------------------------------------
# Pool management
# ---------------------------------------------------------------------------

def get_approximate_count(queue_url: str) -> int:
    resp = _sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages"],
    )
    return int(resp["Attributes"].get("ApproximateNumberOfMessages", 0))


def generate_and_enqueue(pool_name: str, queue_url: str) -> bool:
    """Generate one preview and push it to the pool queue.

    Retries up to 7 random documents before giving up.
    Returns True on success.
    """
    dataset = 2 if pool_name == "random_photo" else None

    for attempt in range(7):
        url, file_id, ds = get_random_epstein_doc_url(dataset=dataset)

        pdf_bytes = download_pdf(url)
        if not pdf_bytes:
            logger.warning("attempt %d: download failed for %s", attempt + 1, file_id)
            continue

        jpeg_bytes = pdf_first_page_to_jpeg(pdf_bytes)
        if not jpeg_bytes:
            logger.warning("attempt %d: convert failed for %s", attempt + 1, file_id)
            continue

        # Unique key to avoid overwrites from concurrent invocations
        s3_key = f"previews/{pool_name}/{file_id}-{uuid4().hex[:8]}.jpg"

        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=jpeg_bytes,
            ContentType="image/jpeg",
        )

        message = {
            "s3_key": s3_key,
            "original_url": url,
            "file_id": file_id,
            "dataset": ds,
        }
        _sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
        )

        logger.info("enqueued %s → %s pool", file_id, pool_name)
        return True

    logger.error("gave up generating preview for %s pool after 7 attempts", pool_name)
    return False


def fill_pool(pool_name: str) -> int:
    """Top up a single pool queue. Returns number of previews added.

    Only starts filling when count drops below POOL_LOW_WATERMARK (15).
    Fills up to POOL_TARGET_SIZE (25).
    """
    queue_url = QUEUE_URL_BY_POOL[pool_name]
    current = get_approximate_count(queue_url)
    logger.info("pool '%s': %d (low=%d, target=%d)", pool_name, current, POOL_LOW_WATERMARK, POOL_TARGET_SIZE)

    if current >= POOL_LOW_WATERMARK:
        logger.info("pool '%s' above low watermark — skipping", pool_name)
        return 0

    needed = min(BATCH_SIZE, POOL_TARGET_SIZE - current)
    added = 0

    for i in range(needed):
        if generate_and_enqueue(pool_name, queue_url):
            added += 1
        logger.info(
            "pool '%s': generated %d / %d (total now ~%d)",
            pool_name, added, needed, current + added,
        )

    return added

# ---------------------------------------------------------------------------
# Cloud function entry point
# ---------------------------------------------------------------------------

def handler(event, context):
    """Triggered by:
    1. YMQ trigger (refill-requests queue) — event["messages"] present.
       Each message body: {"pool": "random"} or {"pool": "random_photo"}.
    2. HTTP / timer trigger — no messages, fills all pools.
    """
    if "messages" in event:
        # Deduplicate: if multiple refill-requests arrived for the same pool
        # in one batch, fill once per unique pool name.
        pools_to_fill: set[str] = set()
        for msg in event["messages"]:
            body = json.loads(msg["details"]["message"]["body"])
            pool = body.get("pool", "random")
            if pool in QUEUE_URL_BY_POOL:
                pools_to_fill.add(pool)

        for pool_name in pools_to_fill:
            fill_pool(pool_name)

        return {"statusCode": 200, "body": "ok"}

    # HTTP / timer trigger — refill everything
    for pool_name in QUEUE_URL_BY_POOL:
        fill_pool(pool_name)

    return {"statusCode": 200, "body": "ok"}
