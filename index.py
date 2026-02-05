import os
import io
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

BOT_TOKEN = os.environ["BOT_TOKEN"]
DUMP_CHAT_ID = os.environ["DUMP_CHAT_ID"]  # private channel for caching files

RANDOM_POOL_QUEUE_URL = os.environ["RANDOM_POOL_QUEUE_URL"]
RANDOM_PHOTO_POOL_QUEUE_URL = os.environ["RANDOM_PHOTO_POOL_QUEUE_URL"]

POOL_LOW_WATERMARK = int(os.environ.get("POOL_LOW_WATERMARK", "25"))
POOL_TARGET_SIZE = int(os.environ.get("POOL_TARGET_SIZE", "35"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5"))

QUEUE_URL_BY_POOL = {
    "random": RANDOM_POOL_QUEUE_URL,
    "random_photo": RANDOM_PHOTO_POOL_QUEUE_URL,
}

# ---------------------------------------------------------------------------
# SQS client (Yandex Message Queue)
# ---------------------------------------------------------------------------
_sqs = boto3.client(
    "sqs",
    endpoint_url=YMQ_ENDPOINT,
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
# PDF fetching / conversion helpers
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
# Telegram upload — send photo to dump channel, get file_id back
# ---------------------------------------------------------------------------

def upload_photo_to_telegram(jpeg_bytes: bytes, caption: str) -> str | None:
    """Send photo to dump channel via Telegram API, return file_id.

    Returns the file_id of the largest photo size, or None on failure.
    """
    try:
        boundary = "----FormBoundary" + uuid4().hex[:16]
        body = io.BytesIO()

        body.write(f"--{boundary}\r\n".encode())
        body.write(b'Content-Disposition: form-data; name="chat_id"\r\n\r\n')
        body.write(f"{DUMP_CHAT_ID}\r\n".encode())

        body.write(f"--{boundary}\r\n".encode())
        body.write(b'Content-Disposition: form-data; name="caption"\r\n\r\n')
        body.write(f"{caption}\r\n".encode())

        body.write(f"--{boundary}\r\n".encode())
        body.write(b'Content-Disposition: form-data; name="parse_mode"\r\n\r\n')
        body.write(b"Markdown\r\n")

        body.write(f"--{boundary}\r\n".encode())
        body.write(b'Content-Disposition: form-data; name="photo"; filename="page.jpg"\r\n')
        body.write(b"Content-Type: image/jpeg\r\n\r\n")
        body.write(jpeg_bytes)
        body.write(b"\r\n")

        body.write(f"--{boundary}--\r\n".encode())

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
        req = urllib.request.Request(
            url,
            data=body.getvalue(),
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )

        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))

        if not result.get("ok"):
            logger.error("sendPhoto not ok: %s", result)
            return None

        # photo array sorted by size — last element is the largest
        photos = result["result"].get("photo", [])
        if not photos:
            logger.error("sendPhoto returned no photo sizes")
            return None

        return photos[-1]["file_id"]

    except Exception as e:
        logger.error("upload_photo_to_telegram failed: %s", e)
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
    """Generate one preview, upload to Telegram, enqueue file_id.

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

        caption = f"[{file_id}]({url})"
        tg_file_id = upload_photo_to_telegram(jpeg_bytes, caption)
        if not tg_file_id:
            logger.warning("attempt %d: telegram upload failed for %s", attempt + 1, file_id)
            continue

        message = {
            "tg_file_id": tg_file_id,
            "original_url": url,
            "file_id": file_id,
            "dataset": ds,
        }
        _sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message),
        )

        logger.info("enqueued %s → %s pool (tg_file_id cached)", file_id, pool_name)
        return True

    logger.error("gave up generating preview for %s pool after 7 attempts", pool_name)
    return False


def fill_pool(pool_name: str) -> int:
    """Top up a single pool queue. Returns number of previews added.

    Only starts filling when count drops below POOL_LOW_WATERMARK.
    Fills up to POOL_TARGET_SIZE.
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
