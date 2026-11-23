from hashlib import sha256
from hmac import compare_digest, new as hmac_new
from urllib.parse import quote


MEDIA_TYPES = (
    "audio",
    "document",
    "photo",
    "sticker",
    "animation",
    "video",
    "voice",
    "video_note",
)


def get_media_from_message(message):
    for media_type in MEDIA_TYPES:
        media = getattr(message, media_type, None)
        if media:
            return media_type, media
    return None, None


def get_file_name(message):
    media_type, media = get_media_from_message(message)
    if media is None:
        return "file"

    if file_name := getattr(media, "file_name", None):
        return file_name

    extensions = {
        "photo": "jpg",
        "audio": "mp3",
        "voice": "ogg",
        "video": "mp4",
        "animation": "mp4",
        "video_note": "mp4",
        "sticker": "webp",
    }

    suffix = f".{extensions[media_type]}" if media_type in extensions else ""
    return f"{media_type}-{message.id}{suffix}"


STREAM_HASH_LENGTH = 16

# Content types that a browser could execute when rendered inline.
ACTIVE_MIME_TYPES = {
    "text/html",
    "application/xhtml+xml",
    "image/svg+xml",
    "text/xml",
    "application/xml",
    "text/javascript",
    "application/javascript",
    "application/x-javascript",
    "application/pdf",
}


def _stream_secret(secret):
    if not secret:
        raise ValueError("Stream secret is empty")
    return secret.encode() if isinstance(secret, str) else secret


def make_stream_hash(secret, chat_id, message_id, file_unique_id):
    payload = f"{chat_id}:{message_id}:{file_unique_id}".encode()
    digest = hmac_new(_stream_secret(secret), payload, sha256).hexdigest()
    return digest[:STREAM_HASH_LENGTH]


def verify_stream_hash(secret, value, chat_id, message_id, media):
    file_unique_id = getattr(media, "file_unique_id", "")
    if not value or not file_unique_id or len(value) != STREAM_HASH_LENGTH:
        return False
    expected = make_stream_hash(secret, chat_id, message_id, file_unique_id)
    return compare_digest(value.lower(), expected)


def is_active_mime(mime_type):
    return (mime_type or "").split(";", 1)[0].strip().lower() in ACTIVE_MIME_TYPES


def build_content_disposition(disposition, file_name):
    safe_name = file_name.replace("\r", "")
    safe_name = safe_name.replace("\n", "")
    safe_name = safe_name.replace("/", "_")
    safe_name = safe_name.replace("\\", "_")

    ascii_name = "".join(
        c for c in safe_name if 32 <= ord(c) < 127 and c not in {'"', "\\"}
    ) or "file"

    return (
        f'{disposition}; filename="{ascii_name}"; '
        f"filename*=UTF-8''{quote(safe_name, safe='')}"
    )
