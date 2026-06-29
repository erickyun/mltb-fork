from hashlib import sha256
from hmac import compare_digest
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


def make_stream_hash(message_id, file_unique_id):
    payload = f"{message_id}:{file_unique_id}"
    return sha256(payload.encode()).hexdigest()[:6]


def make_legacy_stream_token(chat_id, message_id, file_unique_id):
    payload = f"{chat_id}:{message_id}:{file_unique_id}"
    return sha256(payload.encode()).hexdigest()[:32]


def verify_stream_hash(value, chat_id, message_id, media):
    file_unique_id = getattr(media, "file_unique_id", "")
    if not value or not file_unique_id:
        return False

    short_hash = make_stream_hash(message_id, file_unique_id)
    legacy_token = make_legacy_stream_token(chat_id, message_id, file_unique_id)

    return compare_digest(value, short_hash) or compare_digest(value, legacy_token)


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
