from types import SimpleNamespace

from web.stream_utils import (
    STREAM_HASH_LENGTH,
    is_active_mime,
    make_stream_hash,
    verify_stream_hash,
)


def media(uid="AgADxyz"):
    return SimpleNamespace(file_unique_id=uid)


def test_hash_is_keyed_and_long():
    h = make_stream_hash("secret", -100123, 42, "AgADxyz")
    assert len(h) == STREAM_HASH_LENGTH
    assert h != make_stream_hash("other", -100123, 42, "AgADxyz")


def test_hash_binds_chat_and_message():
    h = make_stream_hash("secret", -100123, 42, "AgADxyz")
    assert verify_stream_hash("secret", h, -100123, 42, media())
    assert not verify_stream_hash("secret", h, -100999, 42, media())
    assert not verify_stream_hash("secret", h, -100123, 43, media())
    assert not verify_stream_hash("wrong", h, -100123, 42, media())


def test_old_short_hashes_rejected():
    assert not verify_stream_hash("secret", "abcdef", -100123, 42, media())
    assert not verify_stream_hash("secret", "", -100123, 42, media())


def test_active_mime_types():
    assert is_active_mime("text/html; charset=utf-8")
    assert is_active_mime("image/svg+xml")
    assert not is_active_mime("video/mp4")
    assert not is_active_mime("")
