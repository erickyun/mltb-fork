from asyncio import CancelledError, create_task, gather, sleep as asyncio_sleep
from base64 import b64encode
from hashlib import sha256
from html import escape
from logging import getLogger
from mimetypes import guess_type
from os import path as ospath, walk
from time import time
from urllib.parse import quote, urlparse
from uuid import uuid4

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from httpx import AsyncByteStream, AsyncClient, HTTPError, Limits, Timeout

from ...core.config_manager import Config
from ..ext_utils.bot_utils import sync_to_async
from ..telegram_helper.message_utils import send_message

LOGGER = getLogger(__name__)

_UPLOAD_CHUNK = 1024 * 1024
_HTTP_TIMEOUT = Timeout(connect=30.0, read=600.0, write=600.0, pool=30.0)

IMAGE_HOSTS = {"ibb"}
IMAGE_EXTS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
    ".tif",
    ".tiff",
}


class MultipartFileStream(AsyncByteStream):
    def __init__(
        self,
        uploader,
        file_path,
        file_size,
        fields,
        file_field,
        content_type="application/octet-stream",
    ):
        self._uploader = uploader
        self._file_path = file_path
        self._file_size = file_size
        self.boundary = f"----mltb-host-{uuid4().hex}"
        file_name = ospath.basename(file_path).replace('"', "")
        parts = []
        for key, value in fields.items():
            if value is None:
                continue
            parts.append(
                f"--{self.boundary}\r\n"
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                f"{value}\r\n"
            )
        parts.append(
            f"--{self.boundary}\r\n"
            f'Content-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        )
        self._prefix = "".join(parts).encode()
        self._suffix = f"\r\n--{self.boundary}--\r\n".encode()
        self.content_length = len(self._prefix) + self._file_size + len(self._suffix)

    async def __aiter__(self):
        yield self._prefix
        async with aiopen(self._file_path, "rb") as fh:
            while True:
                if self._uploader._listener.is_cancelled:
                    raise CancelledError()
                chunk = await fh.read(_UPLOAD_CHUNK)
                if not chunk:
                    break
                self._uploader._processed_bytes += len(chunk)
                yield chunk
        yield self._suffix


class RawFileStream(AsyncByteStream):
    def __init__(self, uploader, file_path):
        self._uploader = uploader
        self._file_path = file_path

    async def __aiter__(self):
        async with aiopen(self._file_path, "rb") as fh:
            while True:
                if self._uploader._listener.is_cancelled:
                    raise CancelledError()
                chunk = await fh.read(_UPLOAD_CHUNK)
                if not chunk:
                    break
                self._uploader._processed_bytes += len(chunk)
                yield chunk



class RangeFileStream(AsyncByteStream):
    """Streams one byte range of a file and reports progress to the uploader."""

    def __init__(self, uploader, file_path, start, length, on_progress=None):
        self._uploader = uploader
        self._file_path = file_path
        self._start = start
        self._length = length
        self._on_progress = on_progress

    async def __aiter__(self):
        sent = 0
        async with aiopen(self._file_path, "rb") as fh:
            await fh.seek(self._start)
            while sent < self._length:
                if self._uploader._listener.is_cancelled:
                    raise CancelledError()
                chunk = await fh.read(min(_UPLOAD_CHUNK, self._length - sent))
                if not chunk:
                    raise RuntimeError("file ended before the upload part was sent")
                sent += len(chunk)
                if self._on_progress:
                    self._on_progress(sent)
                yield chunk


class _FdFallback(RuntimeError):
    """FileDitch chunked endpoint unusable; fall back to upload.php."""


class HostUploader:
    def __init__(self, listener, path, host):
        self._listener = listener
        self._path = path
        self._host = host
        self._processed_bytes = 0
        self._start_time = time()
        self._upload_meta = {}
        self._is_folder = False
        self._root_name = ospath.basename(ospath.normpath(path))

    @property
    def host_name(self):
        return self._host

    @property
    def processed_bytes(self):
        return self._processed_bytes

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except Exception:
            return 0

    async def _post_multipart(
        self,
        client,
        url,
        file_path,
        fields,
        file_field,
        headers=None,
        json_response=True,
        content_type="application/octet-stream",
    ):
        file_size = await aiopath.getsize(file_path)
        stream = MultipartFileStream(
            self,
            file_path,
            file_size,
            fields,
            file_field,
            content_type=content_type,
        )
        req_headers = {
            "Content-Type": f"multipart/form-data; boundary={stream.boundary}",
            "Content-Length": str(stream.content_length),
        }
        if headers:
            req_headers.update(headers)

        response = await client.post(url, content=stream, headers=req_headers)
        if response.status_code >= 400:
            raise RuntimeError(
                f"{self._host} upload failed [{response.status_code}]: "
                f"{response.text[:500]}"
            )
        if not json_response:
            text = response.text.strip()
            if not text:
                raise RuntimeError(f"{self._host} returned empty response")
            return text
        try:
            return response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"{self._host} returned non-JSON response "
                f"[{response.status_code}]: {response.text[:500]}"
            ) from exc

    async def _upload_catbox(self, client, file_path):
        size = await aiopath.getsize(file_path)
        if size > 200 * 1024 * 1024:
            raise RuntimeError("Catbox limit is 200 MB per file")
        fields = {
            "reqtype": "fileupload",
            "userhash": (Config.CATBOX_USER_HASH or "").strip() or None,
        }
        link = await self._post_multipart(
            client,
            "https://catbox.moe/user/api.php",
            file_path,
            fields,
            "fileToUpload",
            json_response=False,
        )
        if not link.startswith("http"):
            raise RuntimeError(f"Catbox bad response: {link[:500]}")
        return link

    async def _create_catbox_album(self, client, links):
        if not links or len(links) > 500:
            return None
        filenames = []
        for link in links:
            name = ospath.basename(urlparse(link).path)
            if name:
                filenames.append(name)
        if not filenames:
            return None
        fields = {
            "reqtype": "createalbum",
            "userhash": (Config.CATBOX_USER_HASH or "").strip(),
            "title": self._root_name,
            "desc": "Uploaded by Mirror Leech Telegram Bot",
            "files": " ".join(filenames),
        }
        if not fields["userhash"]:
            del fields["userhash"]
        response = await client.post("https://catbox.moe/user/api.php", data=fields)
        text = response.text.strip()
        if response.status_code >= 400 or not text.startswith("http"):
            raise RuntimeError(
                f"Catbox album creation failed [{response.status_code}]: {text[:500]}"
            )
        return text

    async def _upload_litterbox(self, client, file_path):
        expiry = (Config.LITTERBOX_TIME or "1h").strip()
        if expiry not in {"1h", "12h", "24h", "72h"}:
            expiry = "1h"
        fields = {"reqtype": "fileupload", "time": expiry}
        link = await self._post_multipart(
            client,
            "https://litterbox.catbox.moe/resources/internals/api.php",
            file_path,
            fields,
            "fileToUpload",
            json_response=False,
        )
        if not link.startswith("http"):
            raise RuntimeError(f"Litterbox bad response: {link[:500]}")
        return link

    def _pixeldrain_auth(self):
        api_key = str(Config.PIXELDRAIN_API_KEY or "").strip().strip("'\"")
        # Tolerate keys pasted as "Basic xxx", ":key" or a full URL fragment.
        if api_key.lower().startswith("basic "):
            return api_key
        api_key = api_key.lstrip(":")
        if not api_key:
            raise RuntimeError(
                "PIXELDRAIN_API_KEY is not set. Get it from "
                "https://pixeldrain.com/user/api_keys and add it to config/env."
            )
        credentials = b64encode(f":{api_key}".encode()).decode()
        return f"Basic {credentials}"

    def _pixeldrain_parse(self, response, method):
        if response.status_code in {401, 403}:
            raise RuntimeError(
                f"PixelDrain auth failed [{response.status_code}] — check "
                f"PIXELDRAIN_API_KEY: {response.text[:300]}"
            )
        if response.status_code >= 400:
            raise RuntimeError(
                f"PixelDrain {method} failed [{response.status_code}]: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"PixelDrain returned non-JSON response "
                f"[{response.status_code}]: {response.text[:500]}"
            ) from exc
        if isinstance(payload, dict) and payload.get("success") is False:
            raise RuntimeError(
                f"PixelDrain error: {payload.get('value')} - {payload.get('message')}"
            )
        file_id = payload.get("id") if isinstance(payload, dict) else None
        if not file_id:
            raise RuntimeError(f"PixelDrain response missing id: {payload}")
        return file_id

    async def _pixeldrain_check(self, client, auth):
        """Small request to tell 'server cannot reach pixeldrain' apart from upload bugs."""
        try:
            response = await client.get(
                "https://pixeldrain.com/api/user",
                headers={"Authorization": auth},
                timeout=Timeout(30.0),
            )
        except HTTPError as exc:
            raise RuntimeError(
                f"Cannot reach pixeldrain.com from this server "
                f"({type(exc).__name__}: {exc or 'connection closed'})"
            ) from exc
        if response.status_code in {401, 403}:
            raise RuntimeError(
                f"PixelDrain auth failed [{response.status_code}] — check "
                f"PIXELDRAIN_API_KEY: {response.text[:300]}"
            )

    async def _upload_pixeldrain(self, client, file_path):
        auth = self._pixeldrain_auth()
        base_name = ospath.basename(file_path)
        file_size = await aiopath.getsize(file_path)
        errors = []
        file_id = None
        await self._pixeldrain_check(client, auth)
        for attempt in range(1, 4):
            method = "PUT" if attempt < 3 else "POST"
            self._processed_bytes = 0
            try:
                if method == "PUT":
                    response = await client.put(
                        f"https://pixeldrain.com/api/file/{quote(base_name, safe='')}",
                        content=RawFileStream(self, file_path),
                        headers={
                            "Authorization": auth,
                            "Content-Type": "application/octet-stream",
                            "Content-Length": str(file_size),
                        },
                    )
                else:
                    stream = MultipartFileStream(
                        self, file_path, file_size, {"name": base_name}, "file"
                    )
                    response = await client.post(
                        "https://pixeldrain.com/api/file",
                        content=stream,
                        headers={
                            "Authorization": auth,
                            "Content-Type": (
                                f"multipart/form-data; boundary={stream.boundary}"
                            ),
                            "Content-Length": str(stream.content_length),
                        },
                    )
                file_id = self._pixeldrain_parse(response, method)
                break
            except CancelledError:
                raise
            except (HTTPError, RuntimeError) as exc:
                if "auth failed" in str(exc):
                    raise
                msg = str(exc).strip() or "connection closed by remote"
                errors.append(f"{method}#{attempt} {type(exc).__name__}: {msg}")
                LOGGER.warning(f"PixelDrain upload attempt failed: {errors[-1]}")
                if self._listener.is_cancelled:
                    raise CancelledError()
                await asyncio_sleep(3 * attempt)

        if not file_id:
            raise RuntimeError(" | ".join(errors))
        link = f"https://pixeldrain.com/u/{file_id}"
        self._upload_meta[link] = {"id": file_id}
        return link

    async def _create_pixeldrain_list(self, client, links):
        file_ids = [
            self._upload_meta.get(link, {}).get("id")
            for link in links
            if self._upload_meta.get(link, {}).get("id")
        ]
        if not file_ids:
            return None
        response = await client.post(
            "https://pixeldrain.com/api/list",
            headers={"Authorization": self._pixeldrain_auth()},
            json={
                "title": self._root_name,
                "anonymous": False,
                "files": [{"id": file_id} for file_id in file_ids],
            },
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"PixelDrain list creation failed [{response.status_code}]: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"PixelDrain returned non-JSON list response: {response.text[:500]}"
            ) from exc
        list_id = payload.get("id")
        if not list_id:
            raise RuntimeError(f"PixelDrain list response missing id: {payload}")
        return f"https://pixeldrain.com/l/{list_id}"

    # ------------------------------------------------------------------ 0807.st
    _ST0807_BASE = "https://0807.st"
    _ST0807_CHUNK_THRESHOLD = 10 * 1024 * 1024
    _ST0807_MAX_CHUNK = 16 * 1024 * 1024

    def _st0807_headers(self, extra=None):
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
        }
        token = str(Config.UPLOAD_0807_TOKEN or "").strip().strip("'\"")
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if extra:
            headers.update(extra)
        return headers

    @staticmethod
    def _st0807_has_token():
        return bool(str(Config.UPLOAD_0807_TOKEN or "").strip())

    @staticmethod
    def _st0807_expiry():
        expiry = str(Config.UPLOAD_0807_EXPIRY or "0").strip().lower().rstrip("h")
        return expiry if expiry in {"0", "1", "24", "168", "720"} else "0"

    @staticmethod
    def _st0807_json(response, what):
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code == 401:
            raise RuntimeError(
                f"0807 auth failed [401] — check UPLOAD_0807_TOKEN: "
                f"{payload.get('error') or response.text[:300]}"
            )
        if response.status_code >= 400:
            detail = payload.get("error") if isinstance(payload, dict) else None
            raise RuntimeError(
                f"0807 {what} failed [{response.status_code}]: "
                f"{detail or response.text[:500]}"
            )
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"0807 {what} returned non-JSON response: {response.text[:500]}"
            )
        return payload

    @staticmethod
    def _st0807_solve(challenge_id, bits):
        bits = int(bits)
        full, rem = divmod(bits, 8)
        mask = (0xFF << (8 - rem)) & 0xFF if rem else 0
        nonce = 0
        while True:
            digest = sha256(f"{challenge_id}.{nonce}".encode()).digest()
            if digest[:full] == b"\x00" * full and (not rem or not digest[full] & mask):
                return str(nonce)
            nonce += 1

    async def _st0807_pow(self, client):
        if self._st0807_has_token():
            return {}
        response = await client.get(
            f"{self._ST0807_BASE}/pow",
            headers=self._st0807_headers({"Cache-Control": "no-store"}),
        )
        challenge = self._st0807_json(response, "proof-of-work")
        if not challenge.get("id") or challenge.get("bits") is None:
            raise RuntimeError(f"0807 bad proof-of-work challenge: {challenge}")
        nonce = await sync_to_async(
            self._st0807_solve, challenge["id"], challenge["bits"]
        )
        return {
            "pow_id": challenge["id"],
            "pow_ts": challenge.get("ts"),
            "pow_bits": challenge["bits"],
            "pow_sig": challenge.get("sig"),
            "pow_nonce": nonce,
        }

    async def _st0807_retry_after(self, response, attempt):
        try:
            wait = float(response.headers.get("Retry-After") or 0)
        except ValueError:
            wait = 0
        wait = min(max(wait, 5 * attempt), 300)
        LOGGER.warning(f"0807 rate limited, waiting {wait:.0f}s")
        await asyncio_sleep(wait)

    async def _st0807_simple(self, client, file_path, expiry):
        for attempt in range(1, 4):
            fields = {"expiry": expiry}
            fields.update(await self._st0807_pow(client))
            self._processed_bytes = 0
            file_size = await aiopath.getsize(file_path)
            stream = MultipartFileStream(self, file_path, file_size, fields, "file")
            response = await client.post(
                f"{self._ST0807_BASE}/upload",
                content=stream,
                headers=self._st0807_headers(
                    {
                        "Content-Type": f"multipart/form-data; boundary={stream.boundary}",
                        "Content-Length": str(stream.content_length),
                    }
                ),
            )
            if response.status_code == 429 and attempt < 3:
                await self._st0807_retry_after(response, attempt)
                continue
            return self._st0807_json(response, "upload")
        raise RuntimeError("0807 upload failed: rate limited")

    async def _st0807_chunked(self, client, file_path, file_size, expiry):
        name = ospath.basename(file_path)
        mime = guess_type(file_path)[0] or "application/octet-stream"

        init = None
        for attempt in range(1, 4):
            body = {"name": name, "size": file_size, "mime": mime, "maxdl": "0"}
            body.update(await self._st0807_pow(client))
            response = await client.post(
                f"{self._ST0807_BASE}/upload/init",
                json=body,
                headers=self._st0807_headers(),
            )
            if response.status_code == 429 and attempt < 3:
                await self._st0807_retry_after(response, attempt)
                continue
            init = self._st0807_json(response, "upload init")
            break
        upload_id = (init or {}).get("uploadId")
        if not upload_id:
            raise RuntimeError(f"0807 init response missing uploadId: {init}")
        chunk_size = min(
            int(init.get("chunkSize") or self._ST0807_MAX_CHUNK),
            self._ST0807_MAX_CHUNK,
        )

        self._processed_bytes = 0
        async with aiopen(file_path, "rb") as fh:
            offset = 0
            while offset < file_size:
                if self._listener.is_cancelled:
                    raise CancelledError()
                await fh.seek(offset)
                chunk = await fh.read(chunk_size)
                for attempt in range(1, 6):
                    try:
                        response = await client.post(
                            f"{self._ST0807_BASE}/upload/chunk",
                            params={"id": upload_id, "offset": offset},
                            content=chunk,
                            headers=self._st0807_headers(
                                {"Content-Type": "application/octet-stream"}
                            ),
                        )
                        if response.status_code < 500 and response.status_code != 429:
                            self._st0807_json(response, f"chunk @{offset}")
                            break
                        reason = f"HTTP {response.status_code}"
                    except HTTPError as exc:
                        reason = f"{type(exc).__name__}: {exc}"
                    if attempt == 5:
                        raise RuntimeError(f"0807 chunk @{offset} failed: {reason}")
                    LOGGER.warning(f"0807 chunk @{offset} retry {attempt}: {reason}")
                    await asyncio_sleep(2 * attempt)
                offset += len(chunk)
                self._processed_bytes = offset

        last_error = None
        for attempt in range(1, 11):
            try:
                response = await client.post(
                    f"{self._ST0807_BASE}/upload/finish",
                    json={
                        "id": upload_id,
                        "expiry": expiry,
                        "maxdl": "0",
                        "password": "",
                    },
                    headers=self._st0807_headers(),
                )
                if response.status_code < 500:
                    return self._st0807_json(response, "upload finish")
                last_error = f"HTTP {response.status_code}"
            except HTTPError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
            await asyncio_sleep(min(attempt, 5))
        raise RuntimeError(f"0807 finish failed: {last_error}")

    async def _upload_0807(self, client, file_path):
        file_size = await aiopath.getsize(file_path)
        if file_size <= 0:
            raise RuntimeError("0807 does not accept empty files")
        limit = 50 if self._st0807_has_token() else 10
        if file_size > limit * 1024**3:
            raise RuntimeError(f"0807 limit is {limit} GB per file")
        expiry = self._st0807_expiry()

        if file_size <= self._ST0807_CHUNK_THRESHOLD:
            payload = await self._st0807_simple(client, file_path, expiry)
        else:
            payload = await self._st0807_chunked(client, file_path, file_size, expiry)

        link = payload.get("url")
        if not link:
            raise RuntimeError(f"0807 response missing url: {payload}")
        if payload.get("deletion_url"):
            LOGGER.info(f"0807 deletion url for {link}: {payload['deletion_url']}")
        self._upload_meta[link] = {
            "id": link.rstrip("/").split("/")[-1],
            "deletion_url": payload.get("deletion_url"),
        }
        return link

    async def _create_0807_album(self, client, links):
        ids = [
            self._upload_meta.get(link, {}).get("id")
            for link in links
            if self._upload_meta.get(link, {}).get("id")
        ]
        if len(ids) < 2:
            return None
        response = await client.post(
            f"{self._ST0807_BASE}/album",
            json={"ids": ids, "expiry": self._st0807_expiry(), "name": self._root_name},
            headers=self._st0807_headers(),
        )
        payload = self._st0807_json(response, "album creation")
        return payload.get("url")

    # --------------------------------------------------------------- DropDrive
    _DD_BASE = "https://dropdrive.rip"
    _DD_DEFAULT_PART = 100_000_000

    @staticmethod
    def _dd_token():
        token = str(Config.DROPDRIVE_TOKEN or "").strip().strip("'\"")
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        return token

    def _dd_headers(self, json_body=True):
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
        }
        if json_body:
            headers["Content-Type"] = "application/json"
        if token := self._dd_token():
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _dd_json(response, what):
        try:
            payload = response.json()
        except ValueError:
            payload = None
        error = payload.get("error") if isinstance(payload, dict) else None
        if response.status_code == 401:
            raise RuntimeError(
                f"DropDrive auth failed [401] — check DROPDRIVE_TOKEN: "
                f"{error or response.text[:300]}"
            )
        if response.status_code >= 400 or error or (
            isinstance(payload, dict) and payload.get("success") is False
        ):
            raise RuntimeError(
                f"DropDrive {what} failed [{response.status_code}]: "
                f"{error or response.text[:500]}"
            )
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"DropDrive {what} returned non-JSON response: {response.text[:500]}"
            )
        return payload

    async def _dd_post(self, client, path, body, what, retries=1):
        last = None
        for attempt in range(retries):
            try:
                response = await client.post(
                    f"{self._DD_BASE}{path}", json=body, headers=self._dd_headers()
                )
                if response.status_code in {408, 429} or response.status_code >= 500:
                    last = f"HTTP {response.status_code}: {response.text[:200]}"
                else:
                    return self._dd_json(response, what)
            except HTTPError as exc:
                last = f"{type(exc).__name__}: {exc or 'connection error'}"
            if attempt + 1 < retries:
                await asyncio_sleep(min(2 ** attempt, 15))
        raise RuntimeError(f"DropDrive {what} failed: {last}")

    @staticmethod
    def _dd_solve(challenge, difficulty):
        difficulty = int(difficulty)
        full, rem = divmod(difficulty, 8)
        mask = (0xFF << (8 - rem)) & 0xFF if rem else 0
        nonce = 0
        while True:
            nonce_hex = format(nonce, "x")
            digest = sha256(f"{challenge}{nonce_hex}".encode()).digest()
            if digest[:full] == b"\x00" * full and (not rem or not digest[full] & mask):
                return nonce_hex
            nonce += 1

    async def _dd_pow(self, client):
        response = await client.get(
            f"{self._DD_BASE}/api/upload/pow-challenge",
            headers=self._dd_headers(json_body=False),
        )
        payload = self._dd_json(response, "proof-of-work")
        if payload.get("skipPow"):
            return {}
        challenge, difficulty = payload.get("challenge"), payload.get("difficulty")
        if not challenge or difficulty is None:
            raise RuntimeError(f"DropDrive bad proof-of-work challenge: {payload}")
        nonce = await sync_to_async(self._dd_solve, challenge, difficulty)
        return {"powChallenge": challenge, "powNonce": nonce}

    async def _dd_part_size(self, client):
        if getattr(self, "_dd_cached_part", None):
            return self._dd_cached_part
        part = self._DD_DEFAULT_PART
        try:
            response = await client.get(
                f"{self._DD_BASE}/api/config",
                headers=self._dd_headers(json_body=False),
            )
            config = self._dd_json(response, "config")
            for key in ("chunkSize", "partSize", "storageChunkSize", "uploadChunkSize"):
                if str(config.get(key) or "").isdigit() and int(config[key]) > 0:
                    part = int(config[key])
                    break
            if config.get("maxFileSizeGB"):
                self._dd_max_bytes = float(config["maxFileSizeGB"]) * 1024**3
        except (HTTPError, RuntimeError) as exc:
            LOGGER.warning(f"DropDrive config lookup failed, using defaults: {exc}")
        self._dd_cached_part = part
        return part

    async def _dd_init(self, client, name, file_size, part_size, folder_id):
        body = {
            "fileName": name,
            "fileSize": file_size,
            "totalChunks": max(1, -(-file_size // part_size)),
            "mimeType": guess_type(name)[0] or "application/octet-stream",
        }
        if folder_id:
            body["folderId"] = folder_id
        body.update(await self._dd_pow(client))
        return await self._dd_post(client, "/api/upload/init", body, "upload init")

    async def _dd_storage_upload(self, client, file_path, file_size, init):
        server = str(init["storageServerUrl"]).rstrip("/")
        session_id = init["sessionId"]
        part_size = int(init.get("chunkSize") or self._DD_DEFAULT_PART)
        parts = max(1, -(-file_size // part_size))
        base = 0
        for index in range(parts):
            start = index * part_size
            length = min(part_size, file_size - start)
            last = index == parts - 1

            def progress(sent, _start=start):
                self._processed_bytes = base + _start + sent

            response = await client.post(
                f"{server}/files/{session_id}/{index}",
                params={"last": "1"} if last else None,
                content=RangeFileStream(self, file_path, start, length, progress),
                headers={
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(length),
                    "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
                },
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"DropDrive part {index + 1}/{parts} failed "
                    f"[{response.status_code}]: {response.text[:300]}"
                )

    async def _dd_s3_upload(self, client, file_path, file_size, init):
        bearer = {"sessionId": init["sessionId"], "uploadToken": init["uploadToken"]}
        single = init.get("uploadMode") == "single" and init.get("uploadUrl")
        part_size = file_size if single else int(init.get("partSize") or 0)
        part_count = 1 if single else int(init.get("partCount") or 0)
        if part_size < 1 or part_count < 1 or (
            not single and part_count != -(-file_size // part_size)
        ):
            raise RuntimeError(f"DropDrive returned invalid S3 settings: {init}")

        done_bytes = 0
        active = {"on": True}

        async def heartbeat():
            while active["on"]:
                await asyncio_sleep(30)
                if not active["on"]:
                    break
                try:
                    await client.post(
                        f"{self._DD_BASE}/api/upload/s3-heartbeat",
                        json=bearer,
                        headers=self._dd_headers(),
                        timeout=Timeout(15.0),
                    )
                except HTTPError:
                    pass

        hb = create_task(heartbeat())
        parts = []
        try:
            for number in range(1, part_count + 1):
                start = (number - 1) * part_size
                length = min(part_size, file_size - start)
                for attempt in range(4):
                    if self._listener.is_cancelled:
                        raise CancelledError()

                    def progress(sent, _base=done_bytes):
                        self._processed_bytes = _base + sent

                    try:
                        url = single or (
                            await self._dd_post(
                                client,
                                "/api/upload/s3-part",
                                {**bearer, "partNumber": number},
                                f"S3 part {number} signing",
                                retries=3,
                            )
                        ).get("url")
                        if not url:
                            raise RuntimeError("DropDrive did not return an S3 part URL")
                        response = await client.put(
                            url,
                            content=RangeFileStream(self, file_path, start, length, progress),
                            headers={"Content-Length": str(length)},
                        )
                        if response.status_code in {400, 401, 403, 404, 409, 413}:
                            raise RuntimeError(
                                f"S3 rejected part {number} [{response.status_code}]: "
                                f"{response.text[:300]}"
                            )
                        if response.status_code >= 300:
                            raise HTTPError(f"S3 part {number} HTTP {response.status_code}")
                        etag = response.headers.get("ETag")
                        if not etag:
                            raise RuntimeError(f"S3 part {number} returned no ETag")
                        parts.append({"PartNumber": number, "ETag": etag})
                        break
                    except HTTPError as exc:
                        if attempt == 3:
                            raise RuntimeError(
                                f"DropDrive S3 part {number} failed: "
                                f"{type(exc).__name__}: {exc}"
                            ) from exc
                        LOGGER.warning(f"DropDrive S3 part {number} retry {attempt + 1}: {exc}")
                        await asyncio_sleep(min(8, 2 ** attempt))
                done_bytes += length
                self._processed_bytes = done_bytes
        finally:
            active["on"] = False
            hb.cancel()

        await self._dd_post(
            client, "/api/upload/s3-complete", {**bearer, "parts": parts},
            "S3 completion", retries=3,
        )

    async def _upload_dropdrive(self, client, file_path):
        file_size = await aiopath.getsize(file_path)
        if file_size <= 0:
            raise RuntimeError("DropDrive does not accept empty files")
        name = ospath.basename(file_path)
        if not ospath.splitext(name)[1]:
            name = f"{name}.bin"  # DropDrive requires a file extension
        name = name[-255:]
        folder_id = str(Config.DROPDRIVE_FOLDER_ID or "").strip() or None
        if folder_id and not self._dd_token():
            raise RuntimeError("DROPDRIVE_FOLDER_ID needs DROPDRIVE_TOKEN")

        part_size = await self._dd_part_size(client)
        max_bytes = getattr(self, "_dd_max_bytes", None)
        if max_bytes and file_size > max_bytes:
            raise RuntimeError(f"DropDrive limit is {max_bytes / 1024**3:.0f} GB per file")

        errors = []
        for attempt in range(1, 4):
            if self._listener.is_cancelled:
                raise CancelledError()
            self._processed_bytes = 0
            init = None
            try:
                init = await self._dd_init(client, name, file_size, part_size, folder_id)
                if not init.get("sessionId"):
                    raise RuntimeError(f"DropDrive init response missing sessionId: {init}")
                if init.get("uploadBackend") == "s3":
                    await self._dd_s3_upload(client, file_path, file_size, init)
                elif init.get("storageServerUrl"):
                    chunk = int(init.get("chunkSize") or part_size)
                    if chunk != part_size and -(-file_size // chunk) != -(-file_size // part_size):
                        # Server uses another part size; restart with the right count.
                        self._dd_cached_part = part_size = chunk
                        raise RuntimeError(f"part size changed to {chunk}, restarting")
                    await self._dd_storage_upload(client, file_path, file_size, init)
                else:
                    raise RuntimeError(f"DropDrive init returned no upload target: {init}")

                body = {"sessionId": init["sessionId"]}
                if init.get("uploadBackend") == "s3":
                    body["uploadToken"] = init["uploadToken"]
                if folder_id:
                    body["folderId"] = folder_id
                done = await self._dd_post(
                    client, "/api/upload/complete", body, "upload complete", retries=5
                )
                break
            except CancelledError:
                if init and init.get("uploadBackend") == "s3":
                    await self._dd_s3_abort(client, init)
                raise
            except (HTTPError, RuntimeError) as exc:
                if init and init.get("uploadBackend") == "s3":
                    await self._dd_s3_abort(client, init)
                msg = f"{type(exc).__name__}: {str(exc).strip() or 'connection error'}"
                if "auth failed" in msg or "needs DROPDRIVE_TOKEN" in msg:
                    raise
                errors.append(f"try{attempt} {msg}")
                LOGGER.warning(f"DropDrive upload attempt {attempt} failed: {msg}")
                if attempt < 3:
                    await asyncio_sleep(5 * attempt)
        else:
            raise RuntimeError(" | ".join(errors))

        if short := done.get("shortCode"):
            link = f"{self._DD_BASE}/d/{short}"
        elif url := done.get("downloadUrl"):
            link = url if url.startswith("http") else f"{self._DD_BASE}{url}"
        elif done.get("fileId"):
            link = f"{self._DD_BASE}/d/{done['fileId']}"
        else:
            raise RuntimeError(f"DropDrive complete response missing link: {done}")
        self._processed_bytes = file_size
        self._upload_meta[link] = {"id": done.get("fileId")}
        return link

    async def _dd_s3_abort(self, client, init):
        try:
            await client.post(
                f"{self._DD_BASE}/api/upload/s3-abort",
                json={"sessionId": init["sessionId"], "uploadToken": init.get("uploadToken")},
                headers=self._dd_headers(),
                timeout=Timeout(10.0),
            )
        except Exception:
            pass

    # --------------------------------------------------------------- FileDitch
    _FD_UPLOAD_URL = "https://new.fileditch.com/upload.php"
    _FD_CHUNK_URL = "https://new.fileditch.com/chunked.php"
    _FD_FULL_MSG = (
        "FileDitch servers are out of space (HTTP 507) — this is on FileDitch's "
        "side, try again later or use another host"
    )
    _FD_PARALLEL = 3
    _FD_TRIES = 8

    @staticmethod
    def _fd_token():
        token = str(Config.FILEDITCH_TOKEN or "").strip().strip("'\"")
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        return token

    def _fd_headers(self, extra=None):
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
        }
        if token := self._fd_token():
            headers["Authorization"] = f"Bearer {token}"
        if extra:
            headers.update(extra)
        return headers

    @staticmethod
    def _fd_payload(response):
        try:
            payload = response.json()
        except ValueError:
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _fd_retryable(status):
        return status == 0 or status == 429 or (status >= 500 and status != 507)

    @staticmethod
    def _fd_backoff(attempt, status):
        return min(30, (2 if status == 429 else 1) * 2 ** (attempt - 1))

    async def _fd_api(self, client, params, what, tries=5):
        """POST to chunked.php; returns (status, payload) after retrying 429/5xx."""
        status, payload = 0, {}
        for attempt in range(1, tries + 1):
            try:
                response = await client.post(
                    self._FD_CHUNK_URL, params=params, headers=self._fd_headers()
                )
                status, payload = response.status_code, self._fd_payload(response)
                if not payload and status >= 400:
                    payload = {"error": response.text[:300]}
            except HTTPError as exc:
                status, payload = 0, {"error": f"{type(exc).__name__}: {exc or 'network error'}"}
            if status == 200 and payload.get("success"):
                return status, payload
            if not self._fd_retryable(status) or attempt == tries:
                return status, payload
            LOGGER.warning(f"FileDitch {what} retry {attempt}: HTTP {status} {payload.get('error')}")
            await asyncio_sleep(self._fd_backoff(attempt, status))
        return status, payload

    async def _fd_chunked(self, client, file_path, file_size, name):
        status, init = await self._fd_api(
            client, {"action": "init", "filename": name, "size": file_size}, "init"
        )
        if status == 507:
            raise RuntimeError(self._FD_FULL_MSG)
        if status != 200 or not init.get("success"):
            raise _FdFallback(f"chunked init HTTP {status}: {init.get('error')}")
        up_id = init.get("id")
        chunk_size = int(init.get("chunk_size") or 0)
        total = int(init.get("total_chunks") or 0)
        if not up_id or chunk_size <= 0 or total != -(-file_size // chunk_size):
            raise _FdFallback(f"chunked init returned unexpected data: {init}")

        loaded = [0] * total
        state = {"next": 0, "failed": None}

        def refresh():
            self._processed_bytes = sum(loaded)

        async def put_chunk(i):
            start = i * chunk_size
            length = min(chunk_size, file_size - start)
            for attempt in range(1, self._FD_TRIES + 1):
                if self._listener.is_cancelled:
                    raise CancelledError()
                if state["failed"]:
                    raise state["failed"]

                def progress(sent, _i=i):
                    loaded[_i] = sent
                    refresh()

                try:
                    response = await client.put(
                        self._FD_CHUNK_URL,
                        params={"action": "chunk", "id": up_id, "i": i},
                        content=RangeFileStream(self, file_path, start, length, progress),
                        headers=self._fd_headers(
                            {
                                "Content-Type": "application/octet-stream",
                                "Content-Length": str(length),
                            }
                        ),
                    )
                    code, payload = response.status_code, self._fd_payload(response)
                    error = payload.get("error") or response.text[:200]
                except HTTPError as exc:
                    code, error = 0, f"{type(exc).__name__}: {exc or 'network error'}"
                    payload = {}
                if code == 200 and payload.get("success"):
                    loaded[i] = length
                    refresh()
                    return
                loaded[i] = 0
                refresh()
                if code == 507:
                    state["failed"] = RuntimeError(self._FD_FULL_MSG)
                    raise state["failed"]
                if not self._fd_retryable(code) or attempt == self._FD_TRIES:
                    state["failed"] = RuntimeError(
                        f"FileDitch chunk {i + 1}/{total} failed [HTTP {code}]: {error}"
                    )
                    raise state["failed"]
                LOGGER.warning(f"FileDitch chunk {i + 1}/{total} retry {attempt}: HTTP {code} {error}")
                await asyncio_sleep(self._fd_backoff(attempt, code))

        async def worker():
            while True:
                i = state["next"]
                state["next"] += 1
                if i >= total or state["failed"]:
                    return
                await put_chunk(i)

        try:
            await gather(*(worker() for _ in range(min(self._FD_PARALLEL, total))))
            payload = {}
            for _ in range(30):
                status, payload = await self._fd_api(
                    client, {"action": "finish", "id": up_id}, "finish", tries=8
                )
                if status == 409 and payload.get("error") == "incomplete" and payload.get("missing"):
                    for i in payload["missing"]:
                        await put_chunk(int(i))
                    continue
                break
            if status != 200 or not payload.get("success") or not payload.get("url"):
                raise RuntimeError(
                    f"FileDitch finish failed [HTTP {status}]: {payload.get('error') or payload}"
                )
        except BaseException:
            try:
                await client.post(
                    self._FD_CHUNK_URL,
                    params={"action": "abort", "id": up_id},
                    headers=self._fd_headers(),
                    timeout=Timeout(10.0),
                )
            except Exception:
                pass
            raise
        return payload

    async def _upload_fileditch(self, client, file_path):
        file_size = await aiopath.getsize(file_path)
        if file_size <= 0:
            raise RuntimeError("FileDitch does not accept empty files")
        name = ospath.basename(file_path)
        try:
            payload = await self._fd_chunked(client, file_path, file_size, name)
            link = payload["url"]
            self._upload_meta[link] = {"filename": payload.get("filename")}
            return link
        except _FdFallback as exc:
            LOGGER.warning(f"FileDitch chunked upload unavailable, using upload.php: {exc}")
        return await self._upload_fileditch_raw(client, file_path)

    async def _upload_fileditch_raw(self, client, file_path):
        file_size = await aiopath.getsize(file_path)
        if file_size <= 0:
            raise RuntimeError("FileDitch does not accept empty files")
        name = ospath.basename(file_path)
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/octet-stream",
            "Content-Length": str(file_size),
            "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
        }
        token = str(Config.FILEDITCH_TOKEN or "").strip().strip("'\"")
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        errors = []
        for attempt in range(1, 4):
            if self._listener.is_cancelled:
                raise CancelledError()
            self._processed_bytes = 0
            wait = 5 * attempt
            try:
                response = await client.put(
                    self._FD_UPLOAD_URL,
                    params={"filename": name},
                    content=RawFileStream(self, file_path),
                    headers=headers,
                )
            except HTTPError as exc:
                errors.append(
                    f"try{attempt} {type(exc).__name__}: {str(exc).strip() or 'connection error'}"
                )
            else:
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                error = payload.get("error") or response.text[:300]
                status = response.status_code
                if status < 300 and payload.get("url"):
                    link = payload["url"]
                    self._upload_meta[link] = {"filename": payload.get("filename")}
                    if payload.get("account"):
                        LOGGER.info(f"FileDitch account listing: {payload['account']}")
                    return link
                if status == 403:
                    raise RuntimeError(f"FileDitch blocked this file type: {error}")
                if status == 413:
                    raise RuntimeError(f"FileDitch file too large: {error}")
                if status == 401:
                    raise RuntimeError(
                        f"FileDitch auth failed [401] — check FILEDITCH_TOKEN: {error}"
                    )
                if status < 300:
                    raise RuntimeError(f"FileDitch response missing url: {payload or error}")
                if status in {400, 405, 411}:
                    raise RuntimeError(f"FileDitch upload failed [{status}]: {error}")
                errors.append(f"try{attempt} HTTP {status}: {error}")
                if status == 429:
                    try:
                        wait = max(wait, float(response.headers.get("Retry-After") or 0))
                    except ValueError:
                        pass
                elif status == 507:
                    raise RuntimeError(self._FD_FULL_MSG)
            LOGGER.warning(f"FileDitch upload attempt {attempt} failed: {errors[-1]}")
            if attempt < 3:
                await asyncio_sleep(min(wait, 300))
        raise RuntimeError(" | ".join(errors))

    def _viking_remote_path(self, file_path):
        if not self._is_folder:
            return None
        rel_dir = ospath.relpath(ospath.dirname(file_path), self._path)
        remote_path = self._root_name
        if rel_dir != ".":
            rel_dir = rel_dir.replace(ospath.sep, "/")
            remote_path = f"{remote_path}/{rel_dir}"
        return remote_path

    async def _upload_vikingfile(self, client, file_path):
        server_resp = await client.get("https://vikingfile.com/api/get-server")
        if server_resp.status_code >= 400:
            raise RuntimeError(
                f"VikingFile server lookup failed: {server_resp.text[:500]}"
            )
        try:
            server = server_resp.json().get("server")
        except ValueError as exc:
            raise RuntimeError(
                f"VikingFile returned non-JSON server response: {server_resp.text[:500]}"
            ) from exc
        if not server:
            raise RuntimeError("VikingFile response missing server")
        fields = {
            "user": (Config.VIKINGFILE_USER_HASH or "").strip(),
            "path": self._viking_remote_path(file_path),
        }
        payload = await self._post_multipart(client, server, file_path, fields, "file")
        link = payload.get("url")
        if not link:
            raise RuntimeError(f"VikingFile response missing url: {payload}")
        return link

    async def _upload_krakenfiles(self, client, file_path):
        api_key = (Config.KRAKENFILES_API_KEY or "").strip()
        file_size = await aiopath.getsize(file_path)
        if api_key:
            if file_size > 2_097_152_000:
                raise RuntimeError("KrakenFiles limit is about 2 GB with API key")
        elif file_size > 1_048_576_000:
            raise RuntimeError("KrakenFiles anonymous limit is about 1 GB")

        upload_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; MLTB/1.0)",
        }
        if api_key:
            upload_headers["X-AUTH-TOKEN"] = api_key

        server_response = await client.get(
            "https://krakenfiles.com/api/server/available",
            headers={
                "Accept": "application/json",
                "User-Agent": upload_headers["User-Agent"],
            },
        )
        if server_response.status_code >= 400:
            raise RuntimeError(
                f"KrakenFiles server lookup failed [{server_response.status_code}]: "
                f"{server_response.text[:500]}"
            )
        try:
            server_payload = server_response.json()
        except ValueError as exc:
            raise RuntimeError(
                "KrakenFiles returned non-JSON server response "
                f"[{server_response.status_code}]: {server_response.text[:500]}"
            ) from exc

        data = server_payload.get("data") or {}
        upload_url = data.get("url")
        server_access_token = data.get("serverAccessToken")
        if not upload_url or not server_access_token:
            raise RuntimeError(
                f"KrakenFiles server response missing url/token: {server_payload}"
            )

        payload = await self._post_multipart(
            client,
            upload_url,
            file_path,
            {"serverAccessToken": server_access_token},
            "file",
            headers=upload_headers,
            json_response=True,
            content_type="text/plain",
        )
        data = payload.get("data") or {}
        link = data.get("url") or payload.get("url")
        if not link:
            message = payload.get("message") or payload.get("error") or payload
            raise RuntimeError(f"KrakenFiles upload response has no url: {message}")
        self._upload_meta[link] = data
        return link

    async def _upload_imgur(self, client, file_path):
        client_id = (Config.IMGUR_CLIENT_ID or "").strip()
        if not client_id:
            raise RuntimeError("IMGUR_CLIENT_ID is required")
        payload = await self._post_multipart(
            client,
            "https://api.imgur.com/3/image",
            file_path,
            {},
            "image",
            headers={"Authorization": f"Client-ID {client_id}"},
            json_response=True,
        )
        data = payload.get("data") or {}
        link = data.get("link")
        if not link:
            raise RuntimeError(f"Imgur response missing link: {payload}")
        self._upload_meta[link] = {
            "id": data.get("id"),
            "deletehash": data.get("deletehash"),
        }
        return link

    async def _create_imgur_album(self, client, links):
        client_id = (Config.IMGUR_CLIENT_ID or "").strip()
        deletehashes = [
            self._upload_meta.get(link, {}).get("deletehash")
            for link in links
            if self._upload_meta.get(link, {}).get("deletehash")
        ]
        ids = [
            self._upload_meta.get(link, {}).get("id")
            for link in links
            if self._upload_meta.get(link, {}).get("id")
        ]
        data = {"title": self._root_name}
        if len(deletehashes) == len(links):
            data["deletehashes[]"] = deletehashes
        elif ids:
            data["ids[]"] = ids
        else:
            return None
        response = await client.post(
            "https://api.imgur.com/3/album",
            headers={"Authorization": f"Client-ID {client_id}"},
            data=data,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"Imgur album creation failed [{response.status_code}]: "
                f"{response.text[:500]}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Imgur returned non-JSON album response: {response.text[:500]}"
            ) from exc
        album_id = (payload.get("data") or {}).get("id")
        if not album_id:
            raise RuntimeError(f"Imgur album response missing id: {payload}")
        return f"https://imgur.com/a/{album_id}"

    async def _upload_imgchest(self, client, file_path, post_id=None):
        token = (Config.IMGCHEST_API_KEY or "").strip()
        if not token:
            raise RuntimeError("IMGCHEST_API_KEY is required")
        if post_id:
            url = f"https://api.imgchest.com/v1/post/{post_id}/add"
            fields = {}
        else:
            url = "https://api.imgchest.com/v1/post"
            fields = {"privacy": "hidden"}
            if self._is_folder:
                fields["title"] = self._root_name
        payload = await self._post_multipart(
            client,
            url,
            file_path,
            fields,
            "images[]",
            headers={"Authorization": f"Bearer {token}"},
            json_response=True,
        )
        data = payload.get("data") or {}
        returned_post_id = data.get("id") or post_id
        if not returned_post_id:
            raise RuntimeError(f"ImgChest response missing post id: {payload}")
        images = data.get("images") or []
        file_name = ospath.basename(file_path)
        image_link = None
        for image in reversed(images):
            if image.get("original_name") == file_name and image.get("link"):
                image_link = image["link"]
                break
        if not image_link and images:
            image_link = images[-1].get("link")
        if not image_link:
            raise RuntimeError(f"ImgChest response missing image link: {payload}")
        return (
            returned_post_id,
            f"https://imgchest.com/p/{returned_post_id}",
            image_link,
        )

    async def _upload_imgbb(self, client, file_path):
        api_key = (Config.IMGBB_API_KEY or "").strip()
        if not api_key:
            raise RuntimeError("IMGBB_API_KEY is required")
        size = await aiopath.getsize(file_path)
        if size > 32 * 1024 * 1024:
            raise RuntimeError("ImgBB limit is 32 MB per image")
        payload = await self._post_multipart(
            client,
            f"https://api.imgbb.com/1/upload?key={api_key}",
            file_path,
            {},
            "image",
            json_response=True,
        )
        data = payload.get("data") or {}
        link = data.get("url_viewer") or data.get("url") or data.get("display_url")
        if not link:
            raise RuntimeError(f"ImgBB response missing link: {payload}")
        return link

    async def _upload_one(self, client, file_path):
        if self._listener.is_cancelled:
            raise CancelledError()

        ext = ospath.splitext(file_path)[1].lower()
        if self._host in IMAGE_HOSTS and ext not in IMAGE_EXTS:
            raise RuntimeError(
                f"{self._host} only supports image uploads; skipped: "
                f"{ospath.basename(file_path)}"
            )

        if self._host == "cb":
            return await self._upload_catbox(client, file_path)
        if self._host == "lb":
            return await self._upload_litterbox(client, file_path)
        if self._host == "pd":
            return await self._upload_pixeldrain(client, file_path)
        if self._host == "vf":
            return await self._upload_vikingfile(client, file_path)
        if self._host == "imgur":
            return await self._upload_imgur(client, file_path)
        if self._host == "ibb":
            return await self._upload_imgbb(client, file_path)
        if self._host == "kf":
            return await self._upload_krakenfiles(client, file_path)
        if self._host == "0807":
            return await self._upload_0807(client, file_path)
        if self._host in {"dd", "dropdrive"}:
            return await self._upload_dropdrive(client, file_path)
        if self._host in {"fd", "fileditch"}:
            return await self._upload_fileditch(client, file_path)

        raise RuntimeError(f"Unknown upload host: {self._host}")

    async def _collect_files(self):
        files = []
        subfolders = 0
        if await aiopath.isfile(self._path):
            files.append(self._path)
            return files, subfolders

        self._is_folder = True
        walk_data = await sync_to_async(lambda: list(walk(self._path)))
        for root, dirs, names in walk_data:
            subfolders += len(dirs)
            for name in sorted(names):
                candidate = ospath.join(root, name)
                if await aiopath.isfile(candidate):
                    files.append(candidate)
        return files, subfolders

    async def _make_collection(self, client, links):
        if not self._is_folder or len(links) < 2:
            return None
        if self._host == "cb":
            return await self._create_catbox_album(client, links)
        if self._host == "pd":
            return await self._create_pixeldrain_list(client, links)
        if self._host == "imgur":
            return await self._create_imgur_album(client, links)
        if self._host == "0807":
            return await self._create_0807_album(client, links)
        return None

    async def _send_file_links(self, files_dict):
        if not files_dict:
            return
        header = f"<b>{escape(self._root_name)} — uploaded files</b>\n\n"
        chunk = header
        index = 1
        for link, name in files_dict.items():
            safe_link = escape(link, quote=True)
            safe_name = escape(name)
            line = f"{index}. <a href='{safe_link}'>{safe_name}</a>\n"
            if len((chunk + line).encode()) > 3900:
                await send_message(self._listener.message, chunk)
                chunk = header
            chunk += line
            index += 1
        if chunk != header:
            await send_message(self._listener.message, chunk)

    async def upload(self):
        files, subfolders = await self._collect_files()
        if not files:
            await self._listener.on_upload_error(
                f"{self._host}: no files were found to upload"
            )
            return

        total_files = len(files)
        corrupted = 0
        errors = []
        files_dict = {}
        uploaded_links = []
        collection_link = None

        try:
            async with AsyncClient(
                timeout=_HTTP_TIMEOUT,
                limits=Limits(max_connections=4, max_keepalive_connections=2),
                follow_redirects=True,
            ) as client:
                imgchest_post_id = None
                for file_path in files:
                    try:
                        LOGGER.info(f"Uploading to {self._host}: {file_path}")
                        if self._host == "ic":
                            ext = ospath.splitext(file_path)[1].lower()
                            if ext not in IMAGE_EXTS:
                                raise RuntimeError(
                                    "ic only supports image uploads; skipped: "
                                    f"{ospath.basename(file_path)}"
                                )
                            (
                                imgchest_post_id,
                                imgchest_post_link,
                                link,
                            ) = await self._upload_imgchest(
                                client, file_path, imgchest_post_id
                            )
                            if self._is_folder and uploaded_links:
                                collection_link = imgchest_post_link
                        else:
                            link = await self._upload_one(client, file_path)
                    except (HTTPError, RuntimeError, OSError) as exc:
                        err = str(exc).strip() or "no message"
                        err = f"{type(exc).__name__}: {err}"
                        LOGGER.error(
                            f"{self._host} Upload Error: {err} - File Path: {file_path}"
                        )
                        errors.append(f"{ospath.basename(file_path)}: {err}")
                        corrupted += 1
                        continue
                    except CancelledError:
                        return

                    if self._listener.is_cancelled:
                        return

                    uploaded_links.append(link)
                    files_dict[link] = (
                        ospath.relpath(file_path, self._path)
                        if self._is_folder
                        else ospath.basename(file_path)
                    )

                if uploaded_links and not collection_link:
                    try:
                        collection_link = await self._make_collection(
                            client, uploaded_links
                        )
                    except (HTTPError, RuntimeError) as exc:
                        LOGGER.error(f"{self._host} collection creation error: {exc}")
                        errors.append(f"collection: {exc}")

        except Exception as exc:
            LOGGER.exception(f"{self._host} session error")
            await self._listener.on_upload_error(f"{self._host}: {exc}")
            return

        successful = total_files - corrupted
        if successful <= 0:
            details = "\n".join(errors[-3:]) or "No response detail was returned."
            await self._listener.on_upload_error(
                f"{self._host}: all {total_files} file(s) failed to upload.\n{details}"
            )
            return

        if self._listener.is_cancelled:
            return

        if self._is_folder and successful > 1 and not collection_link:
            await self._send_file_links(files_dict)
        elif (
            self._is_folder
            and successful > 1
            and self._listener.files_links
            and files_dict
        ):
            await self._send_file_links(files_dict)

        cloud_link = collection_link or uploaded_links[0]
        mime_type = "Folder" if self._is_folder else (guess_type(self._path)[0] or "File")

        LOGGER.info(
            f"Uploaded To {self._host}: {self._listener.name} - "
            f"{successful}/{total_files} files"
        )
        await self._listener.on_upload_complete(
            cloud_link,
            successful,
            subfolders,
            mime_type,
        )

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
