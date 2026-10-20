"""Regression tests for folder-aware Buzzheavier uploads (network mocked)."""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def buzzheavier_module(monkeypatch):
    bot_pkg = ModuleType("bot")
    bot_pkg.__path__ = []
    helper_pkg = ModuleType("bot.helper")
    helper_pkg.__path__ = []
    mlu_pkg = ModuleType("bot.helper.mirror_leech_utils")
    mlu_pkg.__path__ = [
        str(Path(__file__).resolve().parent.parent / "bot" / "helper" / "mirror_leech_utils")
    ]
    ext_pkg = ModuleType("bot.helper.ext_utils")
    ext_pkg.__path__ = []
    utils = ModuleType("bot.helper.ext_utils.bot_utils")
    async def sync_to_async(func, *args, **kwargs):
        return func(*args, **kwargs)
    utils.sync_to_async = sync_to_async
    files = ModuleType("bot.helper.ext_utils.files_utils")
    files.get_mime_type = lambda path: "application/octet-stream"
    core_pkg = ModuleType("bot.core")
    core_pkg.__path__ = []
    manager = ModuleType("bot.core.config_manager")
    class Config:
        BUZZHEAVIER_ACCOUNT_ID = "test-account"
        BUZZHEAVIER_FOLDER_ID = ""
    manager.Config = Config
    for name, value in {
        "bot": bot_pkg,
        "bot.helper": helper_pkg,
        "bot.helper.mirror_leech_utils": mlu_pkg,
        "bot.helper.ext_utils": ext_pkg,
        "bot.helper.ext_utils.bot_utils": utils,
        "bot.helper.ext_utils.files_utils": files,
        "bot.core": core_pkg,
        "bot.core.config_manager": manager,
    }.items():
        monkeypatch.setitem(sys.modules, name, value)
    sys.modules.pop("bot.helper.mirror_leech_utils.buzzheavier_uploader", None)
    return importlib.import_module("bot.helper.mirror_leech_utils.buzzheavier_uploader")


def listener(up_dest="bh", **settings):
    return SimpleNamespace(
        is_cancelled=False,
        up_dest=up_dest,
        user_dict=settings,
        name="test",
        on_upload_complete=AsyncMock(),
        on_upload_error=AsyncMock(),
    )


class FakeClient:
    async def aclose(self):
        pass


@pytest.mark.asyncio
async def test_nested_folders_keep_correct_parents(buzzheavier_module, tmp_path, monkeypatch):
    root = tmp_path / "collection"
    (root / "A" / "child").mkdir(parents=True)
    (root / "B").mkdir()
    for path in [root / "top.txt", root / "A" / "a.txt", root / "A" / "child" / "nested.txt", root / "B" / "b.txt"]:
        path.write_text(path.name)
    obj = listener()
    uploader = buzzheavier_module.BuzzHeavierUploader(obj, str(root))
    monkeypatch.setattr(buzzheavier_module, "AsyncClient", lambda **kwargs: FakeClient())
    created, uploaded = [], []
    async def create_folder(self, name, parent):
        new_id = f"{parent or 'root'}/{name}"
        created.append((name, parent, new_id))
        return new_id
    async def upload_file(self, path, parent):
        uploaded.append((os.path.basename(path), parent))
        self._files += 1
        return f"https://buzzheavier.com/{os.path.basename(path)}"
    monkeypatch.setattr(buzzheavier_module.BuzzHeavierUploader, "_create_directory", create_folder)
    monkeypatch.setattr(buzzheavier_module.BuzzHeavierUploader, "_upload_file", upload_file)
    await uploader.upload()
    remote_root = "root/collection"
    assert ("A", remote_root, f"{remote_root}/A") in created
    assert ("B", remote_root, f"{remote_root}/B") in created
    assert ("child", f"{remote_root}/A", f"{remote_root}/A/child") in created
    assert ("top.txt", remote_root) in uploaded
    assert ("a.txt", f"{remote_root}/A") in uploaded
    assert ("b.txt", f"{remote_root}/B") in uploaded
    assert ("nested.txt", f"{remote_root}/A/child") in uploaded
    obj.on_upload_complete.assert_awaited_once_with(f"https://buzzheavier.com/{remote_root}", 4, 3, "Folder")
    obj.on_upload_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_single_file_destination_and_auth(buzzheavier_module, tmp_path, monkeypatch):
    path = tmp_path / "one.txt"
    path.write_text("one")
    obj = listener("bh:destination123")
    uploader = buzzheavier_module.BuzzHeavierUploader(obj, str(path))
    captured = {}
    def client_factory(**kwargs):
        captured.update(kwargs)
        return FakeClient()
    monkeypatch.setattr(buzzheavier_module, "AsyncClient", client_factory)
    async def upload_file(self, path, parent):
        assert parent == "destination123"
        self._files += 1
        return "https://buzzheavier.com/file123"
    monkeypatch.setattr(buzzheavier_module.BuzzHeavierUploader, "_upload_file", upload_file)
    await uploader.upload()
    assert captured["headers"]["Authorization"] == "Bearer test-account"
    obj.on_upload_complete.assert_awaited_once_with(
        "https://buzzheavier.com/file123", 1, 0, "application/octet-stream"
    )


@pytest.mark.asyncio
async def test_empty_directory_creates_folder(buzzheavier_module, tmp_path, monkeypatch):
    obj = listener()
    uploader = buzzheavier_module.BuzzHeavierUploader(obj, str(tmp_path))
    monkeypatch.setattr(buzzheavier_module, "AsyncClient", lambda **kwargs: FakeClient())
    async def create_folder(self, name, parent):
        return "newfolder"
    monkeypatch.setattr(buzzheavier_module.BuzzHeavierUploader, "_create_directory", create_folder)
    await uploader.upload()
    obj.on_upload_complete.assert_awaited_once_with(
        "https://buzzheavier.com/newfolder", 0, 0, "Folder"
    )


def test_own_account_and_folder_settings(buzzheavier_module, tmp_path):
    obj = listener("mt:bh:folder123", BUZZHEAVIER_ACCOUNT_ID="user-account")
    uploader = buzzheavier_module.BuzzHeavierUploader(obj, str(tmp_path))
    assert uploader._account_id == "user-account"
    assert obj.up_dest == "folder123"


def test_status_api_and_cancel_method(buzzheavier_module, tmp_path):
    obj = listener()
    uploader = buzzheavier_module.BuzzHeavierUploader(obj, str(tmp_path))
    assert uploader.processed_bytes == 0
    assert uploader.speed == 0
    assert callable(uploader.cancel_task)
