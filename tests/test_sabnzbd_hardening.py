import re
import shutil
from pathlib import Path

from sabnzbdapi import hardening

REPO_INI = Path(__file__).resolve().parent.parent / "sabnzbd" / "SABnzbd.ini"

INSECURE = """[misc]
host = ::
port = 8070
username = "mltb"
password = "mltbmltb"
inet_exposure = 4
api_key = mltb
nzb_key = mltb
html_login = 0
script_dir = /tmp/evil
pre_script = evil.sh
[nscript]
nscript_enable = 1
nscript_script = evil.sh
[servers]
[[news.example]]
host = news.example
"""


def _misc(path):
    return hardening.read_misc(path)


def test_insecure_ini_is_hardened(tmp_path):
    ini = tmp_path / "SABnzbd.ini"
    ini.write_text(INSECURE)
    (tmp_path / "SABnzbd.ini.bak").write_text(INSECURE)
    changed = hardening.harden(str(ini))
    m = _misc(str(ini))
    assert m["host"] == "127.0.0.1" and m["port"] == "8070"
    assert m["inet_exposure"] == "0" and m["html_login"] == "1"
    assert re.fullmatch(r"[0-9a-f]{32}", m["api_key"])
    assert re.fullmatch(r"[0-9a-f]{32}", m["nzb_key"])
    assert m["api_key"] != m["nzb_key"]
    assert m["username"] != "mltb" and len(m["password"]) >= 24
    assert m["script_dir"] == "" and m["pre_script"] == "None"
    text = ini.read_text()
    assert "nscript_enable = 0" in text and "evil" not in text
    # server sub-section with its own "host" must be untouched
    assert "host = news.example" in text
    assert not (tmp_path / "SABnzbd.ini.bak").exists()
    assert (ini.stat().st_mode & 0o777) == 0o600
    assert {"api_key", "nzb_key", "host", "inet_exposure"} <= set(changed)


def test_strong_keys_are_kept_across_restarts(tmp_path):
    ini = tmp_path / "SABnzbd.ini"
    ini.write_text(INSECURE)
    hardening.harden(str(ini))
    first = _misc(str(ini))
    hardening.harden(str(ini))  # simulated restart (DB restore of hardened ini)
    second = _misc(str(ini))
    for k in ("api_key", "nzb_key", "username", "password"):
        assert first[k] == second[k]
    assert hardening.read_api_key(str(ini)) == first["api_key"]


def test_repo_ini_has_no_public_keys(tmp_path):
    m = _misc(str(REPO_INI))
    assert m["api_key"] == "" and m["nzb_key"] == "" and m["password"] == ""
    assert m["host"] == "127.0.0.1" and m["inet_exposure"] == "0"
    ini = tmp_path / "SABnzbd.ini"
    shutil.copy(REPO_INI, ini)
    hardening.harden(str(ini))
    assert re.fullmatch(r"[0-9a-f]{32}", _misc(str(ini))["api_key"])


def test_garbage_file_fails_closed(tmp_path):
    ini = tmp_path / "SABnzbd.ini"
    ini.write_text("not an ini\n")
    try:
        hardening.harden(str(ini))
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_protected_keys_cover_exposure_settings():
    for k in ("host", "port", "inet_exposure", "api_key", "nzb_key", "password"):
        assert k in hardening.PROTECTED_KEYS


def test_old_version_refused(monkeypatch, tmp_path):
    ini = tmp_path / "SABnzbd.ini"
    ini.write_text(INSECURE)
    monkeypatch.setattr(hardening, "installed_version", lambda: (5, 0, 3))
    assert hardening.main(["x", str(ini)]) == 2
    assert "mltb" in ini.read_text()  # untouched, SABnzbd not started
