"""SABnzbd security hardening for MLTB.

MLTB restores ``sabnzbd/SABnzbd.ini`` from MongoDB on every start, so any
insecure value stored there (public default keys, ``host = ::``,
``inet_exposure = 4``, scripts) would come back after each restart.
``aria-nox-nzb.sh`` therefore runs this module right before ``sabnzbdplus``:

    python3 -m sabnzbdapi.hardening sabnzbd/SABnzbd.ini

It forces SABnzbd to loopback-only, no internet exposure, enabled login and
no user scripts. Missing or weak keys/credentials (e.g. the public ``mltb``
defaults from the repository) are replaced by random values generated on the
server. The generated values never leave the server: they live only in the
ini file (mode 600) and in the bot's own MongoDB record, never in git.

Exit status is non-zero when the ini cannot be hardened or the installed
SABnzbd is older than ``MIN_VERSION``; the shell script then does not start
SABnzbd at all (fail closed).
"""

from os import chmod, fdopen, path, remove, replace
from re import compile as re_compile, search
from secrets import token_hex, token_urlsafe
from subprocess import run
from tempfile import mkstemp
import sys

INI_PATH = "sabnzbd/SABnzbd.ini"

# 5.1.3 is the latest stable release and fixes the critical/high advisories
# (unauthenticated API access, auth bypass, config takeover / RCE).
MIN_VERSION = (5, 1, 3)

LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = "8070"

# Values the bot must not change from Telegram (they would re-open SABnzbd).
PROTECTED_KEYS = frozenset(
    {
        "host",
        "port",
        "https_port",
        "enable_https",
        "inet_exposure",
        "api_key",
        "nzb_key",
        "username",
        "password",
        "html_login",
        "local_ranges",
        "host_whitelist",
        "script_dir",
        "pre_script",
        "email_dir",
        "password_file",
        "api_warnings",
    }
)

_FIXED_MISC = {
    "host": LISTEN_HOST,
    "port": LISTEN_PORT,
    "inet_exposure": "0",
    "html_login": "1",
    "enable_https": "0",
    "https_port": '""',
    "script_dir": '""',
    "pre_script": "None",
}
_FIXED_NSCRIPT = {"nscript_enable": "0", "nscript_script": '""'}

_KEY_RE = re_compile(r"^[0-9a-f]{32}$")
_SEC_RE = re_compile(r"^\[([^\[\]]+)\]\s*$")
_LINE_RE = re_compile(r"^(\s*)([A-Za-z0-9_]+)\s*=\s*(.*?)\s*$")


def _unquote(value):
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == '"':
        return value[1:-1]
    return value


def _strong_key(value):
    return bool(_KEY_RE.match(_unquote(value)))


def _strong_password(value):
    v = _unquote(value)
    return len(v) >= 24 and v not in {"mltb", "mltbmltb"}


def _strong_username(value):
    v = _unquote(value)
    return len(v) >= 8 and v != "mltb"


def read_misc(ini_path=INI_PATH):
    """Return the [misc] section as {key: unquoted value}."""
    values, section = {}, None
    try:
        with open(ini_path, encoding="utf-8") as f:
            for line in f:
                if m := _SEC_RE.match(line.strip()):
                    section = m.group(1)
                elif section == "misc" and (m := _LINE_RE.match(line)):
                    values[m.group(2)] = _unquote(m.group(3))
    except OSError:
        pass
    return values


def read_api_key(ini_path=INI_PATH):
    return read_misc(ini_path).get("api_key", "")


def _apply(lines, section, values):
    out, in_sec, seen, found = [], False, set(), False
    for line in lines:
        if m := _SEC_RE.match(line.strip()):
            if in_sec:
                out += [f"{k} = {v}\n" for k, v in values.items() if k not in seen]
            in_sec = m.group(1) == section
            found = found or in_sec
        elif in_sec and (m := _LINE_RE.match(line)) and m.group(2) in values:
            seen.add(m.group(2))
            line = f"{m.group(1)}{m.group(2)} = {values[m.group(2)]}\n"
        out.append(line)
    if in_sec:
        out += [f"{k} = {v}\n" for k, v in values.items() if k not in seen]
    elif not found:
        out += [f"[{section}]\n"] + [f"{k} = {v}\n" for k, v in values.items()]
    return out


def harden(ini_path=INI_PATH):
    """Harden the ini in place. Returns the list of names that were changed."""
    with open(ini_path, encoding="utf-8") as f:
        lines = f.readlines()
    if not any(_SEC_RE.match(l.strip()) for l in lines):
        raise ValueError("not a SABnzbd ini file")
    current = read_misc(ini_path)
    misc = dict(_FIXED_MISC)
    if not _strong_key(current.get("api_key", "")):
        misc["api_key"] = token_hex(16)
    if not _strong_key(current.get("nzb_key", "")):
        misc["nzb_key"] = token_hex(16)
    if misc.get("api_key") and misc.get("api_key") == misc.get("nzb_key"):
        misc["nzb_key"] = token_hex(16)
    if current.get("api_key") and current.get("api_key") == current.get("nzb_key"):
        misc["nzb_key"] = token_hex(16)
    if not _strong_username(current.get("username", "")):
        misc["username"] = f'"mltb-{token_hex(4)}"'
    if not _strong_password(current.get("password", "")):
        misc["password"] = f'"{token_urlsafe(24)}"'
    changed = sorted(
        k
        for k, v in misc.items()
        if k in {"api_key", "nzb_key", "username", "password"}
        or _unquote(current.get(k, "<missing>")) != _unquote(v)
    )
    lines = _apply(lines, "misc", misc)
    lines = _apply(lines, "nscript", _FIXED_NSCRIPT)
    directory = path.dirname(path.abspath(ini_path))
    fd, tmp = mkstemp(dir=directory, prefix=".SABnzbd.ini.")
    try:
        with fdopen(fd, "w", encoding="utf-8") as f:
            f.writelines(lines)
        chmod(tmp, 0o600)
        replace(tmp, ini_path)
    except BaseException:
        if path.exists(tmp):
            remove(tmp)
        raise
    if path.exists(f"{ini_path}.bak"):  # may still hold the old public keys
        remove(f"{ini_path}.bak")
    return changed


def installed_version():
    try:
        out = run(
            ["sabnzbdplus", "--version"], capture_output=True, text=True, timeout=60
        ).stdout
    except Exception:
        return None
    if m := search(r"(\d+)\.(\d+)\.(\d+)", out or ""):
        return tuple(int(x) for x in m.groups())
    return None


def main(argv):
    ini_path = argv[1] if len(argv) > 1 else INI_PATH
    version = installed_version()
    if version is None or version < MIN_VERSION:
        need = ".".join(map(str, MIN_VERSION))
        have = ".".join(map(str, version)) if version else "unknown"
        print(
            f"sabnzbd-hardening: SABnzbd {have} < {need}; rebuild the image. "
            "SABnzbd NOT started.",
            file=sys.stderr,
        )
        return 2
    try:
        changed = harden(ini_path)
    except Exception as e:
        print(
            f"sabnzbd-hardening: failed ({type(e).__name__}); SABnzbd NOT started.",
            file=sys.stderr,
        )
        return 1
    # Names only - never print key or password values.
    print(
        f"sabnzbd-hardening: OK (SABnzbd {'.'.join(map(str, version))}, "
        f"{LISTEN_HOST}:{LISTEN_PORT}, inet_exposure=0); "
        f"enforced: {', '.join(changed) or 'nothing to change'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
