from html import escape
from re import search

from ..core.config_manager import Config
from ..helper.ext_utils.bot_utils import new_task
from ..helper.ext_utils.url_shortener import SHORTENER_HOSTS, shorten_url
from ..helper.telegram_helper.message_utils import send_message


def _extract_url(text):
    if not text:
        return ""

    match = search(r"https?://\S+", text)
    if match:
        return match.group(0)

    return text.strip().split()[0] if text.strip() else ""


def _get_short_args(message):
    host = None
    url = ""

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        args = parts[1].strip()
        arg_parts = args.split(maxsplit=1)

        if arg_parts and arg_parts[0].lower() in SHORTENER_HOSTS:
            host = arg_parts[0].lower()
            if len(arg_parts) > 1:
                url = _extract_url(arg_parts[1])
        else:
            url = _extract_url(args)

    if not url and message.reply_to_message:
        reply = message.reply_to_message
        url = _extract_url(reply.text or reply.caption or "")

    return host, url


@new_task
async def short_url(_, message):
    host, url = _get_short_args(message)

    if not url:
        hosts = "/".join(SHORTENER_HOSTS.keys())
        await send_message(
            message,
            "<b>Send URL or reply to URL.</b>\n\n"
            "<code>/short https://example.com</code>\n"
            "<code>/short cleanuri https://example.com</code>\n\n"
            f"Hosts: <code>{hosts}</code>",
        )
        return

    try:
        short, provider = await shorten_url(url, host)
    except Exception as exc:
        await send_message(
            message,
            f"<b>Shortener Error:</b> <code>{escape(str(exc))}</code>",
        )
        return

    await send_message(
        message,
        f"🔗 <code>{escape(short)}</code>\n"
        f"Provider: {escape(provider)}",
    )