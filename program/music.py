

import os
import re
import asyncio
import subprocess
from time import time
import logging       
from config import ASSISTANT_NAME, BOT_USERNAME, COOKIES_FILE, IMG_1, IMG_2, MAX_QUEUE_SIZE, PROXY_URL, SPONSORBLOCK_REMOVE
from driver.design.thumbnail import thumb
from driver.design.chatname import CHAT_TITLE
from driver.decorators import errors
from driver.filters import command, other_filters
from driver.queues import QUEUE, add_to_queue, drop_if_live, set_active_thread
from driver.clients import call_py, user
from driver.utils import get_assistant_member, bash, make_progress, control_panel, media_audio, drop_stale_queue
from pyrogram import Client
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import UserAlreadyParticipant, UserNotParticipant, PeerIdInvalid
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from youtubesearchpython import VideosSearch

log = logging.getLogger(__name__)


def log_ytdlp_version():
    """Log installed yt-dlp and yt-dlp-ejs versions at startup."""
    try:
        result = subprocess.run(
            ["yt-dlp", "--version"],
            capture_output=True,
            text=True,
            timeout=15,
            check=True,
        )
        log.info("YT-DLP VERSION: %s", result.stdout.strip())
    except Exception:
        log.exception("Could not check yt-dlp version")

    try:
        from importlib.metadata import version, PackageNotFoundError
        try:
            log.info("YT-DLP-EJS VERSION: %s", version("yt-dlp-ejs"))
        except PackageNotFoundError:
            log.warning("YT-DLP-EJS is not installed")
    except Exception:
        log.exception("Could not check yt-dlp-ejs version")


log_ytdlp_version()

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)


def _ytdl_site_flags():
    """Build extra yt-dlp flags for non-YouTube sites (cookies, user-agent, proxy)."""
    flags = ["--socket-timeout", "60"]
    if COOKIES_FILE:
        flags += ["--cookies", COOKIES_FILE]
    flags += ["--user-agent", _DEFAULT_USER_AGENT]
    if PROXY_URL:
        flags += ["--proxy", PROXY_URL]
    return flags


def ytsearch(query: str):
    try:
        q = query.strip()
        is_yt = bool(re.match(r"https?://(www\.|m\.)?(youtube\.com|youtu\.be)/", q))
        if re.match(r"https?://", q):
            # any direct URL — use yt-dlp for metadata (handles YouTube, Rutube, Vimeo, …)
            cmd = ["yt-dlp", "--no-warnings", "--skip-download",
                   "--print", "%(title)s\x1f%(duration_string)s\x1f%(id)s\x1f%(thumbnail)s"]
            if not is_yt:
                cmd += _ytdl_site_flags()
            cmd.append(q)
            out = subprocess.run(
                cmd,
                capture_output=True, text=True, timeout=90,
            ).stdout.strip().split("\x1f")
            title = out[0] if out and out[0] else q
            duration = out[1] if len(out) > 1 else ""
            vid = out[2] if len(out) > 2 and out[2] else ""
            raw_thumb = out[3] if len(out) > 3 else ""
            # prefer the well-known HQ thumbnail URL for YouTube
            thumbnail = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if is_yt and vid else raw_thumb
            return [title, q, duration, thumbnail]
        search = VideosSearch(query, limit=1).result()
        data = search["result"][0]
        songname = data["title"]
        url = data["link"]
        duration = data["duration"]
        thumbnail = f"https://i.ytimg.com/vi/{data['id']}/hqdefault.jpg"
        return [songname, url, duration, thumbnail]
    except Exception as e:
        log.warning("ytsearch failed: %s", e)
        return 0


_YT_RE = re.compile(r"https?://(www\.|m\.)?(youtube\.com|youtu\.be)/")



async def ytdl(format: str, link: str, status_msg=None):
    """Return (1, stream_url_or_filepath) or (0, error_message)."""
    try:
        if not link or not isinstance(link, str):
            return 0, "Invalid media URL."

        is_youtube = bool(_YT_RE.match(link))

        # Non-YouTube sites: extract a direct stream URL.
        if not is_youtube:
            command_args = [
                "yt-dlp",
                "--no-warnings",
                "--no-playlist",
                *_ytdl_site_flags(),
                "-f", "worstaudio/worst",
                "--print", "%(url)s",
                link,
            ]

            proc = await asyncio.create_subprocess_exec(
                *command_args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            stdout, stderr = await proc.communicate()
            stream_url = stdout.decode(
                "utf-8", errors="replace"
            ).strip()

            if proc.returncode == 0 and stream_url:
                return 1, stream_url

            error_text = stderr.decode(
                "utf-8", errors="replace"
            ).strip()

            return 0, error_text[-1000:] or (
                "Could not extract the media stream URL."
            )

        # YouTube: download audio before handing it to the player.
        os.makedirs("downloads", exist_ok=True)

        command_args = [
            "yt-dlp",
            "--no-warnings",
            "--no-playlist",
            "--socket-timeout", "60",
        ]

        if COOKIES_FILE and os.path.isfile(COOKIES_FILE):
            command_args += ["--cookies", COOKIES_FILE]

        if SPONSORBLOCK_REMOVE:
            command_args += [
                "--sponsorblock-remove",
                SPONSORBLOCK_REMOVE,
            ]

        command_args += [
            "--newline",
            "--progress-template",
            (
                "download:PROG|%(progress._percent_str)s|"
                "%(progress._speed_str)s|%(progress._eta_str)s"
            ),
            "--print", "after_move:filepath",
            "-f", format or "bestaudio/best",
            "-o", "downloads/%(id)s.%(ext)s",
            link,
        ]

        proc = await asyncio.create_subprocess_exec(
            *command_args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()

        output_text = stdout.decode(
            "utf-8", errors="replace"
        ).strip()

        error_text = stderr.decode(
            "utf-8", errors="replace"
        ).strip()

        if proc.returncode != 0:
            return 0, (
                error_text[-1200:]
                or output_text[-1200:]
                or "yt-dlp failed to download the audio."
            )

        # Find the actual downloaded file path.
        for line in reversed(output_text.splitlines()):
            filepath = line.strip()

            if filepath and os.path.isfile(filepath):
                return 1, filepath

        return 0, (
            "Download finished, but the output file was not found. "
            + output_text[-500:]
        )

    except FileNotFoundError:
        log.exception("yt-dlp executable was not found")
        return 0, "yt-dlp is missing. Check the Render dependencies."

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        log.exception("ytdl failed")
        return 0, f"{type(exc).__name__}: {exc}"

@Client.on_message(command(["play", f"play@{BOT_USERNAME}"]) & other_filters)
@errors
async def play(c: Client, m: Message):
    # snapshot before any await: pyrogram's message-cache can mutate .command
    # back to None on a cached Message once another handler's filter check
    # runs against it concurrently, which happens easily once we start
    # awaiting below (a bare `m.command` read later crashed with
    # "TypeError: object of type 'NoneType' has no len()")
    cmd_args = m.command
    await m.delete()
    replied = m.reply_to_message
    chat_id = m.chat.id
    thread_id = m.message_thread_id if getattr(m, "topic_message", False) else None
    keyboard = control_panel
    if m.sender_chat:
        return await m.reply_text("you're an __Anonymous__ Admin !\n\n» revert back to user account from admin rights.")
    try:
        aing = await c.get_me()
    except Exception as e:
        return await m.reply_text(f"error:\n\n{e}")
    a = await c.get_chat_member(chat_id, aing.id)
    if a.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
        await m.reply_text(
            f"💡 To use me, I need to be an **Administrator** with the following **permissions**:\n\n» ❌ __Delete messages__\n» ❌ __Add users__\n» ❌ __Manage video chat__\n\nData is **updated** automatically after you **promote me**"
        )
        return
    priv = a.privileges
    if not (priv and priv.can_manage_video_chats):
        await m.reply_text(
            "missing required permission:" + "\n\n» ❌ __Manage video chat__"
        )
        return
    if not (priv and priv.can_delete_messages):
        await m.reply_text(
            "missing required permission:" + "\n\n» ❌ __Delete messages__"
        )
        return
    if not (priv and priv.can_invite_users):
        await m.reply_text("missing required permission:" + "\n\n» ❌ __Add users__")
        return
    try:
        ubot = (await user.get_me()).id
        b = await get_assistant_member(c, chat_id, ubot)
        if b.status == ChatMemberStatus.BANNED:
            await m.reply_text(
                f"@{ASSISTANT_NAME} **is banned in group** {m.chat.title}\n\n» **unban the userbot first if you want to use this bot.**"
            )
            return
    except (UserNotParticipant, PeerIdInvalid):
        if m.chat.username:
            try:
                await user.join_chat(m.chat.username)
            except UserAlreadyParticipant:
                pass
            except Exception as e:
                await m.reply_text(f"❌ **userbot failed to join**\n\n**reason**: `{e}`")
                return
        else:
            try:
                invitelink = await c.export_chat_invite_link(
                    m.chat.id
                )
                if invitelink.startswith("https://t.me/+"):
                    invitelink = invitelink.replace(
                        "https://t.me/+", "https://t.me/joinchat/"
                    )
                await user.join_chat(invitelink)
            except UserAlreadyParticipant:
                pass
            except Exception as e:
                return await m.reply_text(
                    f"❌ **userbot failed to join**\n\n**reason**: `{e}`"
                )
    # if a previous stream died silently, clear the stale queue so we rejoin
    await drop_stale_queue(chat_id)
    # radio/IPTV never hold a real queue slot — a new /play takes over now
    drop_if_live(chat_id)
    # set after the drop_*() calls above, which may clear_queue() (and with it
    # any stale ACTIVE_THREAD entry) before this session's is recorded
    set_active_thread(chat_id, thread_id)
    if replied:
        # audio sent as a generic file/document (e.g. a bare .mp3) counts too
        audio_doc = (
            replied.document
            if replied.document and (replied.document.mime_type or "").startswith("audio/")
            else None
        )
        if replied.audio or replied.voice or audio_doc:
            media = replied.audio or replied.voice or audio_doc
            ext = os.path.splitext(getattr(media, "file_name", None) or "")[1] or ".m4a"
            cached = os.path.join(os.getcwd(), "downloads", f"{media.file_unique_id}{ext}")
            if os.path.exists(cached) and os.path.getsize(cached) == media.file_size:
                # same Telegram file already fully downloaded — reuse it
                os.utime(cached, None)  # mark fresh for the LRU cache pruner
                suhu = await replied.reply("📦 **already downloaded — starting...**")
                dl = cached
            else:
                suhu = await replied.reply("📥 **downloading audio...**")
                dl = await replied.download(
                    file_name=cached,
                    progress=make_progress(suhu, "📥 Downloading audio"),
                )
            link = replied.link
            # Title preference: audio metadata title > caption > file name > generic
            if replied.audio and replied.audio.title:
                songname = replied.audio.title[:70]
            elif replied.caption:
                songname = str(replied.caption).splitlines()[0][:70]
            elif getattr(media, "file_name", None):
                songname = media.file_name[:70]
            elif replied.voice:
                songname = "Voice Note"
            else:
                songname = "Audio"
            if chat_id in QUEUE:
                pos = add_to_queue(chat_id, songname, dl, link, "Audio", 0)
                if pos == -1:
                    return await suhu.edit(f"🚫 queue is full (max {MAX_QUEUE_SIZE}).")
                await suhu.delete()
                await m.reply_photo(
                    photo=f"{IMG_1}",
                    caption=f"💡 **Track added to queue »** `{pos}`\n\n🏷 **Name:** [{songname}]({link}) | `music`\n💭 **Chat:** `{chat_id}`\n🎧 **Request by:** {m.from_user.mention()}",
                    reply_markup=keyboard,
                )
            else:
             try:
                await suhu.edit("🔄 **Joining vc...**")
                await call_py.play(chat_id, media_audio(dl))
                add_to_queue(chat_id, songname, dl, link, "Audio", 0)
                await suhu.delete()
                requester = f"[{m.from_user.first_name}](tg://user?id={m.from_user.id})"
                await m.reply_photo(
                    photo=f"{IMG_2}",
                    caption=f"🏷 **Name:** [{songname}]({link})\n💭 **Chat:** `{chat_id}`\n💡 **Status:** `Playing`\n🎧 **Request by:** {requester}\n📹 **Stream type:** `Music`",
                    reply_markup=keyboard,
                )
             except Exception as e:
                await suhu.delete()
                await m.reply_text(f"🚫 error:\n\n» {e}")
        else:
            if len(cmd_args) < 2:
                await m.reply(
                    "» reply to an **audio file** or **give something to search.**"
                )
            else:
                suhu = await c.send_message(chat_id, "🔍 **Searching...**", message_thread_id=thread_id)
                query = m.text.split(None, 1)[1]
                search = ytsearch(query)
                if search == 0:
                    await suhu.edit("❌ **no results found.**")
                else:
                    songname = search[0]
                    title = search[0]
                    url = search[1]
                    duration = search[2]
                    thumbnail = search[3]
                    userid = m.from_user.id
                    gcname = m.chat.title
                    try:
                        ctitle = await CHAT_TITLE(gcname)
                        image = await thumb(thumbnail, title, userid, ctitle)
                    except Exception as e:
                        return await suhu.edit(f"🚫 couldn't build the thumbnail: `{e}`")
                    format = "bestaudio[ext=m4a]"
                    ok, ytlink = await ytdl(format, url, suhu)
                    if ok == 0:
                        await suhu.edit(f"❌ yt-dl issues detected\n\n» `{ytlink}`")
                    else:
                        if chat_id in QUEUE:
                            pos = add_to_queue(
                                chat_id, songname, ytlink, url, "Audio", 0
                            )
                            if pos == -1:
                                return await suhu.edit(f"🚫 queue is full (max {MAX_QUEUE_SIZE}).")
                            await suhu.delete()
                            requester = f"[{m.from_user.first_name}](tg://user?id={m.from_user.id})"
                            await m.reply_photo(
                                photo=image,
                                caption=f"💡 **Track added to queue »** `{pos}`\n\n🏷 **Name:** [{songname}]({url}) | `music`\n**⏱ Duration:** `{duration}`\n🎧 **Request by:** {requester}",
                                reply_markup=keyboard,
                            )
                        else:
                            try:
                                await suhu.edit("🔄 **Joining vc...**")
                                await call_py.play(chat_id, media_audio(ytlink))
                                add_to_queue(chat_id, songname, ytlink, url, "Audio", 0)
                                await suhu.delete()
                                requester = f"[{m.from_user.first_name}](tg://user?id={m.from_user.id})"
                                await m.reply_photo(
                                    photo=image,
                                    caption=f"🏷 **Name:** [{songname}]({url})\n**⏱ Duration:** `{duration}`\n💡 **Status:** `Playing`\n🎧 **Request by:** {requester}\n📹 **Stream type:** `Music`",
                                    reply_markup=keyboard,
                                )
                            except Exception as ep:
                                await suhu.delete()
                                await m.reply_text(f"🚫 error: `{ep}`")

    else:
        if len(cmd_args) < 2:
            await m.reply(
                "» reply to an **audio file** or **give something to search.**"
            )
        else:
            suhu = await c.send_message(chat_id, "🔍 **Searching...**", message_thread_id=thread_id)
            query = m.text.split(None, 1)[1]
            search = ytsearch(query)
            if search == 0:
                await suhu.edit("❌ **no results found.**")
            else:
                songname = search[0]
                title = search[0]
                url = search[1]
                duration = search[2]
                thumbnail = search[3]
                userid = m.from_user.id
                gcname = m.chat.title
                try:
                    ctitle = await CHAT_TITLE(gcname)
                    image = await thumb(thumbnail, title, userid, ctitle)
                except Exception as e:
                    return await suhu.edit(f"🚫 couldn't build the thumbnail: `{e}`")
                format = "bestaudio[ext=m4a]"
                ok, ytlink = await ytdl(format, url, suhu)
                if ok == 0:
                    await suhu.edit(f"❌ yt-dl issues detected\n\n» `{ytlink}`")
                else:
                    if chat_id in QUEUE:
                        pos = add_to_queue(chat_id, songname, ytlink, url, "Audio", 0)
                        if pos == -1:
                            return await suhu.edit(f"🚫 queue is full (max {MAX_QUEUE_SIZE}).")
                        await suhu.delete()
                        requester = (
                            f"[{m.from_user.first_name}](tg://user?id={m.from_user.id})"
                        )
                        await m.reply_photo(
                            photo=image,
                            caption=f"💡 **Track added to queue »** `{pos}`\n\n🏷 **Name:** [{songname}]({url}) | `music`\n**⏱ Duration:** `{duration}`\n🎧 **Request by:** {requester}",
                            reply_markup=keyboard,
                        )
                    else:
                        try:
                            await suhu.edit("🔄 **Joining vc...**")
                            await call_py.play(chat_id, media_audio(ytlink))
                            add_to_queue(chat_id, songname, ytlink, url, "Audio", 0)
                            await suhu.delete()
                            requester = f"[{m.from_user.first_name}](tg://user?id={m.from_user.id})"
                            await m.reply_photo(
                                photo=image,
                                caption=f"🏷 **Name:** [{songname}]({url})\n**⏱ Duration:** `{duration}`\n💡 **Status:** `Playing`\n🎧 **Request by:** {requester}\n📹 **Stream type:** `Music`",
                                reply_markup=keyboard,
                            )
                        except Exception as ep:
                            await suhu.delete()
                            await m.reply_text(f"🚫 error: `{ep}`")
