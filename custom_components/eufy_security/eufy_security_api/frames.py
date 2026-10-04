"""Frames kept from each time a camera is woken.

When a P2P live stream starts, a short burst of frames is taken from go2rtc's
MJPEG output (a few, spaced apart, so motion shows against stillness) and
saved with the time. The last few wakes are kept on disk, outside `www`, so
they're only ever handed out by the `get_frames` service, which needs a token.

Pure helpers here; the camera entity does the I/O scheduling.
"""
from __future__ import annotations

import base64
import io
import json
import os
import re
import shutil
from datetime import datetime, timezone

#: Frames in a burst, and how far apart.
BURST_FRAMES = 5
BURST_GAP_S = 0.5
#: Longest a burst may take, from the stream starting.
BURST_TIMEOUT_S = 12
#: How far back wakes are kept, and the most kept whatever their age. The
#: newest is always kept, however old, so a camera that hasn't been seen in a
#: while still has a last picture.
KEEP_S = 20 * 60
WAKES_MOST = 120
#: Each camera is woken on its own this often, for one frame, so the last
#: KEEP_S reads as a picture a minute. A look by anyone counts as a wake.
SAMPLE_EVERY_S = 60
#: How often the sampler checks whose turn it is.
SAMPLER_TICK_S = 5
#: Left between a sample's stop and the next start: a start straight after a
#: stop is what the HomeBase refused on 2026-10-04.
COOLDOWN_S = 3
#: Where they're kept, under Home Assistant's config folder.
FRAMES_DIR = "eufy_security_frames"

_LENGTH = re.compile(rb"Content-Length:\s*(\d+)", re.I)


def next_jpeg(buffer: bytes) -> tuple[bytes | None, bytes]:
    """The next whole JPEG in a multipart MJPEG buffer, and what's left after
    it; (None, buffer) when it hasn't all arrived. go2rtc sends each part with
    a Content-Length, which is trusted over scanning for end markers."""
    m = _LENGTH.search(buffer)
    if not m:
        return None, buffer
    start = buffer.find(b"\r\n\r\n", m.end())
    if start < 0:
        return None, buffer
    start += 4
    end = start + int(m.group(1))
    if len(buffer) < end:
        return None, buffer
    return buffer[start:end], buffer[end:]


def save_wake(root: str, serial: str, woken: float, frames: list[tuple[float, bytes]]) -> str:
    """Write one wake's frames and prune the oldest beyond WAKES_KEPT. Returns
    the wake's folder."""
    folder = os.path.join(root, FRAMES_DIR, serial, f"{int(woken * 1000)}")
    os.makedirs(folder, exist_ok=True)
    for i, (_, data) in enumerate(frames):
        with open(os.path.join(folder, f"{i}.jpg"), "wb") as f:
            f.write(data)
    meta = {
        "time": datetime.fromtimestamp(woken, timezone.utc).isoformat(),
        "offsets": [offset for offset, _ in frames],
    }
    with open(os.path.join(folder, "wake.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f)
    for i, old in enumerate(wakes(root, serial)):
        if i and (i >= WAKES_MOST or woken - wake_time(old) > KEEP_S):
            shutil.rmtree(old, ignore_errors=True)
    return folder


def wake_time(folder: str) -> float:
    """When a wake was, from its folder's name (milliseconds)."""
    return int(os.path.basename(folder)) / 1000


def wakes(root: str, serial: str) -> list[str]:
    """This camera's wake folders, newest first."""
    base = os.path.join(root, FRAMES_DIR, serial)
    if not os.path.isdir(base):
        return []
    names = sorted((n for n in os.listdir(base) if n.isdigit()), key=int, reverse=True)
    return [os.path.join(base, n) for n in names]


def resized(data: bytes, width: int) -> bytes:
    """A JPEG no wider than `width`, as JPEG."""
    from PIL import Image  # noqa: PLC0415 — Home Assistant ships Pillow; only needed here

    image = Image.open(io.BytesIO(data))
    if image.width <= width:
        return data
    height = round(image.height * width / image.width)
    out = io.BytesIO()
    image.convert("RGB").resize((width, height), Image.LANCZOS).save(out, "JPEG", quality=85)
    return out.getvalue()


def load_wakes(root: str, serial: str, count: int, width: int, per_wake: int | None = None,
               since: float | None = None) -> list[dict]:
    """The newest `count` wakes as the service answers them, or every wake
    after `since` (epoch seconds): when, and each frame as base64 JPEG with
    its offset in seconds. `per_wake` keeps only that many frames from each,
    taken from the middle of the burst."""
    out = []
    folders = wakes(root, serial)
    folders = [f for f in folders if wake_time(f) >= since] if since is not None else folders[:count]
    for folder in folders:
        try:
            with open(os.path.join(folder, "wake.json"), encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, ValueError):
            continue
        offsets = meta.get("offsets") or []
        picked = list(range(len(offsets)))
        if per_wake and per_wake < len(picked):
            mid = len(picked) // 2
            picked = picked[max(0, mid - per_wake // 2):][:per_wake]
        frames = []
        for i in picked:
            try:
                with open(os.path.join(folder, f"{i}.jpg"), "rb") as f:
                    data = f.read()
            except OSError:
                continue
            frames.append({"offset": offsets[i], "image": base64.b64encode(resized(data, width)).decode()})
        if frames:
            out.append({"time": meta.get("time"), "frames": frames})
    return out
