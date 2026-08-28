#!/usr/bin/env python3
"""Record docs/demo.mp4 by capturing the Chromium demo window (not the screen)."""

from __future__ import annotations

import atexit
import ctypes
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
from ctypes import (
    POINTER,
    byref,
    c_char_p,
    c_int,
    c_long,
    c_uint,
    c_ulong,
    c_void_p,
    cast,
    CFUNCTYPE,
    Structure,
)
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "target" / "debug"
DEMO = ROOT / "target" / "demo"
OUT = ROOT / "docs" / "demo.mp4"
DURATION = 24.0
FPS = 30
PLAY_AT = 1.2

os.environ.setdefault("DISPLAY", ":0.0")
os.environ.setdefault("XAUTHORITY", str(Path.home() / ".Xauthority"))
os.environ.setdefault(
    "DATABASE_URL", "postgres://postgres:postgres@127.0.0.1:5432/plane"
)

CHILDREN: list[subprocess.Popen] = []

x11 = ctypes.CDLL("libX11.so.6")
xcomp = ctypes.CDLL("libXcomposite.so.1")
xfixes = ctypes.CDLL("libXfixes.so.3")

ZPixmap = 2
AllPlanes = c_ulong(~0)
CompositeRedirectAutomatic = 0


class XImage(Structure):
    _fields_ = [
        ("width", c_int),
        ("height", c_int),
        ("xoffset", c_int),
        ("format", c_int),
        ("data", c_void_p),
        ("byte_order", c_int),
        ("bitmap_unit", c_int),
        ("bitmap_bit_order", c_int),
        ("bitmap_pad", c_int),
        ("depth", c_int),
        ("bytes_per_line", c_int),
        ("bits_per_pixel", c_int),
        ("red_mask", c_ulong),
        ("green_mask", c_ulong),
        ("blue_mask", c_ulong),
        ("obdata", c_void_p),
        ("f", c_void_p * 6),
    ]


x11.XOpenDisplay.restype = c_void_p
x11.XOpenDisplay.argtypes = [c_char_p]
x11.XCloseDisplay.argtypes = [c_void_p]
x11.XDefaultRootWindow.restype = c_ulong
x11.XDefaultRootWindow.argtypes = [c_void_p]
x11.XQueryTree.restype = c_int
x11.XQueryTree.argtypes = [
    c_void_p,
    c_ulong,
    POINTER(c_ulong),
    POINTER(c_ulong),
    POINTER(POINTER(c_ulong)),
    POINTER(c_uint),
]
x11.XFetchName.restype = c_int
x11.XFetchName.argtypes = [c_void_p, c_ulong, POINTER(c_char_p)]
x11.XGetGeometry.restype = c_int
x11.XGetGeometry.argtypes = [
    c_void_p,
    c_ulong,
    POINTER(c_ulong),
    POINTER(c_int),
    POINTER(c_int),
    POINTER(c_uint),
    POINTER(c_uint),
    POINTER(c_uint),
    POINTER(c_uint),
]
x11.XGetImage.restype = POINTER(XImage)
x11.XGetImage.argtypes = [
    c_void_p,
    c_ulong,
    c_int,
    c_int,
    c_uint,
    c_uint,
    c_ulong,
    c_int,
]
x11.XDestroyImage.restype = c_int
x11.XDestroyImage.argtypes = [POINTER(XImage)]
x11.XFree.argtypes = [c_void_p]
x11.XFreePixmap.argtypes = [c_void_p, c_ulong]
x11.XWarpPointer.argtypes = [
    c_void_p,
    c_ulong,
    c_ulong,
    c_int,
    c_int,
    c_uint,
    c_uint,
    c_int,
    c_int,
]
x11.XFlush.argtypes = [c_void_p]
x11.XSync.argtypes = [c_void_p, c_int]
x11.XSetErrorHandler.restype = c_void_p
x11.XSetErrorHandler.argtypes = [c_void_p]
x11.XInternAtom.restype = c_ulong
x11.XInternAtom.argtypes = [c_void_p, c_char_p, c_int]
x11.XGetWindowProperty.restype = c_int
x11.XGetWindowProperty.argtypes = [
    c_void_p,
    c_ulong,
    c_ulong,
    c_long,
    c_long,
    c_int,
    c_ulong,
    POINTER(c_ulong),
    POINTER(c_int),
    POINTER(c_ulong),
    POINTER(c_ulong),
    POINTER(c_void_p),
]

xcomp.XCompositeQueryExtension.restype = c_int
xcomp.XCompositeQueryExtension.argtypes = [c_void_p, POINTER(c_int), POINTER(c_int)]
xcomp.XCompositeRedirectWindow.argtypes = [c_void_p, c_ulong, c_int]
xcomp.XCompositeNameWindowPixmap.restype = c_ulong
xcomp.XCompositeNameWindowPixmap.argtypes = [c_void_p, c_ulong]
xfixes.XFixesQueryExtension.restype = c_int
xfixes.XFixesQueryExtension.argtypes = [c_void_p, POINTER(c_int), POINTER(c_int)]
xfixes.XFixesHideCursor.argtypes = [c_void_p, c_ulong]
xfixes.XFixesShowCursor.argtypes = [c_void_p, c_ulong]


@CFUNCTYPE(c_int, c_void_p, c_void_p)
def _xerr(_dpy, _ev):
    return 0


_KEEP_HANDLER = _xerr


def die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def cleanup() -> None:
    for p in reversed(CHILDREN):
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    time.sleep(0.2)
    for p in reversed(CHILDREN):
        if p.poll() is None:
            p.kill()


def kill_port(port: int) -> None:
    try:
        out = subprocess.check_output(
            ["ss", "-lptn", f"sport = :{port}"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return
    for pid in set(re.findall(r"pid=(\d+)", out)):
        try:
            os.kill(int(pid), signal.SIGTERM)
        except OSError:
            pass


def wait_http(url: str, tries: int = 80) -> None:
    for _ in range(tries):
        try:
            with urllib.request.urlopen(url, timeout=0.5) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.25)
    die(f"{url} did not become ready")


def children_of(dpy, win: int) -> list[int]:
    root = c_ulong()
    parent = c_ulong()
    kids = POINTER(c_ulong)()
    n = c_uint()
    if not x11.XQueryTree(dpy, win, byref(root), byref(parent), byref(kids), byref(n)):
        return []
    out = [kids[i] for i in range(n.value)]
    if kids:
        x11.XFree(cast(kids, c_void_p))
    return out


def net_wm_name(dpy, win: int) -> str:
    atom = x11.XInternAtom(dpy, b"_NET_WM_NAME", 0)
    utf8 = x11.XInternAtom(dpy, b"UTF8_STRING", 0)
    actual_type = c_ulong()
    actual_fmt = c_int()
    nitems = c_ulong()
    bytes_after = c_ulong()
    prop = c_void_p()
    status = x11.XGetWindowProperty(
        dpy,
        win,
        atom,
        0,
        1024,
        0,
        utf8,
        byref(actual_type),
        byref(actual_fmt),
        byref(nitems),
        byref(bytes_after),
        byref(prop),
    )
    if status != 0 or not prop:
        return ""
    s = ctypes.string_at(prop, nitems.value).decode("utf-8", "replace")
    x11.XFree(prop)
    return s


def window_name(dpy, win: int) -> str:
    n = net_wm_name(dpy, win)
    if n:
        return n
    name = c_char_p()
    if x11.XFetchName(dpy, win, byref(name)) and name.value:
        s = name.value.decode("utf-8", "replace")
        x11.XFree(name)
        return s
    return ""


def find_via_xwininfo(title: str) -> int:
    try:
        out = subprocess.check_output(
            ["xwininfo", "-name", title],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return 0
    m = re.search(r"Window id:\s+(0x[0-9a-fA-F]+)", out)
    return int(m.group(1), 16) if m else 0


def find_window(dpy, title: str) -> int:
    wid = find_via_xwininfo(title)
    if wid:
        return wid
    root = x11.XDefaultRootWindow(dpy)
    stack = children_of(dpy, root)
    while stack:
        w = stack.pop()
        name = window_name(dpy, w)
        if name == title or name.startswith(title):
            return w
        stack.extend(children_of(dpy, w))
    return 0


def geometry(dpy, win: int) -> tuple[int, int]:
    root = c_ulong()
    x = c_int()
    y = c_int()
    w = c_uint()
    h = c_uint()
    bw = c_uint()
    depth = c_uint()
    if not x11.XGetGeometry(
        dpy, win, byref(root), byref(x), byref(y), byref(w), byref(h), byref(bw), byref(depth)
    ):
        return 0, 0
    return int(w.value), int(h.value)


def grab_bgr(dpy, win: int, w: int, h: int, redirected: bool) -> bytes | None:
    drawable = win
    pix = 0
    if redirected:
        pix = xcomp.XCompositeNameWindowPixmap(dpy, win)
        if pix:
            drawable = pix
    img_p = x11.XGetImage(dpy, drawable, 0, 0, w, h, AllPlanes, ZPixmap)
    if pix:
        x11.XFreePixmap(dpy, pix)
    if not img_p:
        return None
    img = img_p.contents
    if img.bits_per_pixel != 32 or not img.data:
        x11.XDestroyImage(img_p)
        return None
    raw = ctypes.string_at(img.data, img.bytes_per_line * h)
    stride = img.bytes_per_line
    x11.XDestroyImage(img_p)
    if stride == w * 4:
        return raw
    packed = bytearray(w * h * 4)
    row = w * 4
    for y in range(h):
        packed[y * row : (y + 1) * row] = raw[y * stride : y * stride + row]
    return bytes(packed)


def main() -> None:
    atexit.register(cleanup)
    x11.XSetErrorHandler(_KEEP_HANDLER)

    ffmpeg = os.environ.get("FF") or str(Path.home() / ".local/bin/ffmpeg")
    if not os.access(ffmpeg, os.X_OK):
        ffmpeg = shutil.which("ffmpeg") or ""
    if not ffmpeg:
        die("ffmpeg not found")
    chrome = (
        shutil.which("chromium")
        or shutil.which("chromium-browser")
        or shutil.which("google-chrome")
    )
    if not chrome:
        die("chromium not found")

    (ROOT / "docs").mkdir(parents=True, exist_ok=True)
    DEMO.mkdir(parents=True, exist_ok=True)
    subprocess.check_call(
        [
            "cargo",
            "build",
            "-q",
            "--manifest-path",
            str(ROOT / "Cargo.toml"),
            "--features",
            "demo-web",
            "--bin",
            "connect-control-plane",
            "--bin",
            "demo-web",
        ]
    )

    for port in (3055, 14433, 14434):
        kill_port(port)
    time.sleep(0.5)

    demo_web = subprocess.Popen([str(BIN / "demo-web"), "--bind", "127.0.0.1:3055"])
    CHILDREN.append(demo_web)
    wait_http("http://127.0.0.1:3055/health")

    chrome_dir = DEMO / "chrome"
    shutil.rmtree(chrome_dir, ignore_errors=True)
    chrome_dir.mkdir(parents=True)
    chrome_p = subprocess.Popen(
        [
            chrome,
            f"--user-data-dir={chrome_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-sync",
            "--disable-extensions",
            "--disable-infobars",
            "--window-size=1280,720",
            "--window-position=400,80",
            "--app=http://127.0.0.1:3055/?rec=1",
        ]
    )
    CHILDREN.append(chrome_p)

    dpy = x11.XOpenDisplay(None)
    if not dpy:
        die("cannot open X display")

    wid = 0
    for _ in range(40):
        wid = find_window(dpy, "ccp-demo")
        if wid:
            break
        time.sleep(0.25)
    if not wid:
        die("chromium window not found")

    ev = c_int()
    er = c_int()
    redirected = bool(xcomp.XCompositeQueryExtension(dpy, byref(ev), byref(er)))
    if redirected:
        xcomp.XCompositeRedirectWindow(dpy, wid, CompositeRedirectAutomatic)
        x11.XSync(dpy, 0)

    root = x11.XDefaultRootWindow(dpy)
    hidden = False
    evb = c_int()
    erb = c_int()
    if xfixes.XFixesQueryExtension(dpy, byref(evb), byref(erb)):
        xfixes.XFixesHideCursor(dpy, root)
        hidden = True
    x11.XWarpPointer(dpy, 0, root, 0, 0, 0, 0, 0, 0)
    x11.XFlush(dpy)

    w, h = geometry(dpy, wid)
    if w < 2 or h < 2:
        die("bad window size")
    w -= w % 2
    h -= h % 2

    ff = subprocess.Popen(
        [
            ffmpeg,
            "-y",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "bgr0",
            "-s",
            f"{w}x{h}",
            "-r",
            str(FPS),
            "-i",
            "-",
            "-t",
            str(int(DURATION)),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-crf",
            "18",
            "-preset",
            "fast",
            str(OUT),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    CHILDREN.append(ff)
    assert ff.stdin is not None

    nframes = int(DURATION * FPS)
    t0 = time.monotonic()
    played = False
    last = None
    try:
        for i in range(nframes):
            now = time.monotonic()
            if not played and now - t0 >= PLAY_AT:
                try:
                    urllib.request.urlopen(
                        urllib.request.Request(
                            "http://127.0.0.1:3055/play", method="POST"
                        ),
                        timeout=1,
                    ).read()
                except OSError:
                    pass
                played = True
            x11.XWarpPointer(dpy, 0, root, 0, 0, 0, 0, 0, 0)
            frame = grab_bgr(dpy, wid, w, h, redirected)
            if frame is None:
                if last is None:
                    die("window capture failed")
                frame = last
            last = frame
            ff.stdin.write(frame)
            target = t0 + (i + 1) / FPS
            slack = target - time.monotonic()
            if slack > 0:
                time.sleep(slack)
    finally:
        try:
            ff.stdin.close()
        except OSError:
            pass
        if hidden:
            xfixes.XFixesShowCursor(dpy, root)
            x11.XFlush(dpy)
        x11.XCloseDisplay(dpy)

    rc = ff.wait()
    if rc != 0:
        err = ff.stderr.read().decode("utf-8", "replace") if ff.stderr else ""
        die(f"ffmpeg failed ({rc})\n{err[-2000:]}")
    print(OUT, OUT.stat().st_size)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
