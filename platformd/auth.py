from __future__ import annotations

import ctypes
import platform
import socket
import struct


def peer_uid(sock: socket.socket) -> int:
    """
    Return the UID of the peer connected to a Unix domain socket.

    Linux: SO_PEERCRED returns a struct {pid, uid, gid}.
    macOS (Darwin): libc's getpeereid(fd, &uid, &gid).
    """
    system = platform.system()
    if system == "Linux":
        data = sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i")
        )
        _pid, uid, _gid = struct.unpack("3i", data)
        return uid
    if system == "Darwin":
        libc = ctypes.CDLL("libc.dylib", use_errno=True)
        uid = ctypes.c_uint32()
        gid = ctypes.c_uint32()
        rc = libc.getpeereid(
            sock.fileno(), ctypes.byref(uid), ctypes.byref(gid)
        )
        if rc != 0:
            err = ctypes.get_errno()
            raise OSError(err, f"getpeereid failed: errno={err}")
        return int(uid.value)
    raise RuntimeError(f"peer_uid: unsupported platform {system}")
