"""A NATS-transparent TCP proxy for the two network axes no test had exercised: LATENCY and
DISCONNECTS (``docs/plans/test-axes.md``, increment 3). The twin of ``test/harness/proxy.ts``.

It forwards bytes and knows nothing about the protocol, so the client library and the server see a
real socket with real semantics:

- ``rtt <ms>``   each direction is held for rtt/2 in an ORDERED queue (a release time never precedes
  the one before it, so lowering the delay mid-flight cannot reorder a stream);
- ``cut``        resets every connection NOW, dropping whatever is queued;
- ``down 1|0``   while down a new connection is accepted and dropped at once — an outage that
  outlasts the client's first reconnect attempts;
- ``stall none|both|down``   bytes are held, sockets stay open — the half-open link a WAN actually
  gives you. ``down`` stalls server→client only: requests LAND and their replies never arrive.

It runs as a SUBPROCESS (``ProxyProcess``), never inside the test's event loop: a proxy sharing a
loop with the code under test is not an independent link — every delay it adds would be scheduled by
the very loop whose behaviour is being measured (LEARNINGS #47 is the same lesson from the other
side). Commands go down stdin one line at a time and each is acknowledged on stdout, so a test knows
the change took effect before its next line runs.

A single nats-server sends no ``connect_urls``, so a client cannot learn the real port from INFO and
reconnect around the proxy. A clustered harness must add ``no_advertise: true``.
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from urllib.parse import urlparse

# --- the proxy process -------------------------------------------------------------------------


class _State:
    def __init__(self, rtt_ms: float) -> None:
        self.rtt_ms = rtt_ms
        self.down = False
        self.stall = "none"
        self.accepted = 0
        self.dropped = 0
        self.pairs: set[_Pair] = set()


class _Pipe:
    """One direction of one connection: an ordered, delayed byte queue."""

    def __init__(self, state: _State, direction: str, writer: asyncio.StreamWriter) -> None:
        self._state = state
        self._direction = direction  # "up" (client→server) or "down" (server→client)
        self._writer = writer
        self._queue: deque[tuple[float, bytes | None]] = deque()
        self._last_at = 0.0
        self._wake = asyncio.Event()
        self.dead = False

    def _stalled(self) -> bool:
        stall = self._state.stall
        return stall == "both" or (stall == "down" and self._direction == "down")

    def push(self, data: bytes | None) -> None:
        at = max(self._last_at, time.monotonic() + self._state.rtt_ms / 2000)
        self._last_at = at
        self._queue.append((at, data))
        self._wake.set()

    def poke(self) -> None:
        self._wake.set()

    async def run(self) -> None:
        while not self.dead:
            if not self._queue or self._stalled():
                self._wake.clear()
                await self._wake.wait()
                continue
            at, data = self._queue[0]
            wait = at - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
                continue  # re-check: a stall or a kill may have arrived meanwhile
            self._queue.popleft()
            if data is None:
                # The source ended: end the destination only now that the queue has drained, so
                # bytes written just before a close are DELIVERED first, as on a real link.
                with contextlib.suppress(Exception):
                    self._writer.write_eof()
                return
            self._writer.write(data)
            with contextlib.suppress(Exception):
                await self._writer.drain()

    def kill(self) -> None:
        self.dead = True
        self._queue.clear()
        self._wake.set()


class _Pair:
    def __init__(
        self,
        state: _State,
        client: tuple[asyncio.StreamReader, asyncio.StreamWriter],
        upstream: tuple[asyncio.StreamReader, asyncio.StreamWriter],
    ) -> None:
        self.state = state
        self.client_r, self.client_w = client
        self.up_r, self.up_w = upstream
        self.up = _Pipe(state, "up", self.up_w)
        self.down = _Pipe(state, "down", self.client_w)
        self.tasks: list[asyncio.Task[None]] = []

    async def _read(self, reader: asyncio.StreamReader, pipe: _Pipe) -> None:
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    pipe.push(None)
                    return
                pipe.push(data)
        except OSError:
            self.destroy()  # a reset on either side takes the pair down, like the real thing

    async def serve(self) -> None:
        self.tasks = [
            asyncio.ensure_future(self._read(self.client_r, self.up)),
            asyncio.ensure_future(self._read(self.up_r, self.down)),
            asyncio.ensure_future(self.up.run()),
            asyncio.ensure_future(self.down.run()),
        ]
        # Both directions ended (each pipe returns after forwarding its EOF), or destroy().
        await asyncio.gather(*self.tasks[2:], return_exceptions=True)
        self.destroy(graceful=True)

    def destroy(self, graceful: bool = False) -> None:
        """Take the pair down. A cut ABORTS (a reset; buffered bytes die with it); a connection
        that ended on its own CLOSES, which flushes what was already forwarded."""
        if self not in self.state.pairs:
            return
        self.state.pairs.discard(self)
        self.up.kill()
        self.down.kill()
        for w in (self.client_w, self.up_w):
            with contextlib.suppress(Exception):
                if graceful:
                    w.close()
                else:
                    w.transport.abort()
        for t in self.tasks[:2]:
            t.cancel()


async def _serve(target_host: str, target_port: int, rtt_ms: float) -> None:
    state = _State(rtt_ms)

    async def on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        state.accepted += 1
        if state.down:
            state.dropped += 1
            writer.transport.abort()
            return
        try:
            upstream = await asyncio.open_connection(target_host, target_port)
        except OSError:
            writer.transport.abort()  # the server is away (a restart): the client sees a reset
            return
        pair = _Pair(state, (reader, writer), upstream)
        state.pairs.add(pair)
        await pair.serve()

    server = await asyncio.start_server(on_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    print(f"PORT {port}", flush=True)

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:  # the parent went away: so do we
            return
        cmd = line.split()
        if not cmd:
            continue
        if cmd[0] == "rtt":
            state.rtt_ms = float(cmd[1])
        elif cmd[0] == "down":
            state.down = cmd[1] == "1"
        elif cmd[0] == "stall":
            state.stall = cmd[1]
            for p in list(state.pairs):
                p.up.poke()
                p.down.poke()
        elif cmd[0] == "cut":
            open_now = list(state.pairs)
            for p in open_now:
                p.destroy()
            state.dropped += len(open_now)
            print(f"ok {len(open_now)}", flush=True)
            continue
        elif cmd[0] == "stats":
            print(f"ok {state.accepted} {len(state.pairs)} {state.dropped}", flush=True)
            continue
        print("ok", flush=True)


# --- the handle a test holds -------------------------------------------------------------------


class ProxyProcess:
    """The proxy as a subprocess. Commands are synchronous and acknowledged: each returns once the
    proxy has applied it (well under a millisecond — it never waits on the network)."""

    def __init__(self, target_url: str, rtt_ms: float = 0) -> None:
        u = urlparse(target_url)
        self._proc: subprocess.Popen[str] = subprocess.Popen(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                str(u.hostname),
                str(u.port),
                str(rtt_ms),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        assert self._proc.stdout is not None
        first = self._proc.stdout.readline().split()
        if len(first) != 2 or first[0] != "PORT":
            self.stop()
            raise RuntimeError(f"netproxy did not start: {first!r}")
        self.url = f"nats://127.0.0.1:{first[1]}"

    def _cmd(self, line: str) -> list[str]:
        assert self._proc.stdin is not None
        assert self._proc.stdout is not None
        self._proc.stdin.write(line + "\n")
        self._proc.stdin.flush()
        reply: list[str] = self._proc.stdout.readline().split()
        if not reply or reply[0] != "ok":
            raise RuntimeError(f"netproxy: {line!r} → {reply!r}")
        return reply[1:]

    def set_rtt(self, ms: float) -> None:
        """Round-trip time added by the proxy (half per direction)."""
        self._cmd(f"rtt {ms}")

    def set_down(self, down: bool) -> None:
        """While down, a new connection is dropped at once (an outage, not a blip)."""
        self._cmd(f"down {1 if down else 0}")

    def set_stall(self, mode: str) -> None:
        """``none`` | ``both`` | ``down`` — hold bytes without closing anything."""
        if mode not in ("none", "both", "down"):
            raise ValueError(mode)
        self._cmd(f"stall {mode}")

    def cut(self) -> int:
        """Reset every connection NOW; queued bytes are lost. Returns how many were open."""
        return int(self._cmd("cut")[0])

    def stats(self) -> tuple[int, int, int]:
        """``(accepted, open, dropped)``."""
        a, o, d = self._cmd("stats")
        return int(a), int(o), int(d)

    def stop(self) -> None:
        with contextlib.suppress(Exception):
            if self._proc.stdin is not None:
                self._proc.stdin.close()  # EOF on stdin is the proxy's cue to exit
        with contextlib.suppress(subprocess.TimeoutExpired):
            self._proc.wait(timeout=2)
        if self._proc.poll() is None:
            self._proc.kill()
            self._proc.wait()


if __name__ == "__main__":
    asyncio.run(_serve(sys.argv[1], int(sys.argv[2]), float(sys.argv[3])))
