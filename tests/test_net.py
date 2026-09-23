"""TCP/UDP/streams/sock ops/add_reader/add_writer over the Tokio reactor."""

import asyncio
import socket

import pytest

import tokioop


@pytest.fixture(autouse=True)
def _install():
    tokioop.install()


def test_tcp_echo_streams():
    async def main():
        async def handle(reader, writer):
            data = await reader.read(100)
            writer.write(data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"hello tokioop")
        await writer.drain()
        data = await reader.read(100)
        assert data == b"hello tokioop"
        writer.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_tcp_large_transfer():
    payload = bytes((i % 251 for i in range(2_000_000)))

    async def main():
        async def handle(reader, writer):
            received = bytearray()
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                received.extend(chunk)
            assert bytes(received) == payload
            writer.write(b"done")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(payload)
        await writer.drain()
        writer.write_eof()
        assert await reader.read(100) == b"done"
        writer.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_tcp_many_connections():
    async def main():
        async def handle(reader, writer):
            data = await reader.read(64)
            writer.write(data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]

        async def client(i):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            msg = f"msg-{i:04d}".encode()
            writer.write(msg)
            await writer.drain()
            assert await reader.readexactly(len(msg)) == msg
            writer.close()

        await asyncio.gather(*[client(i) for i in range(50)])
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_create_connection_protocol():
    async def main():
        received = []
        connected = asyncio.Event()

        class ClientProto(asyncio.Protocol):
            def connection_made(self, transport):
                transport.write(b"ping")

            def data_received(self, data):
                received.append(data)
                connected.set()

            def connection_lost(self, exc):
                pass

        async def handle(reader, writer):
            writer.write(b"pong")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        loop = asyncio.get_running_loop()
        transport, proto = await loop.create_connection(ClientProto, "127.0.0.1", port)
        await asyncio.wait_for(connected.wait(), timeout=2.0)
        assert b"".join(received) == b"pong"
        transport.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_udp_echo():
    async def main():
        received = asyncio.Queue()

        class ServerProto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.put_nowait((data, addr))

        class ClientProto(asyncio.DatagramProtocol):
            def __init__(self):
                self.got = asyncio.Queue()

            def datagram_received(self, data, addr):
                self.got.put_nowait(data)

        loop = asyncio.get_running_loop()
        t_server, _ = await loop.create_datagram_endpoint(
            ServerProto, local_addr=("127.0.0.1", 0)
        )
        sport = t_server.get_extra_info("socket").getsockname()[1]
        t_client, cproto = await loop.create_datagram_endpoint(
            ClientProto, remote_addr=("127.0.0.1", sport)
        )
        t_client.sendto(b"hello udp")
        data, addr = await asyncio.wait_for(received.get(), timeout=2.0)
        assert data == b"hello udp"

        # reply path
        t_server.sendto(b"ack", addr)
        assert await asyncio.wait_for(cproto.got.get(), timeout=2.0) == b"ack"

        t_client.close()
        t_server.close()
        await asyncio.sleep(0.05)

    asyncio.run(main())


def test_sock_ops():
    async def main():
        loop = asyncio.get_running_loop()
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setblocking(False)
        lsock.bind(("127.0.0.1", 0))
        lsock.listen(5)
        port = lsock.getsockname()[1]

        async def client():
            csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            csock.setblocking(False)
            await loop.sock_connect(csock, ("127.0.0.1", port))
            await loop.sock_sendall(csock, b"sockdata")
            assert await loop.sock_recv(csock, 100) == b"reply"
            csock.close()

        t = asyncio.create_task(client())
        conn, addr = await loop.sock_accept(lsock)
        data = await loop.sock_recv(conn, 100)
        assert data == b"sockdata"
        await loop.sock_sendall(conn, b"reply")
        await t
        conn.close()
        lsock.close()

    asyncio.run(main())


def test_add_reader_writer_pipe():
    async def main():
        loop = asyncio.get_running_loop()
        r, w = socket.socketpair()
        r.setblocking(False)
        w.setblocking(False)
        got = asyncio.Future()

        def on_read():
            got.set_result(r.recv(100))

        loop.add_reader(r.fileno(), on_read)
        w.send(b"pipedata")
        assert await asyncio.wait_for(got, timeout=2.0) == b"pipedata"
        assert loop.remove_reader(r.fileno()) is True
        assert loop.remove_reader(r.fileno()) is False

        # writer side
        w2r, w2w = socket.socketpair()
        w2r.setblocking(False)
        w2w.setblocking(False)
        fired = []

        def on_write():
            fired.append(True)
            loop.remove_writer(w2w.fileno())

        loop.add_writer(w2w.fileno(), on_write)
        await asyncio.sleep(0.05)
        assert fired == [True]
        r.close()
        w.close()
        w2r.close()
        w2w.close()

    asyncio.run(main())


def test_reader_cancel_and_replace():
    async def main():
        loop = asyncio.get_running_loop()
        r, w = socket.socketpair()
        r.setblocking(False)
        w.setblocking(False)
        calls = []

        # private API returns a handle with cancel()
        h1 = loop._add_reader(r.fileno(), lambda: calls.append("h1"))
        assert not h1.cancelled()
        assert h1.cancel() is True

        def h2_cb():
            # consume so the level-triggered watcher goes quiet
            try:
                r.recv(100)
            except BlockingIOError:
                pass
            calls.append("h2")

        h2 = loop._add_reader(r.fileno(), h2_cb)
        assert h1.cancelled()  # replaced handles are cancelled
        assert not h2.cancelled()
        w.send(b"x")
        await asyncio.sleep(0.05)
        assert calls == ["h2"]
        loop._remove_reader(r.fileno())
        r.close()
        w.close()

    asyncio.run(main())


def test_level_triggered_refires():
    """A reader must fire again while data remains (level-triggered)."""
    async def main():
        loop = asyncio.get_running_loop()
        r, w = socket.socketpair()
        r.setblocking(False)
        w.setblocking(False)
        chunks = []

        def on_read():
            try:
                chunks.append(r.recv(4))  # small reads, data remains
            except BlockingIOError:
                pass

        # 64 bytes, 4 bytes per firing -> needs 16 refires with no new edges
        w.send(b"y" * 64)
        loop.add_reader(r.fileno(), on_read)
        await asyncio.sleep(0.2)
        loop.remove_reader(r.fileno())
        assert b"".join(chunks) == b"y" * 64, chunks
        r.close()
        w.close()

    asyncio.run(main())


def test_pause_resume_with_pending_data():
    """resume_reading with data already pending must not stall (edge race)."""
    async def main():
        received = []

        class Proto(asyncio.Protocol):
            def connection_made(self, transport):
                self.t = transport

            def data_received(self, data):
                received.append(data)

            def connection_lost(self, exc):
                pass

        async def handle(reader, writer):
            writer.write(b"0123456789")
            await writer.drain()
            await asyncio.sleep(0.3)
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_connection(Proto, "127.0.0.1", port)
        await asyncio.sleep(0.05)
        transport.pause_reading()
        await asyncio.sleep(0.1)  # data arrives while paused
        transport.resume_reading()  # must fire despite consumed edge
        await asyncio.sleep(0.1)
        assert b"".join(received) == b"0123456789", received
        transport.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_run_in_executor():
    import concurrent.futures as cf

    async def main():
        loop = asyncio.get_running_loop()

        def blocking(x, y):
            return x * y

        assert await loop.run_in_executor(None, blocking, 6, 7) == 42
        ex = cf.ThreadPoolExecutor(max_workers=1)
        try:
            assert await loop.run_in_executor(ex, blocking, 3, 4) == 12
        finally:
            ex.shutdown(wait=True)

    asyncio.run(main())


def test_getaddrinfo():
    async def main():
        loop = asyncio.get_running_loop()
        infos = await loop.getaddrinfo("127.0.0.1", 80, family=socket.AF_INET)
        assert infos
        assert infos[0][4][0] == "127.0.0.1"

    asyncio.run(main())


def test_unix_domain_sockets(tmp_path):
    async def main():
        path = str(tmp_path / "tokioop.sock")

        async def handle(reader, writer):
            data = await reader.read(100)
            writer.write(data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_unix_server(handle, path)
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write(b"unix hello")
        await writer.drain()
        assert await reader.read(100) == b"unix hello"
        writer.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_tls_echo(tmp_path):
    import ssl

    key = tmp_path / "key.pem"
    cert = tmp_path / "cert.pem"
    import subprocess

    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key), "-out", str(cert),
            "-days", "1", "-nodes", "-subj", "/CN=localhost",
        ],
        check=True,
        capture_output=True,
    )

    async def main():
        srv_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        srv_ctx.load_cert_chain(str(cert), str(key))
        cli_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        cli_ctx.check_hostname = False
        cli_ctx.verify_mode = ssl.CERT_NONE

        async def handle(reader, writer):
            data = await reader.read(100)
            writer.write(b"tls:" + data)
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=srv_ctx)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=cli_ctx
        )
        writer.write(b"hello")
        await writer.drain()
        assert await reader.read(100) == b"tls:hello"
        writer.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_unsupported_apis_fail_like_asyncio():
    """Subprocesses/pipes are unsupported and must raise NotImplementedError
    (asyncio's own failure mode for missing transports), never silently
    misbehave."""

    async def main():
        loop = asyncio.get_running_loop()
        with pytest.raises(NotImplementedError):
            await loop.subprocess_exec(asyncio.SubprocessProtocol, "true")
        with pytest.raises(NotImplementedError):
            await loop.subprocess_shell(asyncio.SubprocessProtocol, "true")
        with pytest.raises(NotImplementedError):
            await loop.connect_read_pipe(asyncio.Protocol, None)
        with pytest.raises(NotImplementedError):
            await loop.connect_write_pipe(asyncio.Protocol, None)

    asyncio.run(main())


def test_tcp_pause_replay_no_loss():
    """Pausing mid-burst must not lose Rust-consumed data (replay buffer)."""

    async def main():
        loop = asyncio.get_running_loop()
        received = []
        N = 21

        class Proto(asyncio.Protocol):
            def connection_made(self, transport):
                self.t = transport

            def data_received(self, data):
                received.append(bytes(data))
                if len(received) == 1:
                    self.t.pause_reading()

            def connection_lost(self, exc):
                pass

        async def handle(reader, writer):
            writer.write(b"A" * 1024)  # first chunk: triggers pause
            await writer.drain()
            await asyncio.sleep(0.05)
            for i in range(1, N):
                writer.write(bytes((i % 250 + 1,)) * 1024)  # distinct markers
                await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        transport, proto = await loop.create_connection(Proto, "127.0.0.1", port)
        await asyncio.sleep(0.15)  # let the burst arrive while paused
        assert transport.is_reading() is False
        transport.resume_reading()
        await asyncio.sleep(0.15)
        total = b"".join(received)
        assert len(total) == N * 1024, len(total)
        assert total[:1024] == b"A" * 1024
        for i in range(1, N):
            assert total[i * 1024 : (i + 1) * 1024] == bytes((i % 250 + 1,)) * 1024
        transport.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())


def test_udp_pause_replay_no_loss():
    async def main():
        loop = asyncio.get_running_loop()
        received = []
        N = 20

        class ServerProto(asyncio.DatagramProtocol):
            def datagram_received(self, data, addr):
                received.append(data)
                if len(received) == 1:
                    transport.pause_reading()

        class ClientProto(asyncio.DatagramProtocol):
            pass

        t_server, _ = await loop.create_datagram_endpoint(
            ServerProto, local_addr=("127.0.0.1", 0)
        )
        transport = t_server
        sport = t_server.get_extra_info("socket").getsockname()[1]
        t_client, _ = await loop.create_datagram_endpoint(
            ClientProto, remote_addr=("127.0.0.1", sport)
        )
        t_client.sendto(b"first")
        await asyncio.sleep(0.05)
        for i in range(1, N):
            t_client.sendto(bytes((i,)) * 32)
        await asyncio.sleep(0.1)
        transport.resume_reading()
        await asyncio.sleep(0.1)
        assert len(received) == N, len(received)
        assert received[0] == b"first"
        for i in range(1, N):
            assert received[i] == bytes((i,)) * 32
        t_client.close()
        t_server.close()
        await asyncio.sleep(0.05)

    asyncio.run(main())


def test_buffered_protocol_fallback():
    """BufferedProtocol uses the callback-mode read path."""

    async def main():
        received = bytearray()
        done = asyncio.Event()

        class BufProto(asyncio.BufferedProtocol):
            def connection_made(self, transport):
                self.transport = transport

            def get_buffer(self, sizehint):
                self._buf = bytearray(65536)
                return self._buf

            def buffer_updated(self, nbytes):
                received.extend(self._buf[:nbytes])
                if len(received) >= 10000:
                    done.set()

            def eof_received(self):
                return False

            def connection_lost(self, exc):
                pass

        async def handle(reader, writer):
            writer.write(b"B" * 10000)
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_connection(BufProto, "127.0.0.1", port)
        await asyncio.wait_for(done.wait(), timeout=2.0)
        assert bytes(received) == b"B" * 10000
        transport.close()
        server.close()
        await server.wait_closed()

    asyncio.run(main())
