#!/usr/bin/env python3
"""
Simple WebSocket Signaling Server for WebRTC Demo

This server handles:
- Room management
- SDP exchange
- ICE candidate exchange
- Peer connection coordination
- Message caching for late joiners

Usage:
    python3 signaling_server.py [--port 8080]
"""

import argparse
import asyncio
import json
from collections import defaultdict
from typing import Dict, Set, List

import websockets

# Room state: room_name -> set of connected clients
rooms: Dict[str, Set[websockets.WebSocketServerProtocol]] = defaultdict(set)

# Message cache: room_name -> list of messages to forward to new joiners
message_cache: Dict[str, List[str]] = defaultdict(list)


async def clear_cache(room: str) -> None:
    """Drop stale cached SDP/candidates for a room when a client leaves."""
    if room in message_cache:
        n = len(message_cache[room])
        del message_cache[room]
        print(f"[Signaling] Cleared {n} cached messages for room '{room}'")


async def notify_peer_left(room: str, excluded: websockets.WebSocketServerProtocol) -> None:
    """Broadcast peer_left to the peers still in the room.

    对端掉线/离开时必须通知剩余对端，否则它们会带着已失效的 PeerConnection
    状态（pc 非空、offerCreated 已置位）卡住：重新 join 的对端收到 peer_joined
    后因 `!pc && !offerCreated` 为假而永远不再 createOffer，房间无法重连。
    """
    for peer in rooms.get(room, set()):
        if peer != excluded:
            try:
                await peer.send(json.dumps({'type': 'peer_left'}))
            except Exception:
                pass


async def handle_client(websocket: websockets.WebSocketServerProtocol):
    """Handle a single WebSocket client connection."""
    current_room = None

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                # Non-JSON message (likely SDP or ICE)
                if current_room:
                    peers = len(rooms[current_room])
                    print(f"[Signaling] Forwarding raw message to room '{current_room}' ({peers} peers)")
                    for peer in rooms[current_room]:
                        if peer != websocket:
                            try:
                                await peer.send(message)
                            except:
                                pass
                continue

            msg_type = data.get('type', '')

            if msg_type == 'join':
                room = data.get('room', 'default')
                current_room = room
                rooms[room].add(websocket)
                print(f"[Signaling] Client joined room '{room}' ({len(rooms[room])} peers now)")

                # Notify the joiner immediately if peers already exist, so it can
                # create its offer without waiting for the 30s timeout.
                if len(rooms[room]) > 1:
                    await websocket.send(json.dumps({'type': 'peer_joined'}))

                # Send cached messages to the new joiner (offer, candidates, etc.)
                if room in message_cache and len(message_cache[room]) > 0:
                    print(f"[Signaling] Sending {len(message_cache[room])} cached messages to new joiner")
                    for cached_msg in message_cache[room]:
                        try:
                            await websocket.send(cached_msg)
                        except:
                            pass

                # Notify other peers
                for peer in rooms[room]:
                    if peer != websocket:
                        try:
                            await peer.send(json.dumps({'type': 'peer_joined'}))
                        except:
                            pass

            elif msg_type == 'leave':
                if current_room and websocket in rooms[current_room]:
                    rooms[current_room].remove(websocket)
                    print(f"[Signaling] Client left room '{current_room}' ({len(rooms[current_room])} peers now)")

                    # Notify remaining peers that this client left (so they reset PC state)
                    await notify_peer_left(current_room, websocket)

                    # Clean up empty rooms
                    if not rooms[current_room]:
                        del rooms[current_room]

                    # Drop stale cached SDP/candidates to avoid cross-session pollution
                    await clear_cache(current_room)

                    current_room = None

            elif msg_type == 'ping':
                await websocket.send(json.dumps({'type': 'pong'}))

            else:
                # Forward all other JSON messages (offer, answer, candidate)
                if current_room:
                    peers = len(rooms[current_room])
                    print(f"[Signaling] Forwarding message type '{msg_type}' to room '{current_room}' ({peers} peers)")

                    # Cache offer, answer, and candidate messages for late joiners
                    if msg_type in ['offer', 'answer', 'candidate']:
                        if len(message_cache[current_room]) < 20:  # Limit cache size
                            message_cache[current_room].append(message)

                    for peer in rooms[current_room]:
                        if peer != websocket:
                            try:
                                await peer.send(message)
                            except:
                                pass

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Clean up on disconnect
        if current_room and websocket in rooms[current_room]:
            rooms[current_room].remove(websocket)
            print(f"[Signaling] Client disconnected from room '{current_room}' ({len(rooms[current_room])} peers now)")

            # Notify remaining peers so they reset PC state and can re-offer
            await notify_peer_left(current_room, websocket)

            if not rooms[current_room]:
                del rooms[current_room]
            await clear_cache(current_room)


async def main(port: int, tls_port: int | None, certfile: str | None, keyfile: str | None):
    """Start the signaling server.

    Listens on `port` (plain ws) and, if a cert is provided, also on `tls_port`
    (wss).  Both share the same rooms/message_cache, so a device on ws:// and a
    browser on wss:// can join the same room.  This lets an HTTPS page access
    getUserMedia while device↔device keeps using plain ws.
    """
    ssl_ctx = None
    if tls_port is not None and certfile and keyfile:
        import ssl
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.load_cert_chain(certfile, keyfile)
        print(f"[Signaling] TLS on wss://0.0.0.0:{tls_port} ({certfile})")

    print(f"[Signaling] Starting server on ws://0.0.0.0:{port}")
    if ssl_ctx:
        async with websockets.serve(handle_client, "0.0.0.0", port, ssl=None), \
                   websockets.serve(handle_client, "0.0.0.0", tls_port, ssl=ssl_ctx):
            await asyncio.Future()  # Run forever
    else:
        async with websockets.serve(handle_client, "0.0.0.0", port):
            await asyncio.Future()  # Run forever


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebRTC Signaling Server")
    parser.add_argument("--port", type=int, default=8080, help="Server port (plain ws)")
    parser.add_argument("--tls-port", type=int, default=None, help="Server port for wss (TLS)")
    parser.add_argument("--cert", type=str, default=None, help="TLS certificate PEM")
    parser.add_argument("--key", type=str, default=None, help="TLS key PEM")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.port, args.tls_port, args.cert, args.key))
    except KeyboardInterrupt:
        print("\n[Signaling] Server stopped")
