// Compile-time checks: both the standard WebSocket and the `ws` package fit.
import { WebSocket as WsWebSocket } from 'ws';
import { WebSocketTransport, type WebSocketConstructor, type WebSocketLike } from '../../src/transport/websocket.ts';

export const standardCtor: WebSocketConstructor = WebSocket;
export const standardSocket = (socket: WebSocket): WebSocketLike => socket;
export const wsCtor: WebSocketConstructor<WsWebSocket.ClientOptions> = WsWebSocket;
export const wsSocket = (socket: WsWebSocket): WebSocketLike => socket;

export const viaWs = () =>
  WebSocketTransport.connect('ws://localhost', { WebSocket: WsWebSocket, webSocketOptions: { headers: { a: 'b' } } });
export const viaGlobal = () => WebSocketTransport.connect('ws://localhost');
