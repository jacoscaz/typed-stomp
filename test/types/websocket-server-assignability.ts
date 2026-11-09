// Compile-time checks: the ws WebSocketServer fits WebSocketServerConstructor.
import { WebSocketServer } from 'ws';
import { WebSocketBroker, type WebSocketServerConstructor, type WebSocketServerLike } from '../../src/server/websocket.ts';

export const ctor: WebSocketServerConstructor<import('ws').ServerOptions> = WebSocketServer;
export const like = (server: WebSocketServer): WebSocketServerLike => server;
export const start = () => WebSocketBroker.listen({ WebSocketServer, webSocketServerOptions: { port: 0 } });
