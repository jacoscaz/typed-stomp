import { FrameParser, type FrameParserOptions, type ParserInput } from '../src/codec/parser.ts';
import type { Frame, StompVersion } from '../src/codec/frame.ts';

export interface Collected {
  readonly frames: Frame[];
  heartbeats: number;
}

export function collectingParser(
  options: Omit<FrameParserOptions, 'onFrame' | 'onHeartbeat'> = {},
): { parser: FrameParser; collected: Collected } {
  const collected: Collected = { frames: [], heartbeats: 0 };
  const parser = new FrameParser({
    ...options,
    onFrame: (frame) => collected.frames.push(frame),
    onHeartbeat: () => {
      collected.heartbeats += 1;
    },
  });
  return { parser, collected };
}

export function parseAll(
  chunks: ParserInput | readonly ParserInput[],
  options: Omit<FrameParserOptions, 'onFrame' | 'onHeartbeat'> & { version?: StompVersion } = {},
): Collected {
  const { parser, collected } = collectingParser(options);
  for (const chunk of Array.isArray(chunks) ? chunks : [chunks]) {
    parser.push(chunk);
  }
  return collected;
}

export const text = (body: Uint8Array): string => new TextDecoder().decode(body);

export const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);

/** Plain-object copy of headers, for deep equality against literals. */
export const plain = (frame: Frame): Record<string, string | undefined> => ({ ...frame.headers });

/** Builds a raw frame for validator tests. */
export const frame = (command: string, headers: Record<string, string> = {}, body = ''): Frame => ({
  command,
  headers,
  body: bytes(body),
});

/** Small deterministic PRNG (mulberry32) for randomized tests. */
export function prng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Waits for pending microtasks and I/O callbacks to run. */
export const flush = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

/** Records everything sent through a transport, without altering behaviour. */
export function recordSends<T extends { send(data: string | Uint8Array): void }>(transport: T): (string | Uint8Array)[] {
  const sent: (string | Uint8Array)[] = [];
  const send = transport.send.bind(transport);
  transport.send = (data) => {
    sent.push(data);
    send(data);
  };
  return sent;
}
