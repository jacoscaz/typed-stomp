import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import type { AddressInfo } from 'node:net';
import { Readable } from 'node:stream';
import { after, before, describe, it } from 'node:test';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';
import { run, type CliIO } from '../../src/cli/cli.ts';
import { WebSocketBroker } from '../../src/server/websocket.ts';
async function until(condition: () => boolean, timeout = 2000): Promise<void> {
  const deadline = Date.now() + timeout;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('Timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

class Output {
  text = '';
  write(chunk: string): void {
    this.text += chunk;
  }
}

function io(extra: Omit<Partial<CliIO>, 'stdout' | 'stderr'> = {}): CliIO & { stdout: Output; stderr: Output } {
  return { env: {}, ...extra, stdout: new Output(), stderr: new Output() };
}

describe('CLI', () => {
  let broker: WebSocketBroker;
  let url: string;

  before(async () => {
    broker = await WebSocketBroker.listen({
      WebSocketServer,
      webSocketServerOptions: { port: 0, host: '127.0.0.1' },
      authenticate: ({ login, passcode }) => {
        if (login !== undefined && passcode !== 'secret') throw new Error('Bad credentials');
        return { user: login };
      },
    });
    url = `ws://127.0.0.1:${(broker.address() as AddressInfo).port}`;
  });

  after(async () => {
    await broker.close();
  });

  it('prints usage', async () => {
    const out = io();
    assert.equal(await run(['--help'], out), 0);
    assert.match(out.stdout.text, /Usage:/);
  });

  it('rejects bad usage with exit code 2', async () => {
    const cases = [['send'], ['frobnicate'], ['send', '/queue/a', 'x'], ['subscribe', '/a', '-u', url, '--ack', 'nope']];
    for (const argv of cases) {
      const out = io();
      assert.equal(await run(argv, out), 2, argv.join(' '));
      assert.match(out.stderr.text, /typed-stomp: /);
    }
  });

  it('sends messages that subscribers receive', async () => {
    const sub = io();
    const subscribing = run(['subscribe', '/queue/cli', '-u', url, '-n', '2', '--ack', 'client'], sub);
    const sent = io({ env: { STOMP_URL: url } });
    assert.equal(await run(['send', '/queue/cli', 'hello', '-r'], sent), 0, sent.stderr.text);
    const piped = io({ env: { STOMP_URL: url }, stdin: Readable.from(['from ', 'stdin']) });
    assert.equal(await run(['send', '/queue/cli', '-H', 'x-test: 1'], piped), 0, piped.stderr.text);
    assert.equal(await subscribing, 0, sub.stderr.text);
    assert.equal(sub.stdout.text, 'hello\nfrom stdin\n');
  });

  it('prints JSON with headers', async () => {
    const sub = io();
    const subscribing = run(['subscribe', '/queue/json', '-u', url, '-n', '1', '--json'], sub);
    assert.equal(await run(['send', '/queue/json', '{}', '-u', url, '-t', 'application/json', '-H', 'x-a:b'], io()), 0);
    assert.equal(await subscribing, 0, sub.stderr.text);
    const line = JSON.parse(sub.stdout.text) as { destination: string; headers: Record<string, string>; body: string };
    assert.equal(line.destination, '/queue/json');
    assert.equal(line.headers['x-a'], 'b');
    assert.equal(line.headers['content-type'], 'application/json');
    assert.equal(line.body, '{}');
  });

  it('stops subscribing when aborted', async () => {
    const controller = new AbortController();
    const sub = io({ signal: controller.signal });
    const subscribing = run(['subscribe', '/topic/forever', '-u', url], sub);
    await until(() => broker.server.sessions.length > 0);
    controller.abort();
    assert.equal(await subscribing, 130);
  });

  it('reports server errors with exit code 1', async () => {
    const out = io();
    assert.equal(await run(['send', '/queue/a', 'x', '-u', url, '-l', 'me', '-p', 'wrong'], out), 1);
    assert.match(out.stderr.text, /Bad credentials/);
  });

  it('runs as a standalone script using the global WebSocket', async () => {
    const main = fileURLToPath(new URL('../../src/cli/main.ts', import.meta.url));
    const child = spawn(process.execPath, [main, 'send', '/queue/spawned', '-u', url], { stdio: ['pipe', 'pipe', 'pipe'] });
    let stderr = '';
    child.stderr.on('data', (chunk) => (stderr += chunk));
    child.stdin.end('spawned body');
    const code = await new Promise((resolve) => child.on('exit', resolve));
    assert.equal(code, 0, stderr);
    const sub = io();
    assert.equal(await run(['subscribe', '/queue/spawned', '-u', url, '-n', '1'], sub), 0);
    assert.equal(sub.stdout.text, 'spawned body\n');
  });
});
