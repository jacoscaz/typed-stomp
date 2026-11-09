import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { StompParseError } from '../../src/codec/errors.ts';
import { FrameParser } from '../../src/codec/parser.ts';
import { bytes, collectingParser, parseAll, plain, text } from '../helpers.ts';

const isParseError = (code: string) => (err: unknown) => err instanceof StompParseError && err.code === code;

describe('FrameParser', () => {
  it('parses a simple frame from a string', () => {
    const { frames } = parseAll('SEND\ndestination:/queue/a\n\nhello\0');
    assert.equal(frames.length, 1);
    assert.equal(frames[0]?.command, 'SEND');
    assert.deepEqual(plain(frames[0]!), { destination: '/queue/a' });
    assert.equal(text(frames[0]!.body), 'hello');
  });

  it('accepts ArrayBuffer and typed array views', () => {
    const data = bytes('SEND\ndestination:/a\n\nx\0');
    const padded = new Uint8Array(data.byteLength + 4);
    padded.set(data, 2);
    const view = new DataView(padded.buffer, 2, data.byteLength);
    const { frames } = parseAll([data.slice().buffer, view]);
    assert.equal(frames.length, 2);
  });

  it('parses frames split byte by byte', () => {
    const data = bytes('MESSAGE\ndestination:/t\nmessage-id:1\nsubscription:0\n\nbody\0');
    const { frames } = parseAll(Array.from(data, (byte) => Uint8Array.of(byte)));
    assert.equal(frames.length, 1);
    assert.equal(text(frames[0]!.body), 'body');
    assert.equal(frames[0]!.headers['message-id'], '1');
  });

  it('parses several frames in one chunk', () => {
    const { frames } = parseAll('RECEIPT\nreceipt-id:1\n\n\0RECEIPT\nreceipt-id:2\n\n\0');
    assert.deepEqual(
      frames.map((f) => f.headers['receipt-id']),
      ['1', '2'],
    );
  });

  it('reports heart-beat EOLs between frames', () => {
    const { frames, heartbeats } = parseAll(['\n', '\r\n\n', 'RECEIPT\nreceipt-id:1\n\n\0\n\r', '\n']);
    assert.equal(frames.length, 1);
    assert.equal(heartbeats, 5);
  });

  it('handles CRLF line endings', () => {
    const { frames } = parseAll('SEND\r\ndestination:/a\r\nfoo:bar\r\n\r\nhi\0');
    assert.equal(frames[0]?.command, 'SEND');
    assert.deepEqual(plain(frames[0]!), { destination: '/a', foo: 'bar' });
  });

  it('rejects a CR that is not followed by LF between frames', () => {
    const { parser } = collectingParser();
    assert.throws(() => parser.push('\rX'), isParseError('invalid-eol'));
  });

  it('uses content-length to read bodies containing NUL', () => {
    const body = Uint8Array.of(1, 0, 2, 0, 3);
    const head = bytes('SEND\ndestination:/a\ncontent-length:5\n\n');
    const data = new Uint8Array(head.byteLength + body.byteLength + 1);
    data.set(head);
    data.set(body, head.byteLength);
    const { frames } = parseAll([data.subarray(0, head.byteLength + 2), data.subarray(head.byteLength + 2)]);
    assert.deepEqual(frames[0]?.body, body);
  });

  it('rejects a missing NUL after content-length bytes', () => {
    assert.throws(() => parseAll('SEND\ncontent-length:2\n\nabc\0'), isParseError('missing-null'));
  });

  it('rejects invalid content-length values', () => {
    for (const value of ['-1', 'abc', '1.5', ' 3', '99999999999999999999']) {
      assert.throws(() => parseAll(`SEND\ncontent-length:${value}\n\n\0`), isParseError('invalid-content-length'));
    }
  });

  it('keeps the first occurrence of repeated headers', () => {
    const { frames } = parseAll('MESSAGE\nfoo:1\nfoo:2\n\n\0');
    assert.equal(frames[0]?.headers['foo'], '1');
  });

  it('keeps colons in header values', () => {
    const { frames } = parseAll('MESSAGE\nurl:http://example.com:8080\n\n\0');
    assert.equal(frames[0]?.headers['url'], 'http://example.com:8080');
  });

  it('does not trim header names or values', () => {
    const { frames } = parseAll('MESSAGE\n foo : bar \n\n\0');
    assert.deepEqual(plain(frames[0]!), { ' foo ': ' bar ' });
  });

  it('is not vulnerable to prototype pollution through header names', () => {
    const { frames } = parseAll('MESSAGE\n__proto__:polluted\nconstructor:x\n\n\0');
    const headers = frames[0]!.headers;
    assert.equal(headers['__proto__'], 'polluted');
    assert.equal(Object.getPrototypeOf(headers), null);
    assert.equal(({} as Record<string, unknown>)['polluted'], undefined);
  });

  it('rejects header lines without a colon or with an empty name', () => {
    assert.throws(() => parseAll('SEND\nfoo\n\n\0'), isParseError('invalid-header'));
    assert.throws(() => parseAll('SEND\n:foo\n\n\0'), isParseError('invalid-header'));
  });

  it('rejects invalid UTF-8 in headers', () => {
    const data = new Uint8Array([...bytes('SEND\nfoo:'), 0xff, ...bytes('\n\n\0')]);
    assert.throws(() => parseAll(data), isParseError('invalid-utf8'));
  });

  it('copies bodies out of its internal buffer', () => {
    const { parser, collected } = collectingParser();
    parser.push('SEND\n\nfirst\0');
    parser.push('SEND\n\nXXXXX\0');
    assert.equal(text(collected.frames[0]!.body), 'first');
  });

  describe('header escaping', () => {
    const escaped = 'MESSAGE\na\\cb:x\\ny\\\\z\\r\n\n\0';

    it('unescapes \\n, \\c, \\\\ and \\r in 1.2', () => {
      const { frames } = parseAll(escaped, { version: '1.2' });
      assert.deepEqual(plain(frames[0]!), { 'a:b': 'x\ny\\z\r' });
    });

    it('rejects \\r in 1.1', () => {
      assert.throws(() => parseAll(escaped, { version: '1.1' }), isParseError('invalid-escape'));
    });

    it('does not unescape in 1.0', () => {
      const { frames } = parseAll(escaped, { version: '1.0' });
      assert.deepEqual(plain(frames[0]!), { 'a\\cb': 'x\\ny\\\\z\\r' });
    });

    it('does not unescape CONNECT and CONNECTED frames', () => {
      const { frames } = parseAll(['CONNECT\nlogin:a\\cb\n\n\0', 'CONNECTED\nserver:x\\y\n\n\0'], {
        version: '1.2',
      });
      assert.equal(frames[0]?.headers['login'], 'a\\cb');
      assert.equal(frames[1]?.headers['server'], 'x\\y');
    });

    it('rejects undefined escape sequences and trailing backslashes', () => {
      assert.throws(() => parseAll('MESSAGE\nfoo:\\t\n\n\0', { version: '1.2' }), isParseError('invalid-escape'));
      assert.throws(() => parseAll('MESSAGE\nfoo:bar\\\n\n\0', { version: '1.2' }), isParseError('invalid-escape'));
    });

    it('allows switching version from within onFrame, mid-chunk', () => {
      const frames: string[] = [];
      const parser = new FrameParser({
        onFrame: (frame) => {
          if (frame.command === 'CONNECTED') {
            parser.version = '1.2';
          }
          frames.push(frame.headers['foo'] ?? '');
        },
      });
      parser.push('CONNECTED\nversion:1.2\n\n\0MESSAGE\nfoo:a\\cb\n\n\0');
      assert.deepEqual(frames, ['', 'a:b']);
    });
  });

  describe('limits', () => {
    it('enforces maxHeaderSize, also on incomplete input', () => {
      assert.throws(
        () => parseAll(`SEND\nfoo:${'x'.repeat(100)}\n\n\0`, { maxHeaderSize: 64 }),
        isParseError('header-too-large'),
      );
      assert.throws(() => parseAll(`SEND\nfoo:${'x'.repeat(100)}`, { maxHeaderSize: 64 }), isParseError('header-too-large'));
    });

    it('enforces maxHeaders', () => {
      assert.throws(() => parseAll('SEND\na:1\nb:2\nc:3\n\n\0', { maxHeaders: 2 }), isParseError('too-many-headers'));
    });

    it('enforces maxBodySize with and without content-length', () => {
      assert.throws(() => parseAll('SEND\ncontent-length:10\n\n', { maxBodySize: 5 }), isParseError('body-too-large'));
      assert.throws(() => parseAll('SEND\n\n0123456789', { maxBodySize: 5 }), isParseError('body-too-large'));
      assert.throws(() => parseAll('SEND\n\n0123456789\0', { maxBodySize: 5 }), isParseError('body-too-large'));
      assert.doesNotThrow(() => parseAll('SEND\n\n01234\0', { maxBodySize: 5 }));
    });
  });

  it('refuses input after a parse error until reset', () => {
    const { parser, collected } = collectingParser();
    assert.throws(() => parser.push('SEND\nbad\n\n\0'), isParseError('invalid-header'));
    assert.throws(() => parser.push('SEND\n\n\0'), isParseError('parser-failed'));
    parser.reset();
    parser.push('SEND\n\n\0');
    assert.equal(collected.frames.length, 1);
  });

  it('stays usable when onFrame throws', () => {
    let calls = 0;
    const throwing = new FrameParser({
      onFrame: () => {
        calls += 1;
        if (calls === 1) {
          throw new Error('boom');
        }
      },
    });
    assert.throws(() => throwing.push('SEND\n\n\0SEND\n\n\0'), /boom/);
    throwing.push('');
    throwing.push('SEND\n\n\0');
    assert.equal(calls, 3);
  });

  describe('pausing', () => {
    it('holds back the rest of a chunk when paused from onFrame', () => {
      const seen: string[] = [];
      const parser = new FrameParser({
        onFrame: (frame) => {
          seen.push(frame.command);
          if (frame.command === 'CONNECT') {
            parser.pause();
          }
        },
      });
      parser.push('CONNECT\n\n\0SEND\n\n\0');
      assert.deepEqual(seen, ['CONNECT']);
      assert.equal(parser.paused, true);
      parser.push('SEND\n\n\0');
      assert.deepEqual(seen, ['CONNECT']);
      parser.resume();
      assert.deepEqual(seen, ['CONNECT', 'SEND', 'SEND']);
      assert.equal(parser.buffered, 0);
    });

    it('treats resume as a no-op when not paused', () => {
      const { parser, collected } = collectingParser();
      parser.resume();
      parser.push('SEND\n\n\0');
      assert.equal(collected.frames.length, 1);
    });

    it('reports parse errors in buffered data on resume', () => {
      const { parser } = collectingParser();
      parser.pause();
      parser.push('SEND\nbad\n\n\0');
      assert.throws(() => parser.resume(), isParseError('invalid-header'));
    });

    it('supports synchronous pause and resume from onFrame', () => {
      const seen: string[] = [];
      const parser = new FrameParser({
        onFrame: (frame) => {
          seen.push(frame.headers['n'] ?? '');
          parser.pause();
          parser.resume();
        },
      });
      parser.push('SEND\nn:1\n\n\0SEND\nn:2\n\n\0');
      assert.deepEqual(seen, ['1', '2']);
    });
  });

  it('tracks partial frames and buffered bytes', () => {
    const { parser } = collectingParser();
    assert.equal(parser.partial, false);
    parser.push('SEND\nfoo:bar\n');
    assert.equal(parser.partial, true);
    parser.push('\nbody\0');
    assert.equal(parser.partial, false);
    assert.equal(parser.buffered, 0);
  });

  it('grows its buffer for large bodies', () => {
    const big = 'x'.repeat(200_000);
    const { frames } = parseAll([`SEND\n\n${big.slice(0, 1000)}`, big.slice(1000), '\0SEND\n\n\0']);
    assert.equal(frames[0]?.body.byteLength, 200_000);
    assert.equal(frames.length, 2);
  });
});
