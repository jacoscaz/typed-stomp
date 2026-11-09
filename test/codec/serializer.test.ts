import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { StompSerializeError } from '../../src/codec/errors.ts';
import type { FrameInit, StompVersion } from '../../src/codec/frame.ts';
import { serializeFrame, serializeFrameToString } from '../../src/codec/serializer.ts';
import { bytes, parseAll, plain, prng, text } from '../helpers.ts';

const isSerializeError = (code: string) => (err: unknown) => err instanceof StompSerializeError && err.code === code;

describe('serializeFrame / serializeFrameToString', () => {
  it('serializes a frame with headers and a body', () => {
    const frame: FrameInit = { command: 'SEND', headers: { destination: '/a' }, body: 'héllo' };
    assert.equal(
      serializeFrameToString(frame, { version: '1.2' }),
      'SEND\ndestination:/a\ncontent-length:6\n\nhéllo\0',
    );
    assert.deepEqual(serializeFrame(frame, { version: '1.2' }), bytes('SEND\ndestination:/a\ncontent-length:6\n\nhéllo\0'));
  });

  it('omits content-length for empty bodies and when disabled', () => {
    assert.equal(serializeFrameToString({ command: 'DISCONNECT' }, { version: '1.2' }), 'DISCONNECT\n\n\0');
    assert.equal(
      serializeFrameToString({ command: 'SEND', body: 'x' }, { version: '1.2', contentLength: false }),
      'SEND\n\nx\0',
    );
  });

  it('skips headers set to undefined', () => {
    assert.equal(
      serializeFrameToString({ command: 'SEND', headers: { a: '1', b: undefined } }, { version: '1.2' }),
      'SEND\na:1\n\n\0',
    );
  });

  it('checks explicit content-length headers', () => {
    assert.equal(
      serializeFrameToString({ command: 'SEND', headers: { 'content-length': '0' } }, { version: '1.2' }),
      'SEND\ncontent-length:0\n\n\0',
    );
    assert.throws(
      () => serializeFrame({ command: 'SEND', headers: { 'content-length': '3' }, body: 'ab' }, { version: '1.2' }),
      isSerializeError('content-length-mismatch'),
    );
  });

  it('rejects NUL in bodies without content-length', () => {
    assert.throws(
      () => serializeFrame({ command: 'SEND', body: Uint8Array.of(0) }, { version: '1.2', contentLength: false }),
      isSerializeError('null-in-body'),
    );
    assert.throws(
      () => serializeFrameToString({ command: 'SEND', body: 'a\0b' }, { version: '1.2', contentLength: false }),
      isSerializeError('null-in-body'),
    );
  });

  it('refuses to serialize binary bodies as text', () => {
    assert.throws(
      () => serializeFrameToString({ command: 'SEND', body: Uint8Array.of(0xff) }, { version: '1.2' }),
      isSerializeError('binary-body'),
    );
  });

  it('rejects invalid commands', () => {
    assert.throws(() => serializeFrame({ command: '' }, { version: '1.2' }), isSerializeError('invalid-command'));
    assert.throws(() => serializeFrame({ command: 'SE\nND' }, { version: '1.2' }), isSerializeError('invalid-command'));
  });

  describe('header escaping', () => {
    const headers = { 'a:b': 'x\ny\\z' };

    it('escapes headers in 1.1 and 1.2', () => {
      for (const version of ['1.1', '1.2'] as const) {
        assert.equal(
          serializeFrameToString({ command: 'SEND', headers }, { version }),
          'SEND\na\\cb:x\\ny\\\\z\n\n\0',
        );
      }
      assert.equal(
        serializeFrameToString({ command: 'SEND', headers: { a: 'x\r' } }, { version: '1.2' }),
        'SEND\na:x\\r\n\n\0',
      );
    });

    it('cannot encode CR in 1.1', () => {
      assert.throws(
        () => serializeFrame({ command: 'SEND', headers: { a: 'x\r' } }, { version: '1.1' }),
        isSerializeError('unencodable-header'),
      );
    });

    it('does not escape CONNECT and CONNECTED frames', () => {
      assert.equal(
        serializeFrameToString({ command: 'CONNECT', headers: { passcode: 'a:b\\c' } }, { version: '1.2' }),
        'CONNECT\npasscode:a:b\\c\n\n\0',
      );
    });

    it('rejects unencodable headers when escaping is not available', () => {
      for (const [command, version] of [
        ['SEND', '1.0'],
        ['CONNECT', '1.2'],
      ] as const) {
        assert.throws(
          () => serializeFrame({ command, headers: { a: 'x\ny' } }, { version }),
          isSerializeError('unencodable-header'),
        );
        assert.throws(
          () => serializeFrame({ command, headers: { 'a:b': 'x' } }, { version }),
          isSerializeError('unencodable-header'),
        );
      }
    });

    it('rejects empty header names', () => {
      assert.throws(
        () => serializeFrame({ command: 'SEND', headers: { '': 'x' } }, { version: '1.2' }),
        isSerializeError('unencodable-header'),
      );
    });
  });

  describe('round trips through the parser', () => {
    const versions: StompVersion[] = ['1.0', '1.1', '1.2'];
    const alphabet = ['a', 'b', 'é', '😀', ':', '\\', '\n', '\r', '\0', ' '];

    const randomString = (random: () => number, max: number, allowed: readonly string[]) => {
      let out = '';
      const length = Math.floor(random() * max);
      for (let i = 0; i < length; i += 1) {
        out += allowed[Math.floor(random() * allowed.length)];
      }
      return out;
    };

    it('preserves randomized frames split at random points', () => {
      const random = prng(42);
      for (let round = 0; round < 300; round += 1) {
        const version = versions[round % versions.length]!;
        // 1.0 cannot escape; 1.1 cannot escape CR.
        const headerAlphabet =
          version === '1.0'
            ? alphabet.filter((c) => !'\n\r:\0'.includes(c))
            : alphabet.filter((c) => c !== '\0' && (version === '1.2' || c !== '\r'));
        const headers: Record<string, string> = {};
        const count = Math.floor(random() * 5);
        for (let i = 0; i < count; i += 1) {
          const name = `h${i}${randomString(random, 6, headerAlphabet)}`;
          headers[name] = randomString(random, 12, headerAlphabet);
        }
        const body = randomString(random, 40, alphabet);
        const wire = serializeFrame({ command: 'MESSAGE', headers, body }, { version });

        const chunks: Uint8Array[] = [];
        let offset = 0;
        while (offset < wire.byteLength) {
          const size = 1 + Math.floor(random() * 16);
          chunks.push(wire.subarray(offset, offset + size));
          offset += size;
        }
        const { frames } = parseAll(chunks, { version });
        assert.equal(frames.length, 1);
        const { ['content-length']: _, ...parsedHeaders } = plain(frames[0]!);
        assert.deepEqual(parsedHeaders, headers);
        assert.equal(text(frames[0]!.body), body);
      }
    });
  });
});
