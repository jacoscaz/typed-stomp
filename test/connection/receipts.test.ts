import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ReceiptTracker } from '../../src/connection/receipts.ts';
import { flush, frame } from '../helpers.ts';

describe('ReceiptTracker', () => {
  it('allocates unique ids and resolves them', async () => {
    const receipts = new ReceiptTracker('r-');
    const a = receipts.create();
    const b = receipts.create();
    assert.equal(a.id, 'r-0');
    assert.equal(b.id, 'r-1');
    assert.equal(receipts.size, 2);
    const receipt = frame('RECEIPT', { 'receipt-id': 'r-1' });
    assert.equal(receipts.resolve('r-1', receipt), true);
    assert.equal(await b.promise, receipt);
    assert.equal(receipts.has('r-1'), false);
    assert.equal(receipts.resolve('r-1', receipt), false);
    assert.equal(receipts.size, 1);
  });

  it('rejects single receipts and all pending receipts', async () => {
    const receipts = new ReceiptTracker();
    const a = receipts.create();
    const b = receipts.create();
    const c = receipts.create();
    assert.equal(receipts.reject(a.id, new Error('a')), true);
    receipts.rejectAll(new Error('all'));
    await assert.rejects(a.promise, /a/);
    await assert.rejects(b.promise, /all/);
    await assert.rejects(c.promise, /all/);
    assert.equal(receipts.size, 0);
  });

  it('does not cause unhandled rejections when nobody awaits', async () => {
    const receipts = new ReceiptTracker();
    receipts.create();
    receipts.rejectAll(new Error('ignored'));
    // node:test reports unhandled rejections as failures.
    await flush();
  });
});
