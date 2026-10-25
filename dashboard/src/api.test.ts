import assert from 'node:assert/strict';
import test from 'node:test';
import { applicationPercent, maxIndex, parseCluster } from './api.ts';

const node = {
  nodeId: 'test-node',
  role: 'Follower' as const,
  currentTerm: '9007199254740993',
  commitIndex: '9007199254740993',
  lastApplied: '9007199254740992',
  lastLogIndex: '9007199254740993',
  lastLogTerm: '7',
  availability: 'Running' as const,
};

test('preserves Int64 values without converting them to JavaScript numbers', () => {
  const cluster = parseCluster({
    enabled: true,
    mode: 'local-in-process',
    availabilityDescription: 'Test observation',
    observedAt: '2026-10-25T12:00:00Z',
    nodes: [node],
  });
  assert.equal(cluster.nodes[0].currentTerm, '9007199254740993');
  assert.equal(maxIndex(['9007199254740992', '9007199254740993']), '9007199254740993');
  assert.equal(applicationPercent(node), 99);
});

test('rejects malformed API responses instead of showing fabricated defaults', () => {
  for (const value of [null, {}, { enabled: true, nodes: [] }]) {
    assert.throws(() => parseCluster(value), /invalid cluster response/);
  }
});

test('rejects numeric or invalid index fields', () => {
  for (const currentTerm of [123, '-1', 'NaN']) {
    assert.throws(() => parseCluster({
      enabled: true,
      mode: 'local-in-process',
      availabilityDescription: 'Test',
      observedAt: new Date().toISOString(),
      nodes: [{ ...node, currentTerm }],
    }), /invalid cluster response/);
  }
});

test('zero committed entries and empty observations do not imply application progress', () => {
  assert.equal(applicationPercent({ ...node, commitIndex: '0', lastApplied: '0' }), 0);
  assert.equal(maxIndex([]), '0');
});
