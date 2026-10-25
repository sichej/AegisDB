import assert from 'node:assert/strict';
import test from 'node:test';
import { applicationPercent, changeNode, DistributedApiError, executeKeyValue, maxIndex, parseCluster } from './api.ts';

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

test('PUT uses only the distributed endpoint and preserves text and Int64 receipts', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async (url: string, options: RequestInit) => {
    calls++;
    assert.equal(url, '/admin/kv/user%3A1');
    assert.equal(options.method, 'PUT');
    assert.equal(options.body, JSON.stringify({ value: '{"name":"Alice"}' }));
    return Response.json({
      key: 'user:1', operation: 'PUT', leaderId: 'node-2',
      logIndex: '9007199254740993', term: '5', committed: true, applied: true,
    });
  });
  const result = await executeKeyValue('PUT', 'user:1', '{"name":"Alice"}', new AbortController().signal);
  assert.equal(result.kind, 'write');
  if (result.kind === 'write') assert.equal(result.response.logIndex, '9007199254740993');
  assert.equal(calls, 1);
});

test('GET exposes non-linearizable reads and accepts an empty committed value', async t => {
  t.mock.method(globalThis, 'fetch', async (_url: string, options: RequestInit) => {
    assert.equal(options.method, 'GET');
    assert.equal(options.body, undefined);
    return Response.json({
      key: 'empty', value: '', leaderId: 'node-1', term: '1', lastApplied: '2',
      consistency: 'leader-local-applied', linearizable: false,
    });
  });
  const result = await executeKeyValue('GET', 'empty', '', new AbortController().signal);
  assert.equal(result.kind, 'read');
  if (result.kind === 'read') assert.equal(result.response.value, '');
});

test('timeouts preserve unknown outcomes and never automatically retry writes', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    calls++;
    return Response.json({
      title: 'confirmation_timeout', detail: 'The command may still commit.',
      outcome: 'unknown', automaticRetry: false,
    }, { status: 504 });
  });
  await assert.rejects(
    executeKeyValue('DELETE', 'key', '', new AbortController().signal),
    error => error instanceof DistributedApiError
      && error.outcome === 'unknown' && /may still commit/.test(error.message),
  );
  assert.equal(calls, 1);
});

test('unconfirmed writes cannot render as successful operations', async t => {
  t.mock.method(globalThis, 'fetch', async () => Response.json({
    key: 'key', operation: 'PUT', leaderId: 'node-1', logIndex: '1', term: '1',
    committed: true, applied: false,
  }));
  await assert.rejects(
    executeKeyValue('PUT', 'key', 'value', new AbortController().signal),
    error => error instanceof DistributedApiError && error.outcome === 'unknown',
  );
});

test('non-JSON gateway errors are not write confirmations', async t => {
  t.mock.method(globalThis, 'fetch', async () => new Response('Bad gateway', { status: 502 }));
  await assert.rejects(
    executeKeyValue('PUT', 'key', 'value', new AbortController().signal),
    error => error instanceof DistributedApiError && error.outcome === 'unknown',
  );
});

test('connection failures are surfaced without resubmitting commands', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    calls++;
    throw new TypeError('Failed to fetch');
  });
  await assert.rejects(
    executeKeyValue('PUT', 'key', 'value', new AbortController().signal),
    /Failed to fetch/,
  );
  assert.equal(calls, 1);
});

test('GET rejects unsupported consistency claims', async t => {
  t.mock.method(globalThis, 'fetch', async () => Response.json({
    key: 'key', value: 'value', leaderId: 'node-1', term: '1', lastApplied: '1',
    consistency: 'linearizable', linearizable: true,
  }));
  await assert.rejects(
    executeKeyValue('GET', 'key', '', new AbortController().signal),
    /invalid confirmation/,
  );
});

test('node controls POST to the real admin endpoint and require confirmed availability', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async (url: string, options: RequestInit) => {
    calls++;
    assert.equal(url, '/admin/nodes/test-node/stop');
    assert.equal(options.method, 'POST');
    return Response.json({ ...node, availability: 'Stopped' });
  });
  assert.equal((await changeNode('test-node', 'stop', new AbortController().signal)).availability, 'Stopped');
  assert.equal(calls, 1);
});

test('lifecycle errors are surfaced without optimistic success or retries', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    calls++;
    return Response.json({ detail: 'Another lifecycle operation is in progress.' }, { status: 409 });
  });
  await assert.rejects(changeNode('test-node', 'restart', new AbortController().signal), /in progress/);
  assert.equal(calls, 1);
});

test('start cannot succeed with a stopped or wrong-node response', async t => {
  const mock = t.mock.method(globalThis, 'fetch', async () => Response.json({ ...node, availability: 'Stopped' }));
  await assert.rejects(changeNode('test-node', 'start', new AbortController().signal), /invalid confirmation/);
  mock.mock.mockImplementation(async () => Response.json({ ...node, nodeId: 'other', availability: 'Running' }));
  await assert.rejects(changeNode('test-node', 'restart', new AbortController().signal), /invalid confirmation/);
});

test('monitoring accepts actual stopping transitions', () => {
  const status = parseCluster({
    enabled: true, mode: 'local-in-process', availabilityDescription: 'Local tasks',
    observedAt: new Date().toISOString(), nodes: [{ ...node, availability: 'Stopping' }],
  });
  assert.equal(status.nodes[0].availability, 'Stopping');
});
