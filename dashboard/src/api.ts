export interface NodeStatus {
  nodeId: string;
  role: 'Follower' | 'Candidate' | 'Leader';
  currentTerm: string;
  commitIndex: string;
  lastApplied: string;
  lastLogIndex: string;
  lastLogTerm: string;
  availability: 'Starting' | 'Running' | 'Stopping' | 'Stopped' | 'Faulted';
}

export interface ClusterStatus {
  enabled: boolean;
  mode: 'local-in-process' | 'disabled';
  availabilityDescription: string;
  observedAt: string;
  nodes: NodeStatus[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isNode(value: unknown): value is NodeStatus {
  if (!isRecord(value)) return false;
  const indices = ['currentTerm', 'commitIndex', 'lastApplied', 'lastLogIndex', 'lastLogTerm'];
  return typeof value.nodeId === 'string' && value.nodeId.length > 0
    && ['Follower', 'Candidate', 'Leader'].includes(String(value.role))
    && ['Starting', 'Running', 'Stopping', 'Stopped', 'Faulted'].includes(String(value.availability))
    && indices.every(key => typeof value[key] === 'string' && /^\d+$/.test(value[key]));
}

function isCluster(value: unknown): value is ClusterStatus {
  return isRecord(value) && typeof value.enabled === 'boolean'
    && ['local-in-process', 'disabled'].includes(String(value.mode))
    && typeof value.availabilityDescription === 'string'
    && typeof value.observedAt === 'string' && Number.isFinite(Date.parse(value.observedAt))
    && Array.isArray(value.nodes) && value.nodes.every(isNode);
}

export function parseCluster(value: unknown): ClusterStatus {
  if (!isCluster(value)) throw new Error('Monitoring API returned an invalid cluster response.');
  return value;
}

export async function fetchCluster(signal: AbortSignal): Promise<ClusterStatus> {
  const response = await fetch('/admin/cluster', { signal, cache: 'no-store' });
  if (!response.ok) throw new Error(`Monitoring API returned HTTP ${response.status}.`);
  return parseCluster(await response.json());
}

export type NodeAction = 'stop' | 'start' | 'restart';

export async function changeNode(
  nodeId: string, action: NodeAction, signal: AbortSignal,
): Promise<NodeStatus> {
  const response = await fetch(`/admin/nodes/${encodeURIComponent(nodeId)}/${action}`, {
    method: 'POST', signal, cache: 'no-store',
  });
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`Node control returned HTTP ${response.status} without valid JSON. Refresh node status before retrying.`);
  }
  if (!response.ok) {
    throw new Error(isRecord(payload) && typeof payload.detail === 'string'
      ? payload.detail : `Node control returned HTTP ${response.status}.`);
  }
  if (!isNode(payload) || payload.nodeId !== nodeId
    || payload.availability !== (action === 'stop' ? 'Stopped' : 'Running')) {
    throw new Error('Node control returned an invalid confirmation. Refresh node status before retrying.');
  }
  return payload;
}

export function maxIndex(values: string[]): string {
  return values.reduce((max, value) => BigInt(value) > BigInt(max) ? value : max, '0');
}

export function applicationPercent(node: NodeStatus): number {
  const committed = BigInt(node.commitIndex);
  if (committed === 0n) return 0;
  const applied = BigInt(node.lastApplied);
  return Number((applied > committed ? committed : applied) * 100n / committed);
}

export interface DistributedWrite {
  key: string;
  operation: 'PUT' | 'DELETE';
  leaderId: string;
  logIndex: string;
  term: string;
  committed: true;
  applied: true;
}

export interface DistributedRead {
  key: string;
  value: string;
  leaderId: string;
  term: string;
  lastApplied: string;
  consistency: 'leader-local-applied';
  linearizable: false;
}

export type KeyValueResult =
  | { kind: 'write'; response: DistributedWrite }
  | { kind: 'read'; response: DistributedRead };

export class DistributedApiError extends Error {
  readonly outcome: string;

  constructor(message: string, outcome: string) {
    super(message);
    this.name = 'DistributedApiError';
    this.outcome = outcome;
  }
}

function isIndex(value: unknown): value is string {
  return typeof value === 'string' && /^\d+$/.test(value);
}

function isWrite(value: unknown): value is DistributedWrite {
  return isRecord(value) && typeof value.key === 'string'
    && (value.operation === 'PUT' || value.operation === 'DELETE')
    && typeof value.leaderId === 'string' && value.leaderId.length > 0
    && isIndex(value.logIndex) && isIndex(value.term)
    && value.committed === true && value.applied === true;
}

function isRead(value: unknown): value is DistributedRead {
  return isRecord(value) && typeof value.key === 'string' && typeof value.value === 'string'
    && typeof value.leaderId === 'string' && value.leaderId.length > 0
    && isIndex(value.term) && isIndex(value.lastApplied)
    && value.consistency === 'leader-local-applied' && value.linearizable === false;
}

export async function executeKeyValue(
  method: 'PUT' | 'GET' | 'DELETE',
  key: string,
  value: string,
  signal: AbortSignal,
): Promise<KeyValueResult> {
  const response = await fetch(`/admin/kv/${encodeURIComponent(key)}`, {
    method,
    signal,
    cache: 'no-store',
    ...(method === 'PUT' ? {
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value }),
    } : {}),
  });
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    throw new DistributedApiError(
      `Distributed API returned HTTP ${response.status} without a valid JSON response.`,
      method === 'GET' ? 'not-applicable' : 'unknown',
    );
  }
  if (!response.ok) {
    const detail = isRecord(payload) && typeof payload.detail === 'string'
      ? payload.detail : `Distributed API returned HTTP ${response.status}.`;
    const outcome = isRecord(payload) && typeof payload.outcome === 'string'
      ? payload.outcome : method === 'GET' ? 'not-applicable' : 'unknown';
    throw new DistributedApiError(detail, outcome);
  }
  if (method === 'GET' && isRead(payload)) return { kind: 'read', response: payload };
  if (method !== 'GET' && isWrite(payload) && payload.operation === method) {
    return { kind: 'write', response: payload };
  }
  throw new DistributedApiError('Distributed API returned an invalid confirmation.',
    method === 'GET' ? 'not-applicable' : 'unknown');
}
