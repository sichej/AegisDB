export interface NodeStatus {
  nodeId: string;
  role: 'Follower' | 'Candidate' | 'Leader';
  currentTerm: string;
  commitIndex: string;
  lastApplied: string;
  lastLogIndex: string;
  lastLogTerm: string;
  availability: 'Starting' | 'Running' | 'Stopped' | 'Faulted';
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
    && ['Starting', 'Running', 'Stopped', 'Faulted'].includes(String(value.availability))
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

export function maxIndex(values: string[]): string {
  return values.reduce((max, value) => BigInt(value) > BigInt(max) ? value : max, '0');
}

export function applicationPercent(node: NodeStatus): number {
  const committed = BigInt(node.commitIndex);
  if (committed === 0n) return 0;
  const applied = BigInt(node.lastApplied);
  return Number((applied > committed ? committed : applied) * 100n / committed);
}
