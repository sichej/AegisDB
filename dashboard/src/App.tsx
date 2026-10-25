import { applicationPercent, maxIndex } from './api';
import type { NodeStatus } from './api';
import { useCluster } from './useCluster';

function Metric({ label, value, note }: { label: string; value: string; note: string }) {
  return (
    <div className="panel min-w-0 p-5">
      <p className="eyebrow">{label}</p>
      <p className="mt-3 break-all font-mono text-2xl font-semibold text-slate-100">{value}</p>
      <p className="mt-2 text-xs text-slate-400">{note}</p>
    </div>
  );
}

function NodeCard({ node, stale }: { node: NodeStatus; stale: boolean }) {
  const leader = node.role === 'Leader';
  const progress = applicationPercent(node);
  const fields = [
    ['Current term', node.currentTerm],
    ['Commit index', node.commitIndex],
    ['Last applied', node.lastApplied],
    ['Last log index', node.lastLogIndex],
    ['Last log term', node.lastLogTerm],
  ];

  return (
    <article className={`panel overflow-hidden ${leader && !stale ? 'border-cyan-700/70' : ''}`}>
      <div className={`h-1 ${leader ? 'bg-cyan-400' : 'bg-slate-700'} ${stale ? 'opacity-30' : ''}`} />
      <div className="p-6">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="eyebrow">Raft member</p>
            <h3 className="mt-2 font-mono text-lg font-semibold">{node.nodeId}</h3>
          </div>
          <span className={leader ? 'badge border-cyan-700 text-cyan-300' : 'badge text-slate-300'}>
            {stale ? `Last seen: ${node.role}` : node.role}
          </span>
        </div>
        <p className="mt-5 flex items-center gap-2 text-xs text-slate-400">
          <span className={`status-dot ${!stale && node.availability === 'Running' ? 'bg-emerald-400' : 'bg-amber-400'}`} />
          {stale ? 'Observation stale' : `Local runtime: ${node.availability}`}
        </p>
        <dl className="mt-6 space-y-3">
          {fields.map(([label, value]) => (
            <div key={label} className="flex items-center justify-between gap-4 text-sm">
              <dt className="text-slate-400">{label}</dt>
              <dd className="break-all text-right font-mono text-slate-100">{value}</dd>
            </div>
          ))}
        </dl>
        <div className="mt-6 border-t border-slate-800 pt-5">
          <div className="flex justify-between gap-3 text-xs text-slate-400">
            <span>Applied / committed</span>
            <span className="font-mono">{node.lastApplied} / {node.commitIndex}</span>
          </div>
          <div
            className="mt-3 h-1.5 overflow-hidden rounded-full bg-slate-800"
            role="progressbar"
            aria-label={`${node.nodeId} committed entries applied`}
            aria-valuenow={progress}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuetext={node.commitIndex === '0' ? 'No committed entries' : `${progress}%`}
          >
            <div className="h-full rounded-full bg-cyan-400" style={{ width: `${progress}%` }} />
          </div>
          <p className="mt-2 text-xs text-slate-500">
            {node.commitIndex === '0' ? 'No committed entries yet' : `${progress}% of committed entries applied`}
          </p>
        </div>
      </div>
    </article>
  );
}

export default function App() {
  const { data, connection, error } = useCluster();
  const stale = connection === 'disconnected';
  const leaders = data?.nodes.filter(node => node.role === 'Leader' && node.availability === 'Running') ?? [];

  return (
    <div className="min-h-screen">
      <header className="border-b border-slate-800/80 bg-slate-950/60">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-5 py-5 sm:px-8">
          <a href="/" className="flex items-center gap-3 text-slate-100">
            <span className="flex h-10 w-10 items-center justify-center rounded-xl border border-cyan-700 bg-cyan-950/50 font-mono text-lg font-bold text-cyan-300">A</span>
            <span className="text-lg font-semibold tracking-tight">AegisDB <span className="ml-2 text-xs font-normal text-slate-500">/ cluster monitor</span></span>
          </a>
          <div className="badge gap-2" role="status" aria-live="polite">
            <span className={`status-dot ${connection === 'connected' ? 'bg-emerald-400' : 'bg-amber-400'}`} />
            {connection === 'connected' ? 'Connected to monitoring API' : connection === 'loading' ? 'Connecting' : 'Disconnected'}
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-5 py-9 sm:px-8 sm:py-12">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="eyebrow text-cyan-400">Observability / local environment</p>
            <h1 className="mt-3 text-3xl font-semibold tracking-tight sm:text-4xl">Cluster overview</h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-400">
              Live consensus state from Raft nodes hosted by this server. Read-only, automatically refreshed.
            </p>
          </div>
          <p className="text-xs text-slate-500">
            {data ? `Last observation ${new Date(data.observedAt).toLocaleTimeString()}` : 'Waiting for the first observation'}
            <span className="mt-1 block sm:text-right">Polling every ~1 second</span>
          </p>
        </div>

        {error && (
          <div role="alert" className="mt-7 rounded-xl border border-amber-800 bg-amber-950/30 p-5 text-sm text-amber-200">
            <p className="font-semibold">Monitoring disconnected</p>
            <p className="mt-2">{error} Retrying automatically.</p>
            <p className="mt-1 text-amber-300/70">
              {data ? 'Displayed values are the last successful observation, not current node health.' : 'No node state is available. Check that the backend and dashboard proxy are running.'}
            </p>
          </div>
        )}

        {connection === 'loading' && (
          <div className="panel mt-8 p-10 text-center text-slate-400" role="status">Loading real cluster state...</div>
        )}

        {data && !data.enabled && (
          <section className="panel mt-8 p-7">
            <h2 className="text-lg font-semibold">Local Raft cluster is disabled</h2>
            <p className="mt-3 text-sm leading-6 text-slate-400">
              This server is running the single-node storage API only. Start it in Development with
              <code className="mx-1 text-cyan-300">Raft__Enabled=true</code> to observe three real in-process Raft nodes.
            </p>
          </section>
        )}

        {data?.enabled && (
          <>
            <section aria-label="Cluster metrics" className={`mt-8 grid gap-4 sm:grid-cols-2 xl:grid-cols-4 ${stale ? 'opacity-60' : ''}`}>
              <Metric label="Observed members" value={String(data.nodes.length)} note="Hosted in this server process" />
              <Metric label={stale ? 'Last observed leader' : 'Observed leader'} value={leaders.map(node => node.nodeId).join(', ') || 'None observed'} note="Role reported by local snapshots" />
              <Metric label="Highest observed term" value={maxIndex(data.nodes.map(node => node.currentTerm))} note="Maximum across individual observations" />
              <Metric label="Highest commit index" value={maxIndex(data.nodes.map(node => node.commitIndex))} note="Not a cluster-wide commit guarantee" />
            </section>

            {leaders.length > 1 && (
              <p role="alert" className="mt-5 text-sm text-amber-300">
                Multiple leader roles observed. Compare terms; per-node snapshots are not cluster-atomic.
              </p>
            )}

            <section className="mt-10" aria-label="Raft nodes">
              <div className="mb-5 flex flex-wrap items-center justify-between gap-3">
                <h2 className="text-lg font-semibold">Raft nodes <span className="ml-2 font-mono text-sm text-slate-500">/ {data.nodes.length}</span></h2>
                <span className="badge text-slate-400">In-process transport</span>
              </div>
              {data.nodes.length === 0
                ? <p className="panel p-6 text-slate-400">No local node observations are available.</p>
                : <div className={`grid gap-5 md:grid-cols-2 xl:grid-cols-3 ${stale ? 'opacity-60' : ''}`}>
                  {data.nodes.map(node => <NodeCard key={node.nodeId} node={node} stale={stale} />)}
                </div>}
            </section>
          </>
        )}

        <aside className="mt-9 rounded-xl border border-slate-800 bg-slate-900/30 p-5 text-xs leading-6 text-slate-400">
          <p className="font-semibold uppercase tracking-wider text-slate-300">Observation scope</p>
          <p className="mt-2">{data?.availabilityDescription ?? 'Local runtime availability is not network reachability or quorum health.'}</p>
          <p>The /kv API uses its existing independent WAL engine. Its writes do not enter these Raft logs. Node stop/restart controls are not exposed.</p>
        </aside>
      </main>
      <footer className="mx-auto max-w-7xl px-5 pb-8 text-xs text-slate-600 sm:px-8">
        AegisDB / development monitoring / read-only consensus observations
      </footer>
    </div>
  );
}
