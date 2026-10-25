import { useEffect, useRef, useState } from 'react';
import { DistributedApiError, executeKeyValue } from './api';
import type { KeyValueResult } from './api';

export function KeyValueExplorer({
  enabled, onCommitted,
}: { enabled: boolean; onCommitted: () => void }) {
  const [key, setKey] = useState('');
  const [value, setValue] = useState('');
  const [format, setFormat] = useState('text');
  const [pending, setPending] = useState<string | null>(null);
  const [result, setResult] = useState<KeyValueResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const request = useRef<AbortController | null>(null);
  const mounted = useRef(false);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      request.current?.abort();
    };
  }, []);

  function clearResult() {
    setResult(null);
    setError(null);
  }

  async function execute(method: 'PUT' | 'GET' | 'DELETE') {
    if (request.current) return;
    clearResult();
    if (!key.trim()) {
      setError('Enter a non-empty key.');
      return;
    }
    if (method === 'PUT' && format === 'json') {
      try {
        JSON.parse(value);
      } catch {
        setError('The value is not valid JSON. Correct it or select Text.');
        return;
      }
    }

    const controller = new AbortController();
    request.current = controller;
    setPending(method);
    const timeout = setTimeout(() => controller.abort(), 10000);
    try {
      const next = await executeKeyValue(method, key, value, controller.signal);
      if (mounted.current) {
        setResult(next);
        if (next.kind === 'read') setValue(next.response.value);
        else onCommitted();
      }
    } catch (failure) {
      if (mounted.current) {
        const message = failure instanceof Error && failure.name !== 'AbortError'
          ? failure.message : 'The request timed out or the connection was interrupted.';
        const outcome = failure instanceof DistributedApiError ? failure.outcome
          : method === 'GET' ? 'not-applicable' : 'unknown';
        setError(`${message} Outcome: ${outcome}.${
          method !== 'GET' && outcome === 'unknown'
            ? ' The command may still commit. No automatic retry was performed; cancellation is not rollback.'
            : ''
        }`);
      }
    } finally {
      clearTimeout(timeout);
      request.current = null;
      if (mounted.current) setPending(null);
    }
  }

  const inputClass = 'mt-2 w-full rounded-lg border border-slate-700 bg-slate-950 p-3 text-sm text-slate-100 outline-none focus:border-cyan-500 disabled:opacity-50';

  return (
    <section className="panel mt-10 p-6 sm:p-7" aria-labelledby="explorer-title">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h2 id="explorer-title" className="text-lg font-semibold">Distributed Key-Value Explorer</h2>
        <span className="badge text-cyan-300">Development / Raft-backed</span>
      </div>
      <p className="mt-3 text-sm leading-6 text-slate-400">
        PUT and DELETE wait for quorum commitment and leader application. GET reads leader-local
        applied state: not linearizable, potentially stale during partitions, leadership changes, or recovery.
      </p>
      {!enabled && <p className="mt-3 text-sm text-amber-300">Connect to an enabled local Raft cluster to execute operations.</p>}
      <fieldset disabled={!enabled || pending !== null} className="mt-6 space-y-5">
        <label className="block text-sm text-slate-300">
          Key
          <input className={inputClass} value={key} placeholder="user:1"
            onChange={event => { setKey(event.target.value); clearResult(); }} />
        </label>
        <div>
          <div className="flex items-center justify-between gap-3">
            <label htmlFor="kv-value" className="text-sm text-slate-300">Value</label>
            <select aria-label="Value format" value={format}
              onChange={event => setFormat(event.target.value)}
              className="rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-xs text-slate-300">
              <option value="text">Text</option>
              <option value="json">JSON</option>
            </select>
          </div>
          <textarea id="kv-value" className={`${inputClass} min-h-32 font-mono`} value={value}
            placeholder='{"name":"Alice"}'
            onChange={event => { setValue(event.target.value); clearResult(); }} />
        </div>
        <div className="flex flex-wrap gap-3">
          {(['PUT', 'GET', 'DELETE'] as const).map(method => (
            <button key={method} type="button" onClick={() => void execute(method)}
              className={`rounded-lg border px-5 py-2.5 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-40 ${
                method === 'DELETE' ? 'border-rose-800 text-rose-300 hover:bg-rose-950/40'
                  : 'border-cyan-800 text-cyan-300 hover:bg-cyan-950/40'
              }`}>
              {pending === method ? `${method} in progress...` : method}
            </button>
          ))}
        </div>
      </fieldset>
      {pending && <p role="status" className="mt-4 text-sm text-slate-400">Waiting for the real cluster response...</p>}
      {error && <p role="alert" className="mt-5 break-words rounded-lg border border-amber-800 bg-amber-950/30 p-4 text-sm text-amber-200">{error}</p>}
      {result && (
        <div role="status" className="mt-5 rounded-lg border border-emerald-900 bg-emerald-950/20 p-4 text-sm text-slate-300">
          {result.kind === 'write' ? (
            <>
              <p className="font-semibold text-emerald-300">{result.response.operation} committed and applied</p>
              <p className="mt-2 break-all font-mono">Leader {result.response.leaderId} / log index {result.response.logIndex} / term {result.response.term}</p>
            </>
          ) : (
            <>
              <p className="font-semibold text-emerald-300">Value read from {result.response.leaderId}</p>
              <pre className="mt-3 whitespace-pre-wrap break-all font-mono">{result.response.value === '' ? '(empty string)' : result.response.value}</pre>
              <p className="mt-3 text-xs text-slate-400">Consistency: {result.response.consistency} (not linearizable) / observed last applied {result.response.lastApplied} / term {result.response.term}</p>
            </>
          )}
        </div>
      )}
    </section>
  );
}
