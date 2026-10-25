import { useEffect, useRef, useState } from 'react';
import { changeNode } from './api';
import type { NodeAction, NodeStatus } from './api';

export function NodeControls({
  node, stale, onRefresh,
}: { node: NodeStatus; stale: boolean; onRefresh: () => void }) {
  const [pending, setPending] = useState<NodeAction | null>(null);
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

  async function execute(action: NodeAction) {
    if (request.current) return;
    const controller = new AbortController();
    request.current = controller;
    setPending(action);
    setError(null);
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      await changeNode(node.nodeId, action, controller.signal);
    } catch (failure) {
      if (mounted.current) {
        setError(failure instanceof Error && failure.name !== 'AbortError'
          ? failure.message
          : 'Node control timed out or disconnected. Its outcome is unconfirmed; refresh status before retrying.');
      }
    } finally {
      clearTimeout(timeout);
      request.current = null;
      if (mounted.current) {
        setPending(null);
        onRefresh();
      }
    }
  }

  const actions: NodeAction[] = node.availability === 'Running' ? ['stop', 'restart']
    : node.availability === 'Stopped' || node.availability === 'Faulted' ? ['start'] : [];

  return (
    <div className="mt-5 border-t border-slate-800 pt-5">
      <div className="flex flex-wrap gap-3">
        {actions.map(action => (
          <button key={action} type="button" disabled={stale || pending !== null}
            onClick={() => void execute(action)}
            className="rounded-lg border border-slate-700 px-4 py-2 text-xs font-semibold capitalize text-slate-300 hover:border-cyan-700 hover:text-cyan-300 disabled:cursor-not-allowed disabled:opacity-40">
            {pending === action ? `${action} in progress...` : action}
          </button>
        ))}
      </div>
      {(pending || actions.length === 0) && (
        <p role="status" className="mt-3 text-xs text-slate-400">
          {pending ? `Waiting for ${pending} confirmation...` : `Node is ${node.availability.toLowerCase()}...`}
        </p>
      )}
      {error && <p role="alert" className="mt-3 break-words text-xs leading-5 text-amber-300">{error}</p>}
    </div>
  );
}
