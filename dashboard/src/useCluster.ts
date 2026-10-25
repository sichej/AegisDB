import { useCallback, useEffect, useState } from 'react';
import { fetchCluster } from './api';
import type { ClusterStatus } from './api';

interface Observation {
  data: ClusterStatus | null;
  connection: 'loading' | 'connected' | 'disconnected';
  error: string | null;
}

export function useCluster(): Observation & { refresh: () => void } {
  const [generation, setGeneration] = useState(0);
  const refresh = useCallback(() => setGeneration(previous => previous + 1), []);
  const [observation, setObservation] = useState<Observation>({
    data: null, connection: 'loading', error: null,
  });

  useEffect(() => {
    let disposed = false;
    let nextPoll: ReturnType<typeof setTimeout> | undefined;
    let request: AbortController | undefined;

    async function poll() {
      request = new AbortController();
      const timeout = setTimeout(() => request?.abort(), 5000);

      try {
        const data = await fetchCluster(request.signal);
        if (!disposed) setObservation({ data, connection: 'connected', error: null });
      } catch (error) {
        if (!disposed) {
          const message = error instanceof Error && error.name !== 'AbortError'
            ? error.message : 'Monitoring request timed out.';
          setObservation(previous => ({ ...previous, connection: 'disconnected', error: message }));
        }
      } finally {
        clearTimeout(timeout);
        if (!disposed) nextPoll = setTimeout(poll, 1000);
      }
    }

    void poll();
    return () => {
      disposed = true;
      clearTimeout(nextPoll);
      request?.abort();
    };
  }, [generation]);

  return { ...observation, refresh };
}
