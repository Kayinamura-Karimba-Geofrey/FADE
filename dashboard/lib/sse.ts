import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

/** Subscribes to the case event stream and invalidates the queue on updates. */
export function useCaseEvents() {
  const queryClient = useQueryClient();
  useEffect(() => {
    const source = new EventSource("/api/proxy/v1/events/stream", {
      withCredentials: true,
    });
    const invalidate = () => {
      queryClient.invalidateQueries({ queryKey: ["cases"] });
      queryClient.invalidateQueries({ queryKey: ["case-metrics"] });
    };
    source.addEventListener("case_created", invalidate);
    source.addEventListener("case_updated", invalidate);
    source.addEventListener("stats_tick", invalidate);
    return () => source.close();
  }, [queryClient]);
}