import { useQuery } from "@tanstack/react-query";
import { queuesApi } from "../api/queues";

export function useQueues() {
    return useQuery({
        queryKey: ["queues"],
        queryFn: queuesApi.listQueues,
        refetchInterval: 5000, // Refetch every 5 seconds for real-time updates
    });
}

export function useQueueStats(queueName: string) {
    return useQuery({
        queryKey: ["queue-stats", queueName],
        queryFn: () => queuesApi.getQueueStats(queueName),
        enabled: !!queueName,
        refetchInterval: 5000,
    });
}

export function useRecentMessages() {
    return useQuery({
        queryKey: ["recent-messages"],
        queryFn: queuesApi.getRecentMessages,
        refetchInterval: 5000,
    });
}
