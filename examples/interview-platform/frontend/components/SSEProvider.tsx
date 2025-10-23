"use client";

import { useSSE } from '@/hooks/useSSE';
import { toast } from 'sonner';
import { useQueryClient } from '@tanstack/react-query';
import { useEffect } from 'react';

export function SSEProvider({ children }: { children: React.ReactNode }) {
  const queryClient = useQueryClient();

  const { isConnected, lastEvent } = useSSE({
    onInterviewCreated: (data) => {
      toast.success('New interview created', {
        description: `${data.candidateName} - ${data.position}`,
      });
      // Invalidate interviews query to refetch data
      queryClient.invalidateQueries({ queryKey: ['interviews'] });
    },
    onInterviewScheduled: (data) => {
      toast.info('Interview scheduled', {
        description: `${data.candidateName} - Calendar invites sent`,
      });
      // Invalidate interviews query to refetch data
      queryClient.invalidateQueries({ queryKey: ['interviews'] });
    },
    onInterviewUpdated: (data) => {
      toast.info('Interview updated', {
        description: `${data.candidateName} - Status: ${data.status}`,
      });
      // Invalidate interviews query to refetch data
      queryClient.invalidateQueries({ queryKey: ['interviews'] });
      // Also invalidate specific interview if viewing details
      if (data.id) {
        queryClient.invalidateQueries({ queryKey: ['interview', data.id] });
      }
    },
    onEvaluationCreated: (data) => {
      toast.success('New evaluation submitted', {
        description: `Interview evaluation completed`,
      });
      // Invalidate evaluations query
      queryClient.invalidateQueries({ queryKey: ['evaluations'] });
    },
    onEvaluationUpdated: (data) => {
      toast.info('Evaluation updated', {
        description: `Evaluation status changed`,
      });
      // Invalidate evaluations query
      queryClient.invalidateQueries({ queryKey: ['evaluations'] });
    },
    onReportGenerated: (data) => {
      toast.success('Report generated', {
        description: `New report is available`,
      });
      // Invalidate reports query
      queryClient.invalidateQueries({ queryKey: ['reports'] });
    },
    onNotificationSent: (data) => {
      toast.info('Notification sent', {
        description: data.message || 'Notification delivered successfully',
      });
    },
  });

  // Show connection status on mount
  useEffect(() => {
    if (isConnected) {
      console.log('[SSE Provider] Connected to real-time updates');
    }
  }, [isConnected]);

  return <>{children}</>;
}
