"use client";

import { useEffect, useRef, useState, useCallback } from 'react';

export interface SSEEvent {
  type: string;
  data: Record<string, any>;
  timestamp: string;
}

export interface UseSSEOptions {
  onEvent?: (event: SSEEvent) => void;
  onInterviewCreated?: (data: any) => void;
  onInterviewScheduled?: (data: any) => void;
  onInterviewUpdated?: (data: any) => void;
  onEvaluationCreated?: (data: any) => void;
  onEvaluationUpdated?: (data: any) => void;
  onReportGenerated?: (data: any) => void;
  onNotificationSent?: (data: any) => void;
  reconnectDelay?: number;
}

export function useSSE(options: UseSSEOptions = {}) {
  const [isConnected, setIsConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState<SSEEvent | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);
  const optionsRef = useRef(options);

  // Keep options ref updated
  useEffect(() => {
    optionsRef.current = options;
  }, [options]);

  const reconnectDelay = options.reconnectDelay || 3000;

  const connect = useCallback(() => {
    if (eventSourceRef.current) {
      return; // Already connected
    }

    const eventSource = new EventSource('http://localhost:8080/api/events');
    eventSourceRef.current = eventSource;

    eventSource.onopen = () => {
      console.log('[SSE] Connected to server');
      setIsConnected(true);
    };

    eventSource.addEventListener('connected', (e) => {
      const data = JSON.parse(e.data);
      console.log('[SSE] Connection established:', data.clientId);
    });

    eventSource.addEventListener('interview_created', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Interview created:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onInterviewCreated?.(event.data);
    });

    eventSource.addEventListener('interview_scheduled', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Interview scheduled:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onInterviewScheduled?.(event.data);
    });

    eventSource.addEventListener('interview_updated', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Interview updated:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onInterviewUpdated?.(event.data);
    });

    eventSource.addEventListener('evaluation_created', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Evaluation created:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onEvaluationCreated?.(event.data);
    });

    eventSource.addEventListener('evaluation_updated', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Evaluation updated:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onEvaluationUpdated?.(event.data);
    });

    eventSource.addEventListener('report_generated', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Report generated:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onReportGenerated?.(event.data);
    });

    eventSource.addEventListener('notification_sent', (e) => {
      const event: SSEEvent = JSON.parse(e.data);
      console.log('[SSE] Notification sent:', event);
      setLastEvent(event);
      optionsRef.current.onEvent?.(event);
      optionsRef.current.onNotificationSent?.(event.data);
    });

    eventSource.onerror = (error) => {
      // Only log and reconnect if this is an actual error (not a normal close)
      if (eventSource.readyState === EventSource.CLOSED) {
        console.log('[SSE] Connection closed, attempting to reconnect...');
        setIsConnected(false);
        eventSourceRef.current = null;

        // Attempt to reconnect after delay
        reconnectTimeoutRef.current = setTimeout(() => {
          console.log('[SSE] Reconnecting...');
          connect();
        }, reconnectDelay);
      } else if (eventSource.readyState === EventSource.CONNECTING) {
        console.log('[SSE] Connecting...');
      } else {
        console.error('[SSE] Connection error:', error);
      }
    };
  }, [reconnectDelay]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    setIsConnected(false);
  }, []);

  useEffect(() => {
    connect();

    return () => {
      disconnect();
    };
  }, []); // Empty deps - connect once on mount

  return {
    isConnected,
    lastEvent,
    disconnect,
    reconnect: () => {
      disconnect();
      setTimeout(connect, 100);
    },
  };
}
