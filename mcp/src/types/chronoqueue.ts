/**
 * TypeScript type definitions for ChronoQueue operations
 */

// Queue Types
export enum QueueType {
    SIMPLE = 0,
    EXCLUSIVE = 1,
}

export enum QueueStatus {
    ACTIVE = 'ACTIVE',
    PAUSED = 'PAUSED',
    DELETED = 'DELETED',
}

// Queue Interfaces
export interface QueueOptions {
    type?: QueueType;
    dequeueAttempts?: number;
    leaseDuration?: string;
    invisibilityDuration?: string;
    exclusivityKey?: string;
    deadLetterQueueName?: string;
    autoCreateDLQ?: boolean;
}

export interface QueueInfo {
    queueName: string;
    queueType: QueueType;
    status: QueueStatus;
    leaseDuration: number; // Duration in seconds
    maxAttempts: number;
    deadLetterQueueName?: string;
}

export interface QueueState {
    queueName: string;
    status: QueueStatus;
    queueType: QueueType;
    pending: number;
    running: number;
    completed: number;
    errored: number;
    leaseDuration: number; // Duration in seconds
    maxAttempts: number;
    deadLetterQueueName?: string;
    throughput?: number;
    avgProcessingTime?: string;
}

// Message Interfaces
export interface MessagePayload {
    [key: string]: any;
}

export interface MessageOptions {
    priority?: number;
    leaseDuration?: number; // Duration in seconds
    maxAttempts?: number;
    contentType?: string;
    scheduledTime?: {
        seconds: number;
        nanos: number;
    };
    metadata?: Record<string, any>;
    schemaId?: string;
    schemaVersion?: number;
}

export interface Message {
    messageId: string;
    queueName: string;
    payload: MessagePayload;
    priority: number;
    attempts: number;
    maxAttempts: number;
    createdAt: string;
    leasedUntil?: string;
}

export interface PostMessageResponse {
    success: boolean;
    messageId: string;
}

export interface GetMessageResponse {
    message: Message;
}

export interface AcknowledgeMessageResponse {
    success: boolean;
    message: string;
}

// Schedule Interfaces
export enum ScheduleType {
    CRON = 'CRON',
    CALENDAR = 'CALENDAR',
}

export enum CalendarType {
    ONCE = 'ONCE',
    WEEKLY = 'WEEKLY',
    DAILY = 'DAILY',
    BUSINESS_DAYS = 'BUSINESS_DAYS',
}

export interface CronSchedule {
    cronExpression: string;
}

export interface CalendarSchedule {
    calendarType: CalendarType;
    timesOfDay: string[];
    daysOfWeek?: number[];
    skipHolidays?: boolean;
    timezone?: string;
}

export interface Schedule {
    scheduleId: string;
    queueName: string;
    scheduleType: ScheduleType;
    cronSchedule?: CronSchedule;
    calendarSchedule?: CalendarSchedule;
    payloadData: MessagePayload;
    priority?: number;
    enabled: boolean;
    createdAt: string;
}

export interface CreateScheduleResponse {
    success: boolean;
    scheduleId: string;
}

// Error Types
export interface ChronoQueueError {
    code: string;
    message: string;
    details?: any;
}
