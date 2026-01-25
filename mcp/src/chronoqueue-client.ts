/**
 * ChronoQueue gRPC Client Wrapper
 * 
 * Provides a typed interface to interact with ChronoQueue server via gRPC
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { promisify } from 'util';
import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import type {
    QueueOptions,
    QueueInfo,
    QueueState,
    MessageOptions,
    MessagePayload,
    PostMessageResponse,
    GetMessageResponse,
    AcknowledgeMessageResponse,
    Schedule,
    CreateScheduleResponse,
} from './types/chronoqueue.js';
import { QueueStatus, QueueType } from './types/chronoqueue.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Helper function to convert protobuf Struct to plain JavaScript object
 */
function structToObject(struct: any): any {
    if (!struct || !struct.fields) {
        return {};
    }

    const result: any = {};
    for (const [key, value] of Object.entries(struct.fields)) {
        result[key] = valueToJs(value);
    }
    return result;
}

function valueToJs(value: any): any {
    if (!value) return null;

    if (value.nullValue !== undefined) return null;
    if (value.numberValue !== undefined) return value.numberValue;
    if (value.stringValue !== undefined) return value.stringValue;
    if (value.boolValue !== undefined) return value.boolValue;
    if (value.structValue !== undefined) return structToObject(value.structValue);
    if (value.listValue !== undefined) {
        return (value.listValue.values || []).map(valueToJs);
    }

    return null;
}

/**
 * Helper function to convert JavaScript object to protobuf Struct
 */
function objectToStruct(obj: any): any {
    if (obj === null || obj === undefined) {
        return { fields: {} };
    }

    const fields: any = {};
    for (const [key, value] of Object.entries(obj)) {
        fields[key] = jsToValue(value);
    }

    return { fields };
}

/**
 * Helper function to convert JavaScript value to protobuf Value
 */
function jsToValue(value: any): any {
    if (value === null || value === undefined) {
        return { nullValue: 0 };
    }

    if (typeof value === 'number') {
        return { numberValue: value };
    }

    if (typeof value === 'string') {
        return { stringValue: value };
    }

    if (typeof value === 'boolean') {
        return { boolValue: value };
    }

    if (Array.isArray(value)) {
        return {
            listValue: {
                values: value.map(jsToValue)
            }
        };
    }

    if (typeof value === 'object') {
        return { structValue: objectToStruct(value) };
    }

    return { nullValue: 0 };
}

export class ChronoQueueClient {
    private queueServiceClient: any;
    private readonly timeout: number;

    constructor(
        address: string,
        options: {
            insecure?: boolean;
            certPath?: string;
            keyPath?: string;
            caPath?: string;
            timeout?: number;
        } = {}
    ) {
        const { insecure = true, certPath, keyPath, caPath, timeout = 30000 } = options;
        this.timeout = timeout;

        // Load proto files - includeDirs must be workspace root for proto imports to resolve
        const workspaceRoot = join(__dirname, '../..');
        const protoPath = 'proto/queueservice/v1/service.proto';

        const packageDefinition = protoLoader.loadSync(protoPath, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
            includeDirs: [workspaceRoot],
        });

        const queuePackage = grpc.loadPackageDefinition(packageDefinition) as any;

        // Setup credentials
        let credentials: grpc.ChannelCredentials;
        if (insecure) {
            credentials = grpc.credentials.createInsecure();
        } else if (certPath && keyPath && caPath) {
            const cert = readFileSync(certPath);
            const key = readFileSync(keyPath);
            const ca = readFileSync(caPath);
            credentials = grpc.credentials.createSsl(ca, key, cert);
        } else {
            credentials = grpc.credentials.createSsl();
        }

        // Initialize client - package path: chronoqueue.api.queueservice.v1.QueueService
        this.queueServiceClient = new queuePackage.chronoqueue.api.queueservice.v1.QueueService(
            address,
            credentials
        );
    }

    /**
     * Create a new queue
     */
    async createQueue(queueName: string, options: QueueOptions = {}): Promise<{ success: boolean }> {
        const createQueueAsync = promisify(
            this.queueServiceClient.createQueue.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const grpcMetadata = new grpc.Metadata();

        // Parse lease duration from string (e.g., "30s") to seconds
        let leaseDurationSeconds = 30; // Default 30 seconds
        if (options.leaseDuration) {
            const match = options.leaseDuration.match(/^(\d+)(s|m|h)$/);
            if (match) {
                const value = parseInt(match[1], 10);
                const unit = match[2];
                switch (unit) {
                    case 's':
                        leaseDurationSeconds = value;
                        break;
                    case 'm':
                        leaseDurationSeconds = value * 60;
                        break;
                    case 'h':
                        leaseDurationSeconds = value * 60 * 60;
                        break;
                }
            }
        }

        const metadata: any = {
            type: options.type || 0, // 0 = SIMPLE, 1 = EXCLUSIVE
            default_max_attempts: options.dequeueAttempts || 3,
            lease_duration: { seconds: leaseDurationSeconds, nanos: 0 },
            dead_letter_queue_name: options.deadLetterQueueName || `${queueName}-dlq`,
            auto_create_dlq: options.autoCreateDLQ !== undefined ? options.autoCreateDLQ : true,
        };

        if (options.exclusivityKey) {
            metadata.exclusivity_key = options.exclusivityKey;
        }

        const request = {
            name: queueName,
            metadata: metadata,
        };

        const response = await createQueueAsync(request, grpcMetadata, { deadline });
        return { success: response.success };
    }

    /**
     * Delete a queue
     */
    async deleteQueue(queueName: string): Promise<{ success: boolean }> {
        const deleteQueueAsync = promisify(
            this.queueServiceClient.deleteQueue.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await deleteQueueAsync({ name: queueName }, metadata, { deadline });
        return { success: response.success };
    }

    /**
     * List all queues
     */
    async listQueues(): Promise<QueueInfo[]> {
        const listQueuesAsync = promisify(
            this.queueServiceClient.listQueues.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await listQueuesAsync({}, metadata, { deadline });
        return (response.queues || []).map((q: any) => {
            const type = q.metadata?.type;
            const isExclusive = type === 1 || type === 'EXCLUSIVE' || type === QueueType.EXCLUSIVE;
            return {
                queueName: q.name,
                queueType: isExclusive ? QueueType.EXCLUSIVE : QueueType.SIMPLE,
                status: QueueStatus.ACTIVE,
                leaseDuration: q.metadata?.lease_duration?.seconds || 30,
                maxAttempts: q.metadata?.default_max_attempts || 3,
                deadLetterQueueName: q.metadata?.dead_letter_queue_name,
            };
        });
    }

    /**
     * Get queue state and statistics
     * 
     * Note: The server's getQueueState response does not include queue configuration
     * (queueType, leaseDuration, maxAttempts, deadLetterQueueName).
     * To get accurate queue metadata, use listQueues() and filter by queue name.
     */
    async getQueueState(queueName: string): Promise<QueueState> {
        const getQueueStateAsync = promisify(
            this.queueServiceClient.getQueueState.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await getQueueStateAsync({ queue_name: queueName }, metadata, { deadline });
        const stateCounts = response.state_counts || {};

        return {
            queueName: queueName,
            status: QueueStatus.ACTIVE,
            queueType: QueueType.SIMPLE, // Not provided by server - use listQueues() for accurate value
            pending: stateCounts['PENDING'] || 0,
            running: stateCounts['RUNNING'] || 0,
            completed: stateCounts['COMPLETED'] || 0,
            errored: stateCounts['ERRORED'] || 0,
            leaseDuration: 30, // Not provided by server (seconds) - use listQueues() for accurate value
            maxAttempts: 3, // Not provided by server - use listQueues() for accurate value
            deadLetterQueueName: undefined, // Not provided by server - use listQueues() for accurate value
            throughput: response.throughput,
            avgProcessingTime: response.avg_processing_time,
        };
    }

    /**
     * Post a message to a queue
     */
    async postMessage(
        queueName: string,
        messageId: string,
        payload: MessagePayload,
        options: MessageOptions = {}
    ): Promise<PostMessageResponse> {
        const postMessageAsync = promisify(
            this.queueServiceClient.postMessage.bind(this.queueServiceClient)
        );

        // Parse payload if it's a string
        let payloadData = payload;
        if (typeof payload === 'string') {
            try {
                payloadData = JSON.parse(payload);
            } catch (e) {
                payloadData = { data: payload };
            }
        }

        const payloadObj: any = {
            data: objectToStruct(payloadData),
            content_type: options.contentType || 'application/json',
            metadata: options.metadata || {},
        };

        if (options.schemaId) {
            payloadObj.schema_id = options.schemaId;
        }

        if (options.schemaVersion !== undefined) {
            payloadObj.schema_version = options.schemaVersion;
        }

        const metadata: any = {
            payload: payloadObj,
            priority: options.priority || 5,
        };

        if (options.maxAttempts !== undefined) {
            metadata.max_attempts = options.maxAttempts;
        }

        if (options.leaseDuration) {
            metadata.lease_duration = {
                seconds: options.leaseDuration,
                nanos: 0
            };
        }

        if (options.scheduledTime) {
            metadata.scheduled_time = options.scheduledTime;
        }

        const request = {
            queue_name: queueName,
            message: {
                message_id: messageId,
                metadata: metadata,
            },
        };

        const deadline = new Date(Date.now() + this.timeout);
        const grpcMetadata = new grpc.Metadata();

        const response = await postMessageAsync(request, grpcMetadata, { deadline });
        return {
            success: response.success,
            messageId: messageId,
        };
    }

    /**
     * Get next message from queue
     */
    async getNextMessage(queueName: string): Promise<GetMessageResponse | null> {
        const getNextMessageAsync = promisify(
            this.queueServiceClient.getNextMessage.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const grpcMetadata = new grpc.Metadata();

        try {
            const response = await getNextMessageAsync({ queue_name: queueName }, grpcMetadata, { deadline });
            if (!response.message) {
                return null;
            }

            const msg = response.message;
            const msgMetadata = msg.metadata || {};
            const payload = msgMetadata.payload || {};
            const payloadData = structToObject(payload.data);

            return {
                message: {
                    messageId: msg.message_id,
                    queueName: queueName,
                    payload: payloadData,
                    priority: msgMetadata.priority || 0,
                    attempts: msgMetadata.attempts_left || 0,
                    maxAttempts: msgMetadata.max_attempts || 0,
                    createdAt: '',
                    leasedUntil: msgMetadata.lease_expiry || '',
                },
            };
        } catch (error: any) {
            if (error.code === grpc.status.NOT_FOUND) {
                return null;
            }
            throw error;
        }
    }

    /**
     * Peek at messages without consuming them
     */
    async peekMessages(queueName: string, limit: number = 10): Promise<GetMessageResponse[]> {
        const peekMessagesAsync = promisify(
            this.queueServiceClient.peekQueueMessages.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await peekMessagesAsync({
            queue_name: queueName,
            limit: limit,
        }, metadata, { deadline });

        return (response.messages || []).map((msg: any) => {
            const metadata = msg.metadata || {};
            const payload = metadata.payload || {};
            const payloadData = structToObject(payload.data);

            return {
                message: {
                    messageId: msg.message_id,
                    queueName: queueName,
                    payload: payloadData,
                    priority: metadata.priority || 0,
                    attempts: metadata.attempts_left || 0,
                    maxAttempts: metadata.max_attempts || 0,
                    createdAt: '',
                    leasedUntil: metadata.lease_expiry || '',
                },
            };
        });
    }

    /**
     * Acknowledge message processing
     */
    async acknowledgeMessage(
        queueName: string,
        messageId: string,
        status: 'completed' | 'errored'
    ): Promise<AcknowledgeMessageResponse> {
        const ackMessageAsync = promisify(
            this.queueServiceClient.acknowledgeMessage.bind(this.queueServiceClient)
        );

        // State enum values from message.proto:
        // With enums: String in protoLoader options, we send string names not integers
        // INVISIBLE = 0, PENDING = 1, RUNNING = 2, COMPLETED = 3, CANCELED = 4, ERRORED = 5
        const stateMap = { completed: 'COMPLETED', errored: 'ERRORED' };

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await ackMessageAsync({
            queue_name: queueName,
            message_id: messageId,
            state: stateMap[status],
        }, metadata, { deadline });

        return {
            success: response.success,
            message: response.message || 'Message acknowledged',
        };
    }

    /**
     * Parse calendar schedule from server response
     */
    private parseCalendarSchedule(calendarSchedule: any): any {
        const typeMap: Record<number | string, string> = {
            0: 'MONTHLY',
            1: 'WEEKLY',
            2: 'DAILY',
            3: 'YEARLY',
            4: 'BUSINESS_DAYS',
            5: 'CUSTOM',
            'MONTHLY': 'MONTHLY',
            'WEEKLY': 'WEEKLY',
            'DAILY': 'DAILY',
            'YEARLY': 'YEARLY',
            'BUSINESS_DAYS': 'BUSINESS_DAYS',
            'CUSTOM': 'CUSTOM',
        };

        const calType = typeMap[calendarSchedule.type] || 'WEEKLY';

        // Extract times and days from rules
        const rules = calendarSchedule.rules || [];
        const firstRule = rules[0] || {};

        // Format execution times as "HH:MM" strings
        const timesOfDay = (firstRule.execution_times || firstRule.executionTimes || []).map((t: any) => {
            const hour = String(t.hour || 0).padStart(2, '0');
            const minute = String(t.minute || 0).padStart(2, '0');
            return `${hour}:${minute}`;
        });

        // Extract days of week from weekly rule
        let daysOfWeek: number[] = [];
        if (firstRule.weekly) {
            daysOfWeek = firstRule.weekly.days_of_week || firstRule.weekly.daysOfWeek || [];
        }

        return {
            calendarType: calType,
            timesOfDay: timesOfDay,
            daysOfWeek: daysOfWeek,
            skipHolidays: false,
            timezone: calendarSchedule.timezone || 'UTC',
        };
    }

    /**
     * Renew message lease
     */
    async renewMessageLease(
        queueName: string,
        messageId: string,
        leaseDuration?: number // Duration in seconds
    ): Promise<{ success: boolean; newLeaseExpiry: string }> {
        const renewLeaseAsync = promisify(
            this.queueServiceClient.renewMessageLease.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const leaseDurationProto = leaseDuration ? { seconds: leaseDuration, nanos: 0 } : undefined;

        const response = await renewLeaseAsync({
            queue_name: queueName,
            message_id: messageId,
            lease_duration: leaseDurationProto,
        }, metadata, { deadline });

        return {
            success: response.success,
            newLeaseExpiry: response.new_lease_expiry,
        };
    }

    /**
     * Create a schedule
     */
    async createSchedule(schedule: Partial<Schedule>): Promise<CreateScheduleResponse> {
        const createScheduleAsync = promisify(
            this.queueServiceClient.createSchedule.bind(this.queueServiceClient)
        );

        const metadata: any = {
            payload: {
                data: objectToStruct(schedule.payloadData),
                content_type: 'application/json',
                metadata: {},
            },
            queue_name: schedule.queueName,
        };

        // Set the oneof schedule_config field directly on metadata
        // For protobuf oneof fields, we set the specific field directly, not wrapped in an object
        if (schedule.scheduleType === 'CRON' && schedule.cronSchedule) {
            metadata.cron_schedule = schedule.cronSchedule.cronExpression;
        } else if (schedule.scheduleType === 'CALENDAR' && schedule.calendarSchedule) {
            // Parse times_of_day from "HH:MM" format to {hour, minute} objects
            const executionTimes = schedule.calendarSchedule.timesOfDay?.map((time: string) => {
                const [hour, minute] = time.split(':').map(Number);
                return { hour, minute: minute || 0 };
            }) || [{ hour: 0, minute: 0 }];

            // Map calendar type string to enum value
            const typeMap: Record<string, number> = {
                'MONTHLY': 0,
                'WEEKLY': 1,
                'DAILY': 2,
                'YEARLY': 3,
                'BUSINESS_DAYS': 4,
                'CUSTOM': 5,
            };

            // Build the rule based on calendar type
            const rule: any = {
                execution_times: executionTimes,
            };

            const calType = schedule.calendarSchedule.calendarType;
            if (calType === 'WEEKLY') {
                rule.weekly = {
                    days_of_week: schedule.calendarSchedule.daysOfWeek || [1, 2, 3, 4, 5],
                };
            } else if (calType === 'DAILY') {
                rule.daily = {
                    day_interval: 1,
                    weekdays_only: schedule.calendarSchedule.skipHolidays || false,
                };
            } else if (calType === 'BUSINESS_DAYS') {
                rule.business_days = {
                    business_calendar_id: 'default',
                };
            }

            metadata.calendar_schedule = {
                type: typeMap[calType] || 1,
                rules: [rule],
                timezone: schedule.calendarSchedule.timezone || 'UTC',
            };
        }

        const request = {
            schedule: {
                schedule_id: schedule.scheduleId,
                metadata: metadata,
            },
        };

        const deadline = new Date(Date.now() + this.timeout);
        const grpcMetadata = new grpc.Metadata();

        const response = await createScheduleAsync(request, grpcMetadata, { deadline });
        return {
            success: response.success,
            scheduleId: schedule.scheduleId || '',
        };
    }

    /**
     * List schedules
     */
    async listSchedules(): Promise<Schedule[]> {
        const listSchedulesAsync = promisify(
            this.queueServiceClient.listSchedules.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await listSchedulesAsync({}, metadata, { deadline });
        return (response.schedules || []).map((s: any) => {
            const scheduleMetadata = s.metadata || {};
            const payload = scheduleMetadata.payload || {};
            // The oneof fields cron_schedule and calendar_schedule are directly on metadata
            const cronSchedule = scheduleMetadata.cron_schedule || scheduleMetadata.cronSchedule;
            const calendarSchedule = scheduleMetadata.calendar_schedule || scheduleMetadata.calendarSchedule;

            return {
                scheduleId: s.schedule_id,
                queueName: scheduleMetadata.queue_name || scheduleMetadata.queueName,
                scheduleType: cronSchedule ? 'CRON' : 'CALENDAR',
                cronSchedule: cronSchedule
                    ? { cronExpression: cronSchedule }
                    : undefined,
                calendarSchedule: calendarSchedule
                    ? this.parseCalendarSchedule(calendarSchedule)
                    : undefined,
                payloadData: structToObject(payload.data),
                priority: scheduleMetadata.priority,
                enabled: scheduleMetadata.state === 'SCHEDULED',
                createdAt: scheduleMetadata.created_at || scheduleMetadata.createdAt,
            };
        });
    }

    /**
     * Delete a schedule
     */
    async deleteSchedule(scheduleId: string): Promise<{ success: boolean }> {
        const deleteScheduleAsync = promisify(
            this.queueServiceClient.deleteSchedule.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const metadata = new grpc.Metadata();

        const response = await deleteScheduleAsync({ schedule_id: scheduleId }, metadata, { deadline });
        return { success: response.success };
    }

    /**
     * Register a schema for message validation
     */
    async registerSchema(
        schemaId: string,
        name: string,
        content: string,
        description?: string,
        contentType?: string
    ): Promise<{ schemaId: string; version: number; createdAt: string }> {
        const registerSchemaAsync = promisify(
            this.queueServiceClient.registerSchema.bind(this.queueServiceClient)
        );

        const deadline = new Date(Date.now() + this.timeout);
        const grpcMetadata = new grpc.Metadata();

        const response = await registerSchemaAsync({
            schema_id: schemaId,
            name: name,
            description: description || '',
            content: content,
            content_type: contentType || 'json-schema',
            metadata: {},
        }, grpcMetadata, { deadline });

        return {
            schemaId: response.schema_id,
            version: response.version,
            createdAt: response.created_at?.toString() || '',
        };
    }

    /**
     * Close the client connection
     */
    close(): void {
        if (this.queueServiceClient) {
            this.queueServiceClient.close();
        }
    }
}
