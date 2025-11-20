/**
 * Tool execution handlers
 * 
 * Implements the actual logic for each MCP tool
 */

import { ChronoQueueClient } from '../chronoqueue-client.js';
import { QueueType } from '../types/chronoqueue.js';
import { parseDuration } from '../config.js';

/**
 * Route tool calls to appropriate handlers
 */
export async function handleToolCall(
    toolName: string,
    args: any,
    client: ChronoQueueClient
): Promise<string> {
    switch (toolName) {
        // Queue Management
        case 'create_queue':
            return handleCreateQueue(args, client);
        case 'delete_queue':
            return handleDeleteQueue(args, client);
        case 'list_queues':
            return handleListQueues(args, client);
        case 'get_queue_state':
            return handleGetQueueState(args, client);

        // Message Operations
        case 'post_message':
            return handlePostMessage(args, client);
        case 'get_next_message':
            return handleGetNextMessage(args, client);
        case 'peek_messages':
            return handlePeekMessages(args, client);
        case 'acknowledge_message':
            return handleAcknowledgeMessage(args, client);
        case 'renew_message_lease':
            return handleRenewMessageLease(args, client);

        // Scheduling
        case 'create_schedule':
            return handleCreateSchedule(args, client);
        case 'list_schedules':
            return handleListSchedules(args, client);
        case 'delete_schedule':
            return handleDeleteSchedule(args, client);

        // Schema Management
        case 'register_schema':
            return handleRegisterSchema(args, client);

        default:
            throw new Error(`Unknown tool: ${toolName}`);
    }
}

// Queue Management Handlers

async function handleCreateQueue(args: any, client: ChronoQueueClient): Promise<string> {
    const queueName = args.queue_name || args.name;
    if (!queueName) {
        throw new Error('Queue name is required');
    }

    const queueType = args.queue_type === 'exclusive' ? QueueType.EXCLUSIVE : QueueType.SIMPLE;

    // Parse lease_duration if provided
    let leaseDuration: string | undefined;
    if (args.lease_duration) {
        leaseDuration = args.lease_duration;
    }

    await client.createQueue(queueName, {
        type: queueType,
        dequeueAttempts: args.max_attempts || 3,
        leaseDuration: leaseDuration,
        deadLetterQueueName: args.dlq_name,
        autoCreateDLQ: args.auto_create_dlq !== undefined ? args.auto_create_dlq : true,
        exclusivityKey: args.exclusivity_key,
    });

    let result = `✓ Queue created successfully

Queue: ${queueName}
Type: ${args.queue_type || 'simple'}
Lease Duration: ${args.lease_duration || '30s'}
Max Attempts: ${args.max_attempts || 3}
Auto-create DLQ: ${args.auto_create_dlq !== false ? 'Yes' : 'No'}`;

    if (args.exclusivity_key) {
        result += `\nExclusivity Key: ${args.exclusivity_key}`;
    }

    return result;
}

async function handleDeleteQueue(args: any, client: ChronoQueueClient): Promise<string> {
    await client.deleteQueue(args.queue_name);
    return `✓ Queue '${args.queue_name}' deleted successfully`;
}

async function handleListQueues(args: any, client: ChronoQueueClient): Promise<string> {
    const queues = await client.listQueues();

    if (queues.length === 0) {
        return 'No queues found';
    }

    let result = `📋 Queues (${queues.length} total)\n\n`;
    for (const queue of queues) {
        result += `• ${queue.queueName}\n`;
        result += `  Type: ${queue.queueType === QueueType.EXCLUSIVE ? 'Exclusive' : 'Simple'}\n`;
        result += `  Lease: ${queue.leaseDuration}s\n`;
        result += `  Max Attempts: ${queue.maxAttempts}\n`;
        if (queue.deadLetterQueueName) {
            result += `  DLQ: ${queue.deadLetterQueueName}\n`;
        }
        result += '\n';
    }

    return result;
}

async function handleGetQueueState(args: any, client: ChronoQueueClient): Promise<string> {
    const state = await client.getQueueState(args.queue_name);
    const queueTypeStr = state.queueType === QueueType.EXCLUSIVE ? 'Exclusive' : 'Simple';

    return `📊 Queue State: ${state.queueName}

Status: ${state.status}
Type: ${queueTypeStr}

Messages:
  • Pending:   ${state.pending}
  • Running:   ${state.running}
  • Completed: ${state.completed}
  • Errored:   ${state.errored}

Configuration:
  • Lease Duration: ${state.leaseDuration}s
  • Max Attempts: ${state.maxAttempts}
  • DLQ: ${state.deadLetterQueueName || 'None'}

${state.throughput ? `Throughput: ${state.throughput} msg/min` : ''}
${state.avgProcessingTime ? `Avg Processing Time: ${state.avgProcessingTime}` : ''}`;
}

// Message Operation Handlers

async function handlePostMessage(args: any, client: ChronoQueueClient): Promise<string> {
    // Parse lease_duration from string (e.g., "5m") to seconds
    let leaseDurationSeconds: number | undefined;
    if (args.lease_duration) {
        leaseDurationSeconds = Math.floor(parseDuration(args.lease_duration) / 1000);
    }

    const response = await client.postMessage(
        args.queue_name,
        args.message_id,
        args.payload,
        {
            priority: args.priority,
            leaseDuration: leaseDurationSeconds,
            schemaId: args.schema_id,
            schemaVersion: args.schema_version,
        }
    );

    return `✓ Message posted successfully

Queue: ${args.queue_name}
Message ID: ${args.message_id}
Priority: ${args.priority || 5}
${args.schema_id ? `Schema: ${args.schema_id}${args.schema_version ? ` v${args.schema_version}` : ''}` : ''}
Stream Entry ID: ${response.streamEntryId}`;
}

async function handleGetNextMessage(args: any, client: ChronoQueueClient): Promise<string> {
    const response = await client.getNextMessage(args.queue_name);

    if (!response) {
        return `No messages available in queue '${args.queue_name}'`;
    }

    const { message } = response;
    return `📨 Message Retrieved

Queue: ${args.queue_name}
Message ID: ${message.messageId}
Priority: ${message.priority}
Attempts: ${message.attempts}/${message.maxAttempts}
Stream Entry ID: ${response.streamEntryId}
Leased Until: ${message.leasedUntil || 'N/A'}

Payload:
${JSON.stringify(message.payload, null, 2)}

⚠️  Remember to acknowledge this message after processing using:
   acknowledge_message with stream_entry_id: ${response.streamEntryId}`;
}

async function handlePeekMessages(args: any, client: ChronoQueueClient): Promise<string> {
    const limit = args.limit || 10;
    const messages = await client.peekMessages(args.queue_name, limit);

    if (messages.length === 0) {
        return `No messages in queue '${args.queue_name}'`;
    }

    let result = `👀 Peeking at ${messages.length} message(s) from '${args.queue_name}'\n\n`;

    for (let i = 0; i < messages.length; i++) {
        const { message } = messages[i];
        result += `${i + 1}. Message ID: ${message.messageId}\n`;
        result += `   Priority: ${message.priority}\n`;
        result += `   Attempts: ${message.attempts}/${message.maxAttempts}\n`;
        result += `   Payload: ${JSON.stringify(message.payload).substring(0, 100)}...\n\n`;
    }

    return result;
}

async function handleAcknowledgeMessage(args: any, client: ChronoQueueClient): Promise<string> {
    await client.acknowledgeMessage(
        args.queue_name,
        args.message_id,
        args.status,
        args.stream_entry_id
    );

    const statusEmoji = args.status === 'completed' ? '✅' : '❌';
    return `${statusEmoji} Message acknowledged as ${args.status}

Queue: ${args.queue_name}
Message ID: ${args.message_id}
Status: ${args.status}`;
}

async function handleRenewMessageLease(args: any, client: ChronoQueueClient): Promise<string> {
    // Parse lease_duration from string to seconds if provided
    let leaseDurationSeconds: number | undefined;
    if (args.lease_duration) {
        leaseDurationSeconds = Math.floor(parseDuration(args.lease_duration) / 1000);
    }

    const response = await client.renewMessageLease(
        args.queue_name,
        args.message_id,
        args.stream_entry_id,
        leaseDurationSeconds
    );

    return `✓ Message lease renewed

Queue: ${args.queue_name}
Message ID: ${args.message_id}
New Lease Expiry: ${response.newLeaseExpiry}`;
}

// Schedule Handlers

async function handleCreateSchedule(args: any, client: ChronoQueueClient): Promise<string> {
    if (!args.schedule_type) {
        throw new Error('schedule_type is required');
    }
    const schedule: any = {
        scheduleId: args.schedule_id,
        queueName: args.queue_name,
        scheduleType: args.schedule_type.toUpperCase(),
        payloadData: args.payload,
        priority: args.priority || 5,
        enabled: args.enabled !== false,
    };

    if (args.schedule_type === 'cron') {
        if (!args.cron_expression) {
            throw new Error('cron_expression is required for cron schedules');
        }
        schedule.cronSchedule = {
            cronExpression: args.cron_expression,
        };
    } else if (args.schedule_type === 'calendar') {
        if (!args.calendar_type || !args.times_of_day) {
            throw new Error('calendar_type and times_of_day are required for calendar schedules');
        }
        schedule.calendarSchedule = {
            calendarType: args.calendar_type.toUpperCase(),
            timesOfDay: args.times_of_day,
            daysOfWeek: args.days_of_week,
            skipHolidays: args.skip_holidays || false,
            timezone: args.timezone || 'UTC',
        };
    }

    await client.createSchedule(schedule);

    let scheduleInfo = '';
    if (args.schedule_type === 'cron') {
        scheduleInfo = `Cron: ${args.cron_expression}`;
    } else {
        scheduleInfo = `Calendar: ${args.calendar_type}, Times: ${args.times_of_day.join(', ')}`;
    }

    return `✓ Schedule created successfully

Schedule ID: ${args.schedule_id}
Queue: ${args.queue_name}
Type: ${args.schedule_type}
${scheduleInfo}
Enabled: ${args.enabled !== false ? 'Yes' : 'No'}`;
}

async function handleListSchedules(args: any, client: ChronoQueueClient): Promise<string> {
    const schedules = await client.listSchedules();

    if (schedules.length === 0) {
        return 'No schedules found';
    }

    let result = `📅 Schedules (${schedules.length} total)\n\n`;

    for (const schedule of schedules) {
        result += `• ${schedule.scheduleId}\n`;
        result += `  Queue: ${schedule.queueName}\n`;
        result += `  Type: ${schedule.scheduleType}\n`;

        if (schedule.cronSchedule) {
            result += `  Cron: ${schedule.cronSchedule.cronExpression}\n`;
        } else if (schedule.calendarSchedule) {
            result += `  Calendar: ${schedule.calendarSchedule.calendarType}\n`;
            result += `  Times: ${schedule.calendarSchedule.timesOfDay.join(', ')}\n`;
        }

        result += `  Enabled: ${schedule.enabled ? 'Yes' : 'No'}\n`;
        result += `  Created: ${schedule.createdAt}\n\n`;
    }

    return result;
}

async function handleDeleteSchedule(args: any, client: ChronoQueueClient): Promise<string> {
    await client.deleteSchedule(args.schedule_id);
    return `✓ Schedule '${args.schedule_id}' deleted successfully`;
}

// Schema Management Handlers

async function handleRegisterSchema(args: any, client: ChronoQueueClient): Promise<string> {
    const response = await client.registerSchema(
        args.schema_id,
        args.name,
        args.content,
        args.description,
        args.content_type
    );

    return `✓ Schema registered successfully

Schema ID: ${response.schemaId}
Version: ${response.version}
Created: ${response.createdAt}`;
}

