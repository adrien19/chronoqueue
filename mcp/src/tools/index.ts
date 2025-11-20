/**
 * Tool registry - defines all available MCP tools for ChronoQueue
 */

import { Tool } from '@modelcontextprotocol/sdk/types.js';

// Queue Management Tools
export const createQueueTool: Tool = {
    name: 'create_queue',
    description: 'Create a new ChronoQueue queue with specified configuration',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue to create',
            },
            queue_type: {
                type: 'string',
                enum: ['simple', 'exclusive'],
                description: 'Queue type (default: simple)',
            },
            lease_duration: {
                type: 'string',
                description: 'Default message lease duration (e.g., "30s", "5m") (default: "30s")',
                default: '30s',
            },
            max_attempts: {
                type: 'number',
                description: 'Maximum dequeue attempts before moving to DLQ (default: 3)',
                default: 3,
            },
            auto_create_dlq: {
                type: 'boolean',
                description: 'Automatically create dead letter queue (default: true)',
                default: true,
            },
            dlq_name: {
                type: 'string',
                description: 'Custom dead letter queue name',
            },
            exclusivity_key: {
                type: 'string',
                description: 'Exclusivity key for exclusive queues (required for exclusive queue type)',
            },
        },
        required: ['queue_name'],
    },
};

export const deleteQueueTool: Tool = {
    name: 'delete_queue',
    description: 'Delete an existing queue',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue to delete',
            },
        },
        required: ['queue_name'],
    },
};

export const listQueuesTool: Tool = {
    name: 'list_queues',
    description: 'List all queues in the system',
    inputSchema: {
        type: 'object',
        properties: {},
    },
};

export const getQueueStateTool: Tool = {
    name: 'get_queue_state',
    description: 'Get current state and statistics for a queue',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue to inspect',
            },
        },
        required: ['queue_name'],
    },
};

// Message Operation Tools
export const postMessageTool: Tool = {
    name: 'post_message',
    description: 'Post a message to a queue',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue',
            },
            message_id: {
                type: 'string',
                description: 'Unique message identifier',
            },
            payload: {
                type: 'object',
                description: 'Message payload as JSON object',
            },
            priority: {
                type: 'number',
                minimum: 1,
                maximum: 10,
                description: 'Message priority (1-10, default: 5)',
            },
            lease_duration: {
                type: 'string',
                description: 'Override default lease duration (e.g., "5m")',
            },
            schema_id: {
                type: 'string',
                description: 'Schema ID for validation (e.g., "user.profile.v1")',
            },
            schema_version: {
                type: 'number',
                description: 'Schema version number for validation',
            },
        },
        required: ['queue_name', 'message_id', 'payload'],
    },
};

export const getNextMessageTool: Tool = {
    name: 'get_next_message',
    description: 'Retrieve the next message from a queue for processing',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue',
            },
        },
        required: ['queue_name'],
    },
};

export const peekMessagesTool: Tool = {
    name: 'peek_messages',
    description: 'Preview messages in a queue without consuming them',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue',
            },
            limit: {
                type: 'number',
                description: 'Maximum number of messages to peek (default: 10)',
            },
        },
        required: ['queue_name'],
    },
};

export const acknowledgeMessageTool: Tool = {
    name: 'acknowledge_message',
    description: 'Acknowledge message processing completion or failure',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue',
            },
            message_id: {
                type: 'string',
                description: 'Message identifier',
            },
            status: {
                type: 'string',
                enum: ['completed', 'errored'],
                description: 'Processing status',
            },
            stream_entry_id: {
                type: 'string',
                description: 'Stream entry ID from get_next_message',
            },
        },
        required: ['queue_name', 'message_id', 'status', 'stream_entry_id'],
    },
};

export const renewMessageLeaseTool: Tool = {
    name: 'renew_message_lease',
    description: 'Extend the lease time for a message being processed',
    inputSchema: {
        type: 'object',
        properties: {
            queue_name: {
                type: 'string',
                description: 'Name of the queue',
            },
            message_id: {
                type: 'string',
                description: 'Message identifier',
            },
            stream_entry_id: {
                type: 'string',
                description: 'Stream entry ID',
            },
            lease_duration: {
                type: 'string',
                description: 'New lease duration (e.g., "5m")',
            },
        },
        required: ['queue_name', 'message_id', 'stream_entry_id'],
    },
};

// Schedule Tools
export const createScheduleTool: Tool = {
    name: 'create_schedule',
    description: 'Create a scheduled task that posts messages automatically',
    inputSchema: {
        type: 'object',
        properties: {
            schedule_id: {
                type: 'string',
                description: 'Unique schedule identifier',
            },
            queue_name: {
                type: 'string',
                description: 'Target queue for scheduled messages',
            },
            schedule_type: {
                type: 'string',
                enum: ['cron', 'calendar'],
                description: 'Schedule type',
            },
            cron_expression: {
                type: 'string',
                description: 'Cron expression (required if schedule_type is cron)',
            },
            calendar_type: {
                type: 'string',
                enum: ['once', 'weekly', 'daily', 'business_days'],
                description: 'Calendar type (required if schedule_type is calendar)',
            },
            times_of_day: {
                type: 'array',
                items: { type: 'string' },
                description: 'Times of day in HH:MM format (for calendar schedules)',
            },
            days_of_week: {
                type: 'array',
                items: { type: 'number' },
                description: 'Days of week (1=Mon, 7=Sun) for weekly schedules',
            },
            payload: {
                type: 'object',
                description: 'Message payload to post',
            },
            priority: {
                type: 'number',
                description: 'Message priority (1-10)',
            },
            enabled: {
                type: 'boolean',
                description: 'Whether schedule is enabled (default: true)',
            },
        },
        required: ['schedule_id', 'queue_name', 'schedule_type', 'payload'],
    },
};

export const listSchedulesTool: Tool = {
    name: 'list_schedules',
    description: 'List all schedules in the system',
    inputSchema: {
        type: 'object',
        properties: {},
    },
};

export const deleteScheduleTool: Tool = {
    name: 'delete_schedule',
    description: 'Delete a schedule',
    inputSchema: {
        type: 'object',
        properties: {
            schedule_id: {
                type: 'string',
                description: 'Schedule identifier to delete',
            },
        },
        required: ['schedule_id'],
    },
};

// Schema Management Tools
export const registerSchemaTool: Tool = {
    name: 'register_schema',
    description: 'Register a JSON schema for message validation',
    inputSchema: {
        type: 'object',
        properties: {
            schema_id: {
                type: 'string',
                description: 'Unique schema identifier (e.g., "user.profile.v1")',
            },
            name: {
                type: 'string',
                description: 'Human-readable schema name',
            },
            description: {
                type: 'string',
                description: 'Schema description',
            },
            content: {
                type: 'string',
                description: 'JSON Schema content as a JSON string',
            },
            content_type: {
                type: 'string',
                description: 'Schema type (default: "json-schema")',
            },
        },
        required: ['schema_id', 'name', 'content'],
    },
};

// Export all tools
export const allTools: Tool[] = [
    // Queue management
    createQueueTool,
    deleteQueueTool,
    listQueuesTool,
    getQueueStateTool,
    // Message operations
    postMessageTool,
    getNextMessageTool,
    peekMessagesTool,
    acknowledgeMessageTool,
    renewMessageLeaseTool,
    // Scheduling
    createScheduleTool,
    listSchedulesTool,
    deleteScheduleTool,
    // Schema management
    registerSchemaTool,
];
