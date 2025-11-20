/**
 * Unit tests for tool handlers
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { handleToolCall } from '../tools/handlers';

// Mock the client
vi.mock('../chronoqueue-client');

describe('Tool Handlers', () => {
    let mockClient: any;

    beforeEach(() => {
        mockClient = {
            createQueue: vi.fn(),
            deleteQueue: vi.fn(),
            listQueues: vi.fn(),
            getQueueState: vi.fn(),
            postMessage: vi.fn(),
            getNextMessage: vi.fn(),
            peekMessages: vi.fn(),
            acknowledgeMessage: vi.fn(),
            renewMessageLease: vi.fn(),
            createSchedule: vi.fn(),
            listSchedules: vi.fn(),
            deleteSchedule: vi.fn(),
            registerSchema: vi.fn(),
        };
    });

    describe('Queue Management', () => {
        it('should handle create_queue', async () => {
            mockClient.createQueue.mockResolvedValue({
                success: true,
                queueName: 'test-queue',
            });

            const result = await handleToolCall('create_queue', {
                queue_name: 'test-queue',
                queue_type: 'simple',
                max_attempts: 3,
            }, mockClient);

            expect(mockClient.createQueue).toHaveBeenCalledWith(
                'test-queue',
                expect.objectContaining({
                    type: 0,
                    dequeueAttempts: 3,
                    autoCreateDLQ: true,
                })
            );
            expect(result).toContain('✓ Queue created successfully');
            expect(result).toContain('test-queue');
        });

        it('should handle delete_queue', async () => {
            mockClient.deleteQueue.mockResolvedValue({ success: true });

            const result = await handleToolCall('delete_queue', {
                queue_name: 'test-queue',
            }, mockClient);

            expect(mockClient.deleteQueue).toHaveBeenCalledWith('test-queue');
            expect(result).toContain('deleted successfully');
        });

        it('should handle list_queues with empty result', async () => {
            mockClient.listQueues.mockResolvedValue([]);

            const result = await handleToolCall('list_queues', {}, mockClient);

            expect(result).toContain('No queues found');
        });

        it('should handle list_queues with results', async () => {
            mockClient.listQueues.mockResolvedValue([
                {
                    queueName: 'queue1',
                    queueType: 0,
                    leaseDuration: '30s',
                    maxAttempts: 3,
                },
                {
                    queueName: 'queue2',
                    queueType: 0,
                    leaseDuration: '60s',
                    maxAttempts: 5,
                },
            ]);

            const result = await handleToolCall('list_queues', {}, mockClient);

            expect(result).toContain('📋 Queues (2 total)');
            expect(result).toContain('queue1');
            expect(result).toContain('queue2');
        });

        it('should handle get_queue_state', async () => {
            mockClient.getQueueState.mockResolvedValue({
                queueName: 'test-queue',
                status: 'ACTIVE',
                queueType: 0,
                pending: 5,
                running: 2,
                completed: 10,
                errored: 1,
                leaseDuration: '30s',
                maxAttempts: 3,
            });

            const result = await handleToolCall('get_queue_state', {
                queue_name: 'test-queue',
            }, mockClient);

            expect(mockClient.getQueueState).toHaveBeenCalledWith('test-queue');
            expect(result).toContain('test-queue');
            expect(result).toContain('ACTIVE');
            expect(result).toContain('Pending:   5');
        });
    });

    describe('Message Operations', () => {
        it('should handle post_message', async () => {
            mockClient.postMessage.mockResolvedValue({
                success: true,
                messageId: 'msg-123',
                streamEntryId: '1234-0',
            });

            const result = await handleToolCall('post_message', {
                queue_name: 'test-queue',
                message_id: 'msg-123',
                payload: { key: 'value' },
                priority: 8,
            }, mockClient);

            expect(mockClient.postMessage).toHaveBeenCalledWith(
                'test-queue',
                'msg-123',
                { key: 'value' },
                expect.objectContaining({ priority: 8 })
            );
            expect(result).toContain('✓ Message posted successfully');
        });

        it('should handle post_message with schema validation', async () => {
            mockClient.postMessage.mockResolvedValue({
                success: true,
                messageId: 'msg-123',
                streamEntryId: '1234-0',
            });

            const result = await handleToolCall('post_message', {
                queue_name: 'test-queue',
                message_id: 'msg-123',
                payload: { key: 'value' },
                schema_id: 'user.profile.v1',
                schema_version: 1,
            }, mockClient);

            expect(mockClient.postMessage).toHaveBeenCalledWith(
                'test-queue',
                'msg-123',
                { key: 'value' },
                expect.objectContaining({
                    schemaId: 'user.profile.v1',
                    schemaVersion: 1,
                })
            );
            expect(result).toContain('Schema: user.profile.v1 v1');
        });

        it('should handle get_next_message', async () => {
            mockClient.getNextMessage.mockResolvedValue({
                message: {
                    messageId: 'msg-123',
                    queueName: 'test-queue',
                    payload: { key: 'value' },
                    priority: 5,
                    attempts: 0,
                    maxAttempts: 3,
                    streamEntryId: '1234-0',
                    leasedUntil: '1234567890',
                },
                streamEntryId: '1234-0',
            });

            const result = await handleToolCall('get_next_message', {
                queue_name: 'test-queue',
            }, mockClient);

            expect(mockClient.getNextMessage).toHaveBeenCalledWith('test-queue');
            expect(result).toContain('📨 Message Retrieved');
            expect(result).toContain('msg-123');
        });

        it('should handle get_next_message with no messages', async () => {
            mockClient.getNextMessage.mockResolvedValue(null);

            const result = await handleToolCall('get_next_message', {
                queue_name: 'test-queue',
            }, mockClient);

            expect(result).toContain('No messages available');
        });

        it('should handle peek_messages', async () => {
            mockClient.peekMessages.mockResolvedValue([
                {
                    message: {
                        messageId: 'msg-1',
                        payload: { key: 'value1' },
                        priority: 5,
                        attempts: 0,
                        maxAttempts: 3,
                    },
                },
                {
                    message: {
                        messageId: 'msg-2',
                        payload: { key: 'value2' },
                        priority: 8,
                        attempts: 1,
                        maxAttempts: 3,
                    },
                },
            ]);

            const result = await handleToolCall('peek_messages', {
                queue_name: 'test-queue',
                limit: 5,
            }, mockClient);

            expect(mockClient.peekMessages).toHaveBeenCalledWith('test-queue', 5);
            expect(result).toContain('👀 Peeking at 2 message(s)');
        });

        it('should handle acknowledge_message', async () => {
            mockClient.acknowledgeMessage.mockResolvedValue({
                success: true,
                message: 'Message acknowledged',
            });

            const result = await handleToolCall('acknowledge_message', {
                queue_name: 'test-queue',
                message_id: 'msg-123',
                status: 'completed',
                stream_entry_id: '1234-0',
            }, mockClient);

            expect(mockClient.acknowledgeMessage).toHaveBeenCalledWith(
                'test-queue',
                'msg-123',
                'completed',
                '1234-0'
            );
            expect(result).toContain('✅ Message acknowledged');
        });

        it('should handle renew_message_lease', async () => {
            mockClient.renewMessageLease.mockResolvedValue({
                success: true,
                newLeaseExpiry: '1234567890',
            });

            const result = await handleToolCall('renew_message_lease', {
                queue_name: 'test-queue',
                message_id: 'msg-123',
                stream_entry_id: '1234-0',
                lease_duration: '60s',
            }, mockClient);

            expect(mockClient.renewMessageLease).toHaveBeenCalledWith(
                'test-queue',
                'msg-123',
                '1234-0',
                '60s'
            );
            expect(result).toContain('✓ Message lease renewed');
        });
    });

    describe('Schedule Operations', () => {
        it('should handle create_schedule with cron', async () => {
            mockClient.createSchedule.mockResolvedValue({
                success: true,
                scheduleId: 'daily-task',
            });

            const result = await handleToolCall('create_schedule', {
                schedule_id: 'daily-task',
                queue_name: 'test-queue',
                schedule_type: 'cron',
                cron_expression: '0 9 * * *',
                payload: { task: 'daily' },
            }, mockClient);

            expect(mockClient.createSchedule).toHaveBeenCalledWith(
                expect.objectContaining({
                    scheduleId: 'daily-task',
                    queueName: 'test-queue',
                    scheduleType: 'CRON',
                })
            );
            expect(result).toContain('✓ Schedule created successfully');
        });

        it('should handle create_schedule with calendar', async () => {
            mockClient.createSchedule.mockResolvedValue({
                success: true,
                scheduleId: 'weekly-task',
            });

            const result = await handleToolCall('create_schedule', {
                schedule_id: 'weekly-task',
                queue_name: 'test-queue',
                schedule_type: 'calendar',
                calendar_type: 'weekly',
                times_of_day: ['09:00', '17:00'],
                days_of_week: [1, 3, 5],
                payload: { task: 'weekly' },
            }, mockClient);

            expect(mockClient.createSchedule).toHaveBeenCalledWith(
                expect.objectContaining({
                    scheduleType: 'CALENDAR',
                    calendarSchedule: expect.objectContaining({
                        calendarType: 'WEEKLY',
                    }),
                })
            );
            expect(result).toContain('✓ Schedule created successfully');
        });

        it('should handle list_schedules', async () => {
            mockClient.listSchedules.mockResolvedValue([
                {
                    scheduleId: 'schedule-1',
                    queueName: 'queue-1',
                    scheduleType: 'CRON',
                    cronSchedule: { cronExpression: '0 9 * * *' },
                    enabled: true,
                },
            ]);

            const result = await handleToolCall('list_schedules', {}, mockClient);

            expect(result).toContain('📅 Schedules (1 total)');
            expect(result).toContain('schedule-1');
        });

        it('should handle delete_schedule', async () => {
            mockClient.deleteSchedule.mockResolvedValue({ success: true });

            const result = await handleToolCall('delete_schedule', {
                schedule_id: 'old-schedule',
            }, mockClient);

            expect(mockClient.deleteSchedule).toHaveBeenCalledWith('old-schedule');
            expect(result).toContain('deleted successfully');
        });
    });

    describe('Schema Operations', () => {
        it('should handle register_schema', async () => {
            mockClient.registerSchema.mockResolvedValue({
                schemaId: 'user.profile.v1',
                version: 1,
                createdAt: '1234567890',
            });

            const result = await handleToolCall('register_schema', {
                schema_id: 'user.profile.v1',
                name: 'User Profile',
                content: '{"type":"object"}',
                description: 'User profile schema',
            }, mockClient);

            expect(mockClient.registerSchema).toHaveBeenCalledWith(
                'user.profile.v1',
                'User Profile',
                '{"type":"object"}',
                'User profile schema',
                undefined
            );
            expect(result).toContain('✓ Schema registered successfully');
            expect(result).toContain('user.profile.v1');
        });
    });

    describe('Error Handling', () => {
        it('should throw error for unknown tool', async () => {
            await expect(
                handleToolCall('unknown_tool', {}, mockClient)
            ).rejects.toThrow('Unknown tool: unknown_tool');
        });

        it('should propagate client errors', async () => {
            mockClient.createQueue.mockRejectedValue(new Error('Connection failed'));

            await expect(
                handleToolCall('create_queue', {
                    queue_name: 'test-queue',
                }, mockClient)
            ).rejects.toThrow('Connection failed');
        });
    });
});
