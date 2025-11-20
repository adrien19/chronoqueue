/**
 * Unit tests for ChronoQueueClient
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { ChronoQueueClient } from '../chronoqueue-client';
import { ScheduleType } from '../types/chronoqueue';
import * as grpc from '@grpc/grpc-js';

// Use vi.hoisted to define all mocks before they're used
const { mockService, mockQueueService, mockCredentials, mockMetadata, mockLoadPackage } = vi.hoisted(() => {
    const service = {
        createQueue: vi.fn(),
        deleteQueue: vi.fn(),
        listQueues: vi.fn(),
        getQueueState: vi.fn(),
        postMessage: vi.fn(),
        getNextMessage: vi.fn(),
        peekQueueMessages: vi.fn(),
        acknowledgeMessage: vi.fn(),
        renewMessageLease: vi.fn(),
        createSchedule: vi.fn(),
        listSchedules: vi.fn(),
        deleteSchedule: vi.fn(),
        registerSchema: vi.fn(),
        close: vi.fn(),
    };

    const QueueService = vi.fn(() => service);

    return {
        mockService: service,
        mockQueueService: QueueService,
        mockCredentials: {
            createInsecure: vi.fn(() => ({})),
            createSsl: vi.fn(() => ({})),
        },
        mockMetadata: vi.fn().mockImplementation(function (this: any) {
            return {};
        }),
        mockLoadPackage: vi.fn(() => ({
            chronoqueue: {
                api: {
                    queueservice: {
                        v1: {
                            QueueService,
                        },
                    },
                },
            },
        })),
    };
});

// Mock grpc and proto-loader
vi.mock('@grpc/grpc-js', () => ({
    credentials: mockCredentials,
    Metadata: mockMetadata,
    loadPackageDefinition: mockLoadPackage,
    status: {
        NOT_FOUND: 5,
        DEADLINE_EXCEEDED: 4,
    },
}));

vi.mock('@grpc/proto-loader', () => ({
    loadSync: vi.fn(() => ({})),
}));

describe('ChronoQueueClient', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Constructor', () => {
        it('should create client with insecure credentials', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
            expect(mockCredentials.createInsecure).toHaveBeenCalled();
        });

        it('should create client with custom timeout', () => {
            const client = new ChronoQueueClient('localhost:9000', {
                insecure: true,
                timeout: 60000
            });
            expect(client).toBeDefined();
        });

        it('should create client with SSL credentials when certPath provided', () => {
            // Mock fs.readFileSync
            vi.mock('fs', () => ({
                readFileSync: vi.fn(() => Buffer.from('fake-cert')),
            }));

            const client = new ChronoQueueClient('localhost:9000', {
                insecure: false,
                certPath: '/path/to/cert',
                keyPath: '/path/to/key',
                caPath: '/path/to/ca',
            });
            expect(client).toBeDefined();
        });
    });

    describe('Protobuf Conversion', () => {
        let client: ChronoQueueClient;

        beforeEach(() => {
            client = new ChronoQueueClient('localhost:9000', { insecure: true });
        });

        it('should convert nested objects through post and get message', async () => {
            const testPayload = {
                user: {
                    name: 'John Doe',
                    age: 30,
                    metadata: {
                        level: 'premium',
                    },
                },
            };

            mockService.postMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.postMessage('test-queue', 'msg-1', testPayload);

            expect(mockService.postMessage).toHaveBeenCalled();
            const callArgs = mockService.postMessage.mock.calls[0][0];
            expect(callArgs.message.metadata.payload.data.fields).toBeDefined();
        });

        it('should convert arrays in payload', async () => {
            const testPayload = {
                items: [1, 2, 3],
                tags: ['a', 'b', 'c'],
            };

            mockService.postMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.postMessage('test-queue', 'msg-2', testPayload);
            expect(mockService.postMessage).toHaveBeenCalled();
        });

        it('should handle null and undefined values in payload', async () => {
            const testPayload = {
                nullValue: null,
                undefinedValue: undefined,
                validValue: 'test',
            };

            mockService.postMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.postMessage('test-queue', 'msg-3', testPayload);
            expect(mockService.postMessage).toHaveBeenCalled();
        });

        it('should handle various primitive types (string, number, boolean)', async () => {
            const testPayload = {
                stringVal: 'hello',
                numberVal: 42,
                boolVal: true,
                floatVal: 3.14,
            };

            mockService.postMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.postMessage('test-queue', 'msg-4', testPayload);
            expect(mockService.postMessage).toHaveBeenCalled();
        });

        it('should round-trip convert complex payload through protobuf Struct', async () => {
            const originalPayload = {
                user: {
                    id: 123,
                    name: 'Alice',
                    active: true,
                    tags: ['premium', 'verified'],
                    metadata: {
                        level: 5,
                        score: 98.5,
                    },
                },
            };

            mockService.getNextMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, {
                    message: {
                        message_id: 'msg-1',
                        metadata: {
                            payload: {
                                data: {
                                    fields: {
                                        user: {
                                            structValue: {
                                                fields: {
                                                    id: { numberValue: 123 },
                                                    name: { stringValue: 'Alice' },
                                                    active: { boolValue: true },
                                                    tags: {
                                                        listValue: {
                                                            values: [
                                                                { stringValue: 'premium' },
                                                                { stringValue: 'verified' },
                                                            ],
                                                        },
                                                    },
                                                    metadata: {
                                                        structValue: {
                                                            fields: {
                                                                level: { numberValue: 5 },
                                                                score: { numberValue: 98.5 },
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        },
                                    },
                                },
                            },
                            priority: 5,
                        },
                    },
                    stream_entry_id: 'entry-1',
                });
            });

            const response = await client.getNextMessage('test-queue');
            expect(response).toBeDefined();
            expect(response?.message.payload).toEqual(originalPayload);
        });
    });

    describe('gRPC Method Calls', () => {
        let client: ChronoQueueClient;

        beforeEach(() => {
            client = new ChronoQueueClient('localhost:9000', { insecure: true, timeout: 5000 });
        });

        it('should call createQueue with correct arguments and deadline', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                expect(options.deadline).toBeDefined();
                expect(options.deadline).toBeInstanceOf(Date);
                callback(null, { success: true });
            });

            await client.createQueue('test-queue', {
                dequeueAttempts: 5,
                leaseDuration: '60s',
                autoCreateDLQ: true,
            });

            expect(mockService.createQueue).toHaveBeenCalled();
            const callArgs = mockService.createQueue.mock.calls[0][0];
            expect(callArgs.name).toBe('test-queue');
            expect(callArgs.metadata.default_max_attempts).toBe(5);
            expect(callArgs.metadata.lease_duration.seconds).toBe(60);
            expect(callArgs.metadata.auto_create_dlq).toBe(true);
        });

        it('should call postMessage with schema validation parameters', async () => {
            mockService.postMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.postMessage('test-queue', 'msg-1', { data: 'test' }, {
                schemaId: 'user.v1',
                schemaVersion: 2,
                priority: 8,
                leaseDuration: 120,
            });

            expect(mockService.postMessage).toHaveBeenCalled();
            const callArgs = mockService.postMessage.mock.calls[0][0];
            expect(callArgs.message.metadata.payload.schema_id).toBe('user.v1');
            expect(callArgs.message.metadata.payload.schema_version).toBe(2);
            expect(callArgs.message.metadata.priority).toBe(8);
            expect(callArgs.message.metadata.lease_duration.seconds).toBe(120);
        });

        it('should handle getNextMessage returning null when no messages', async () => {
            mockService.getNextMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                const error: any = new Error('NOT_FOUND');
                error.code = 5; // grpc.status.NOT_FOUND
                callback(error);
            });

            const response = await client.getNextMessage('empty-queue');
            expect(response).toBeNull();
        });

        it('should apply timeout deadline to all gRPC calls', async () => {
            const shortTimeoutClient = new ChronoQueueClient('localhost:9000', {
                insecure: true,
                timeout: 1000,
            });

            mockService.listQueues.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                const deadline = options.deadline;
                const now = new Date();
                const timeUntilDeadline = deadline.getTime() - now.getTime();
                expect(timeUntilDeadline).toBeGreaterThan(0);
                expect(timeUntilDeadline).toBeLessThanOrEqual(1000);
                callback(null, { queues: [] });
            });

            await shortTimeoutClient.listQueues();
            expect(mockService.listQueues).toHaveBeenCalled();
        });
    });

    describe('Error Handling', () => {
        let client: ChronoQueueClient;

        beforeEach(() => {
            client = new ChronoQueueClient('localhost:9000', { insecure: true });
        });

        it('should propagate gRPC errors correctly', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(new Error('Queue already exists'));
            });

            await expect(client.createQueue('duplicate-queue')).rejects.toThrow('Queue already exists');
        });

        it('should handle NOT_FOUND error in getNextMessage', async () => {
            mockService.getNextMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                const error: any = new Error('NOT_FOUND');
                error.code = 5;
                callback(error);
            });

            const result = await client.getNextMessage('test-queue');
            expect(result).toBeNull();
        });

        it('should propagate non-NOT_FOUND errors in getNextMessage', async () => {
            mockService.getNextMessage.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(new Error('Internal server error'));
            });

            await expect(client.getNextMessage('test-queue')).rejects.toThrow('Internal server error');
        });
    });

    describe('Schedule Operations', () => {
        let client: ChronoQueueClient;

        beforeEach(() => {
            client = new ChronoQueueClient('localhost:9000', { insecure: true });
        });

        it('should create cron schedule with correct structure', async () => {
            mockService.createSchedule.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, { success: true });
            });

            await client.createSchedule({
                scheduleId: 'daily-report',
                queueName: 'reports',
                scheduleType: ScheduleType.CRON,
                cronSchedule: { cronExpression: '0 9 * * *' },
                payloadData: { type: 'daily' },
            });

            expect(mockService.createSchedule).toHaveBeenCalled();
            const callArgs = mockService.createSchedule.mock.calls[0][0];
            // The cron_schedule is directly on metadata (oneof field), not nested in schedule_config
            expect(callArgs.schedule.metadata.cron_schedule).toBe('0 9 * * *');
        });

        it('should list schedules reading cron_schedule from metadata', async () => {
            mockService.listSchedules.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                callback(null, {
                    schedules: [
                        {
                            schedule_id: 'test-schedule',
                            metadata: {
                                queue_name: 'test-queue',
                                // The cron_schedule is directly on metadata (oneof field)
                                cron_schedule: '0 * * * *',
                                payload: {
                                    data: {
                                        fields: {
                                            message: { stringValue: 'hourly test' },
                                        },
                                    },
                                },
                            },
                        },
                    ],
                });
            });

            const schedules = await client.listSchedules();
            expect(schedules).toHaveLength(1);
            expect(schedules[0].scheduleType).toBe('CRON');
            expect(schedules[0].cronSchedule?.cronExpression).toBe('0 * * * *');
        });
    });

    describe('Lease Duration Parsing', () => {
        let client: ChronoQueueClient;

        beforeEach(() => {
            client = new ChronoQueueClient('localhost:9000', { insecure: true });
        });

        it('should parse lease duration from seconds', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                expect(req.metadata.lease_duration.seconds).toBe(45);
                callback(null, { success: true });
            });

            await client.createQueue('test-queue', { leaseDuration: '45s' });
        });

        it('should parse lease duration from minutes', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                expect(req.metadata.lease_duration.seconds).toBe(300); // 5 minutes = 300 seconds
                callback(null, { success: true });
            });

            await client.createQueue('test-queue', { leaseDuration: '5m' });
        });

        it('should parse lease duration from hours', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                expect(req.metadata.lease_duration.seconds).toBe(7200); // 2 hours = 7200 seconds
                callback(null, { success: true });
            });

            await client.createQueue('test-queue', { leaseDuration: '2h' });
        });

        it('should use default 30s when no lease duration provided', async () => {
            mockService.createQueue.mockImplementation((req: any, metadata: any, options: any, callback: any) => {
                expect(req.metadata.lease_duration.seconds).toBe(30);
                callback(null, { success: true });
            });

            await client.createQueue('test-queue', {});
        });
    });
});
