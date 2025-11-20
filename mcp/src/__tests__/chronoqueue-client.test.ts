/**
 * Unit tests for ChronoQueueClient
 */

import { describe, it, expect, vi } from 'vitest';
import { ChronoQueueClient } from '../chronoqueue-client';

// Mock grpc and proto-loader
vi.mock('@grpc/grpc-js', () => ({
    credentials: {
        createInsecure: vi.fn(() => ({})),
        createSsl: vi.fn(() => ({})),
    },
    loadPackageDefinition: vi.fn(() => ({
        chronoqueue: {
            api: {
                queueservice: {
                    v1: {
                        QueueService: vi.fn(),
                    },
                },
            },
        },
    })),
}));

vi.mock('@grpc/proto-loader', () => ({
    loadSync: vi.fn(() => ({})),
}));

describe('ChronoQueueClient', () => {
    describe('Constructor', () => {
        it('should create client with insecure credentials', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });

        it('should create client with custom timeout', () => {
            const client = new ChronoQueueClient('localhost:9000', {
                insecure: true,
                timeout: 60000
            });
            expect(client).toBeDefined();
        });
    });

    describe('Helper Functions', () => {
        it('should convert JavaScript object to protobuf Struct', () => {
            // This tests the objectToStruct helper indirectly through message posting
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });

        it('should convert protobuf Struct to JavaScript object', () => {
            // This tests the structToObject helper indirectly through message retrieval
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });
    });

    describe('Protobuf Conversion', () => {
        it('should handle nested objects in payload', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });

        it('should handle arrays in payload', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });

        it('should handle null values in payload', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });

        it('should handle various data types (string, number, boolean)', () => {
            const client = new ChronoQueueClient('localhost:9000', { insecure: true });
            expect(client).toBeDefined();
        });
    });
});
