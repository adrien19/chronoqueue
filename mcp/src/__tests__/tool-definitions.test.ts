/**
 * Unit tests for tool definitions
 */

import { describe, it, expect } from 'vitest';
import { allTools } from '../tools/index';

describe('Tool Definitions', () => {
    describe('Tool Registry', () => {
        it('should export all 13 tools', () => {
            expect(allTools).toHaveLength(13);
        });

        it('should have unique tool names', () => {
            const names = allTools.map(tool => tool.name);
            const uniqueNames = new Set(names);
            expect(uniqueNames.size).toBe(names.length);
        });

        it('should include all queue management tools', () => {
            const names = allTools.map(tool => tool.name);
            expect(names).toContain('create_queue');
            expect(names).toContain('delete_queue');
            expect(names).toContain('list_queues');
            expect(names).toContain('get_queue_state');
        });

        it('should include all message operation tools', () => {
            const names = allTools.map(tool => tool.name);
            expect(names).toContain('post_message');
            expect(names).toContain('get_next_message');
            expect(names).toContain('peek_messages');
            expect(names).toContain('acknowledge_message');
            expect(names).toContain('renew_message_lease');
        });

        it('should include all schedule tools', () => {
            const names = allTools.map(tool => tool.name);
            expect(names).toContain('create_schedule');
            expect(names).toContain('list_schedules');
            expect(names).toContain('delete_schedule');
        });

        it('should include schema management tool', () => {
            const names = allTools.map(tool => tool.name);
            expect(names).toContain('register_schema');
        });
    });

    describe('Tool Input Schemas', () => {
        it('should have valid input schema for each tool', () => {
            allTools.forEach(tool => {
                expect(tool.inputSchema).toBeDefined();
                expect(tool.inputSchema.type).toBe('object');
                expect(tool.inputSchema.properties).toBeDefined();
            });
        });

        it('create_queue should have required queue_name', () => {
            const tool = allTools.find(t => t.name === 'create_queue');
            expect(tool?.inputSchema.required).toContain('queue_name');
        });

        it('post_message should have required fields', () => {
            const tool = allTools.find(t => t.name === 'post_message');
            expect(tool?.inputSchema.required).toContain('queue_name');
            expect(tool?.inputSchema.required).toContain('message_id');
            expect(tool?.inputSchema.required).toContain('payload');
        });

        it('post_message should have optional schema fields', () => {
            const tool = allTools.find(t => t.name === 'post_message');
            expect(tool?.inputSchema.properties).toBeDefined();
            expect((tool?.inputSchema.properties as any).schema_id).toBeDefined();
            expect((tool?.inputSchema.properties as any).schema_version).toBeDefined();
        });

        it('acknowledge_message should have required fields', () => {
            const tool = allTools.find(t => t.name === 'acknowledge_message');
            expect(tool?.inputSchema.required).toContain('queue_name');
            expect(tool?.inputSchema.required).toContain('message_id');
            expect(tool?.inputSchema.required).toContain('status');
        });

        it('create_schedule should have required fields', () => {
            const tool = allTools.find(t => t.name === 'create_schedule');
            expect(tool?.inputSchema.required).toContain('schedule_id');
            expect(tool?.inputSchema.required).toContain('queue_name');
            expect(tool?.inputSchema.required).toContain('schedule_type');
            expect(tool?.inputSchema.required).toContain('payload');
        });

        it('register_schema should have required fields', () => {
            const tool = allTools.find(t => t.name === 'register_schema');
            expect(tool?.inputSchema.required).toContain('schema_id');
            expect(tool?.inputSchema.required).toContain('name');
            expect(tool?.inputSchema.required).toContain('content');
        });
    });

    describe('Tool Descriptions', () => {
        it('should have non-empty descriptions', () => {
            allTools.forEach(tool => {
                expect(tool.description).toBeDefined();
                expect(tool.description?.length).toBeGreaterThan(0);
            });
        });

        it('should have descriptive property descriptions', () => {
            allTools.forEach(tool => {
                const properties = tool.inputSchema.properties;
                if (properties) {
                    Object.values(properties).forEach((prop: any) => {
                        if (prop.description) {
                            expect(prop.description.length).toBeGreaterThan(0);
                        }
                    });
                }
            });
        });
    });

    describe('Tool Parameter Types', () => {
        it('priority should be a number with range 1-10', () => {
            const tool = allTools.find(t => t.name === 'post_message');
            const priority = (tool?.inputSchema.properties as any)?.priority;
            expect(priority.type).toBe('number');
            expect(priority.minimum).toBe(1);
            expect(priority.maximum).toBe(10);
        });

        it('payload should be an object', () => {
            const tool = allTools.find(t => t.name === 'post_message');
            const payload = (tool?.inputSchema.properties as any)?.payload;
            expect(payload.type).toBe('object');
        });

        it('status should be an enum', () => {
            const tool = allTools.find(t => t.name === 'acknowledge_message');
            const status = (tool?.inputSchema.properties as any)?.status;
            expect(status.enum).toEqual(['completed', 'errored']);
        });

        it('schedule_type should be an enum', () => {
            const tool = allTools.find(t => t.name === 'create_schedule');
            const scheduleType = (tool?.inputSchema.properties as any)?.schedule_type;
            expect(scheduleType.enum).toEqual(['cron', 'calendar']);
        });
    });
});
