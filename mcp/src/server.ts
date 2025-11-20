/**
 * MCP Server Implementation for ChronoQueue
 *
 * Sets up the Model Context Protocol server with tool handlers
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
    CallToolRequestSchema,
    ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

import { ChronoQueueClient } from './chronoqueue-client.js';
import { loadConfig, parseDuration } from './config.js';
import { allTools } from './tools/index.js';
import { handleToolCall } from './tools/handlers.js';

/**
 * Create and configure the MCP server
 */
export async function createMCPServer(): Promise<Server> {
    const server = new Server(
        {
            name: 'chronoqueue-mcp',
            version: '0.1.0',
        },
        {
            capabilities: {
                tools: {},
            },
        }
    );

    // Load configuration
    const config = loadConfig();

    // Initialize ChronoQueue client
    const chronoQueueClient = new ChronoQueueClient(config.chronoqueueAddress, {
        insecure: config.insecure,
        certPath: config.certPath,
        keyPath: config.keyPath,
        caPath: config.caPath,
        timeout: parseDuration(config.timeout),
    });

    // Log configuration (to stderr to not interfere with MCP protocol)
    console.error('ChronoQueue MCP Server Configuration:');
    console.error(`  Address: ${config.chronoqueueAddress}`);
    console.error(`  Insecure: ${config.insecure}`);
    console.error(`  Timeout: ${config.timeout}`);
    console.error('');

    // Handle tool listing
    server.setRequestHandler(ListToolsRequestSchema, async () => {
        return {
            tools: allTools,
        };
    });

    // Handle tool execution
    server.setRequestHandler(CallToolRequestSchema, async (request) => {
        try {
            const result = await handleToolCall(
                request.params.name,
                request.params.arguments || {},
                chronoQueueClient
            );

            return {
                content: [
                    {
                        type: 'text',
                        text: result,
                    },
                ],
            };
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown error';
            console.error(`Tool execution error (${request.params.name}):`, errorMessage);

            return {
                content: [
                    {
                        type: 'text',
                        text: `❌ Error: ${errorMessage}`,
                    },
                ],
                isError: true,
            };
        }
    });

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.error('Received SIGINT, shutting down gracefully...');
        await chronoQueueClient.close();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.error('Received SIGTERM, shutting down gracefully...');
        await chronoQueueClient.close();
        process.exit(0);
    });

    return server;
}
