#!/usr/bin/env node

/**
 * ChronoQueue MCP Server
 *
 * Entry point for the Model Context Protocol server that exposes
 * ChronoQueue operations as AI-accessible tools.
 */

import { createMCPServer } from './server.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

async function main() {
    try {
        const server = await createMCPServer();
        const transport = new StdioServerTransport();

        await server.connect(transport);

        // Log to stderr to avoid interfering with MCP protocol on stdout
        console.error('ChronoQueue MCP Server running on stdio');
        console.error('Version: 0.1.0');
    } catch (error) {
        console.error('Fatal error starting MCP server:', error);
        process.exit(1);
    }
}

main();
