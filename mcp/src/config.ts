/**
 * Configuration management for ChronoQueue MCP Server
 */

export interface ServerConfig {
    chronoqueueAddress: string;
    insecure: boolean;
    certPath?: string;
    keyPath?: string;
    caPath?: string;
    timeout: string;
}

/**
 * Load configuration from environment variables
 */
export function loadConfig(): ServerConfig {
    return {
        chronoqueueAddress: process.env.CHRONOQUEUE_ADDRESS || 'localhost:9000',
        insecure: process.env.CHRONOQUEUE_INSECURE !== 'false',
        certPath: process.env.CHRONOQUEUE_CERT_PATH,
        keyPath: process.env.CHRONOQUEUE_KEY_PATH,
        caPath: process.env.CHRONOQUEUE_CA_PATH,
        timeout: process.env.CHRONOQUEUE_TIMEOUT || '30s',
    };
}

/**
 * Parse duration string to milliseconds
 */
export function parseDuration(duration: string): number {
    const match = duration.match(/^(\d+)(ms|s|m|h)$/);
    if (!match) {
        throw new Error(`Invalid duration format: ${duration}`);
    }

    const value = parseInt(match[1], 10);
    const unit = match[2];

    switch (unit) {
        case 'ms':
            return value;
        case 's':
            return value * 1000;
        case 'm':
            return value * 60 * 1000;
        case 'h':
            return value * 60 * 60 * 1000;
        default:
            throw new Error(`Unknown duration unit: ${unit}`);
    }
}
