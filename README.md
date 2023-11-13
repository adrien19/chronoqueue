# ChronoQueue

ChronoQueue is queue management system designed to handle high-volume message processing with efficiency and reliability. It offers a priority-based messaging system, real-time monitoring, and flexible scheduling options, making it an ideal solution for complex asynchronous task management.


## Features

- **Priority Queue Management:** ChronoQueue allows users to assign priorities to messages, ensuring that critical tasks are processed first. This feature is crucial for systems where task urgency varies significantly.

- **Real-time Monitoring and Analytics (WIP):** A dashboard provides a comprehensive overview of all queues and messages, including real-time updates on message statuses, queue health, and system performance metrics.

- **Flexible Scheduling (WIP):** Supports both calendar-based and cron expression scheduling, allowing precise control over when messages are processed.

- **High Scalability and Performance:** Designed to handle millions of messages efficiently, ChronoQueue ensures high throughput and low latency even under heavy loads.

- **Robust Error Handling and Retry Mechanisms:** Automated handling of failed messages with customizable retry policies and error tracking.

- **Secure and Compliant:** Adheres to best practices in security and data handling, ensuring that your data is safe and compliant with relevant regulations.

- **Customizable and Extensible:** Easily adaptable to specific use cases, with support for custom extensions and integrations.

- **Detailed Documentation and Community Support (WIP):** Comprehensive guides, API documentation, and a supportive community for troubleshooting and best practices.

## Getting Started

### Prerequisites

- [Redis](https://redis.io/)
- [Go](https://golang.org/) (for server-side & client-side SDK)
- [Python](https://www.python.org/) (for client-side SDKs)

### Installation

#### Docker Compose Option:

The easiest way to get started locally is to use [docker-compose](https://docs.docker.com/compose/). Simply:

1. Clone the repository:
   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```
2. Cd into deploy - `cd deploy` and run:
    ```bash
    docker-compose up 
    ```

#### Run Server Option:

1. Clone the repository:
   ```bash
   git clone https://github.com/adrien19/chronoqueue.git
   ```

2. Install dependencies:
    ```bash
    # For Go server
    go mod tidy

    # For Python/Go clients
    pip install chronoqueuesdk
    # or
    go get https://github.com/adrien19/chronoqueue/client
    ```

3. Configure your environment (refer to the .env.example file for guidance).

4. Start the ChronoQueue server:
    ```bash
    go run cmd/server/main.go

    ```

If you choose to use mTLS option, you will need to generate certificates. You can use already provided script `generate_certs.sh` to quickly generate these certificates.

## Documentation

For detailed documentation, including API references and usage examples, visit [ChronoQueue Docs]()

## Contribution

We welcome contributions! Please read our [Contributing Guidelines]() for more information.

## License

ChronoQueue is licensed under [MIT License](./LICENSE).

## Acknowledgments

Special thanks to all the contributors and users who have made ChronoQueue a robust and evolving system.