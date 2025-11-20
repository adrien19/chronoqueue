package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/structpb"

	queuev1 "github.com/adrien19/chronoqueue/api/queue/v1"
	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/agents"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/coordinator"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/llm"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/models"
	"github.com/adrien19/chronoqueue/examples/ai-agent-orchestrator/pkg/monitoring"
)

// newInitCommand creates the init command to set up all queues
func newInitCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Initialize all agent queues",
		Long:  `Create all required queues for the AI agent orchestrator system.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return initQueues()
		},
	}
}

// newSubmitCommand creates the submit command to submit tasks
func newSubmitCommand() *cobra.Command {
	var priority int32

	cmd := &cobra.Command{
		Use:   "submit <task-file.json>",
		Short: "Submit a task for processing",
		Long:  `Submit a task definition file to the coordinator queue for processing.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskFile := args[0]
			return submitTask(taskFile, priority)
		},
	}

	cmd.Flags().Int32VarP(&priority, "priority", "p", 5, "Task priority (1-10)")

	return cmd
}

// newStatusCommand creates the status command to check task status
func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status <task-id>",
		Short: "Check task status",
		Long:  `Check the current status of a submitted task.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			taskID := args[0]
			return checkTaskStatus(taskID)
		},
	}
}

// newMonitorCommand creates the monitor command for real-time monitoring
func newMonitorCommand() *cobra.Command {
	var follow bool

	cmd := &cobra.Command{
		Use:   "monitor",
		Short: "Monitor agent system status",
		Long:  `Display real-time monitoring dashboard for all agents and queues.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return monitorSystem(follow)
		},
	}

	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow mode (continuous updates)")

	return cmd
}

// newLogsCommand creates the logs command for viewing logs
func newLogsCommand() *cobra.Command {
	var (
		lines  int
		follow bool
		filter string
		since  string
	)

	cmd := &cobra.Command{
		Use:   "logs",
		Short: "View agent logs",
		Long:  `View and filter logs from coordinator and agent processes.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return viewLogs(lines, follow, filter, since)
		},
	}

	cmd.Flags().IntVarP(&lines, "lines", "n", 50, "Number of recent lines to show")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "Follow mode (stream logs)")
	cmd.Flags().StringVar(&filter, "filter", "", "Filter logs by text (task ID, agent, etc)")
	cmd.Flags().StringVar(&since, "since", "1h", "Show logs since duration (e.g., 1h, 30m)")

	return cmd
}

// newResultsCommand creates the results command for viewing agent results
func newResultsCommand() *cobra.Command {
	var (
		taskID     string
		agentType  string
		exportJSON bool
	)

	cmd := &cobra.Command{
		Use:   "results",
		Short: "View agent results",
		Long:  `View historical agent results for completed tasks.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return viewResults(taskID, agentType, exportJSON)
		},
	}

	cmd.Flags().StringVar(&taskID, "task", "", "Filter by parent task ID")
	cmd.Flags().StringVar(&agentType, "agent", "", "Filter by agent type")
	cmd.Flags().BoolVar(&exportJSON, "json", false, "Export as JSON")

	return cmd
}

// newCoordinatorCommand creates the coordinator command to run coordinator agent
func newCoordinatorCommand() *cobra.Command {
	var (
		workers     int
		llmProvider string
		llmModel    string
		llmBaseURL  string
	)

	cmd := &cobra.Command{
		Use:   "coordinator",
		Short: "Run coordinator agent",
		Long:  `Start the coordinator agent that decomposes tasks and routes to specialized agents.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCoordinator(workers, llmProvider, llmModel, llmBaseURL)
		},
	}

	cmd.Flags().IntVarP(&workers, "workers", "w", 2, "Number of worker goroutines")
	cmd.Flags().StringVar(&llmProvider, "llm-provider", "mock", "LLM provider to use (mock, ollama, openai, anthropic)")
	cmd.Flags().StringVar(&llmModel, "llm-model", "", "Override LLM model (optional, uses provider defaults)")
	cmd.Flags().StringVar(&llmBaseURL, "llm-base-url", "", "LLM base URL (optional, uses provider defaults)")

	return cmd
}

// newAgentsCommand creates the agents command to run specialized agents
func newAgentsCommand() *cobra.Command {
	var (
		all           bool
		webSearch     bool
		codeAnalyzer  bool
		dataProcessor bool
		aggregator    bool
		notification  bool
		llmWriter     bool
		llmResearcher bool
		llmCoder      bool
		llmProvider   string
		llmModel      string
		llmBaseURL    string
		workers       int
	)

	cmd := &cobra.Command{
		Use:   "agents",
		Short: "Run specialized agents",
		Long:  `Start one or more specialized agent workers.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgents(all, webSearch, codeAnalyzer, dataProcessor, aggregator, notification,
				llmWriter, llmResearcher, llmCoder, llmProvider, llmModel, llmBaseURL, workers)
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Run all mock agents")
	cmd.Flags().BoolVar(&webSearch, "web-search", false, "Run web search agent")
	cmd.Flags().BoolVar(&codeAnalyzer, "code-analyzer", false, "Run code analyzer agent")
	cmd.Flags().BoolVar(&dataProcessor, "data-processor", false, "Run data processor agent")
	cmd.Flags().BoolVar(&aggregator, "aggregator", false, "Run aggregator agent")
	cmd.Flags().BoolVar(&notification, "notification", false, "Run notification agent")
	cmd.Flags().BoolVar(&llmWriter, "llm-writer", false, "Run LLM writer agent (requires --llm-provider)")
	cmd.Flags().BoolVar(&llmResearcher, "llm-researcher", false, "Run LLM researcher agent (requires --llm-provider)")
	cmd.Flags().BoolVar(&llmCoder, "llm-coder", false, "Run LLM coder agent (requires --llm-provider)")
	cmd.Flags().StringVar(&llmProvider, "llm-provider", "mock", "LLM provider for LLM agents (mock, ollama)")
	cmd.Flags().StringVar(&llmModel, "llm-model", "", "Override LLM model (optional)")
	cmd.Flags().StringVar(&llmBaseURL, "llm-base-url", "", "LLM base URL (optional)")
	cmd.Flags().IntVarP(&workers, "workers", "w", 2, "Number of workers per agent")

	return cmd
}

// newCleanupCommand creates the cleanup command
func newCleanupCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up all demo data",
		Long:  `Delete all queues and demo data created by the orchestrator.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cleanup()
		},
	}
}

// initQueues creates all required queues
func initQueues() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	queues := []struct {
		name          string
		queueType     queuev1.QueueType
		leaseDuration string
		maxAttempts   int32
		description   string
	}{
		{"agent-coordinator", queuev1.QueueType_SIMPLE, "2m", 3, "Task decomposition and routing"},
		{"agent-web-search", queuev1.QueueType_SIMPLE, "5m", 5, "Web research and API calls"},
		{"agent-code-analyzer", queuev1.QueueType_SIMPLE, "10m", 3, "Code review and analysis"},
		{"agent-data-processor", queuev1.QueueType_SIMPLE, "5m", 3, "Data analysis and statistics"},
		{"agent-aggregator", queuev1.QueueType_SIMPLE, "3m", 3, "Result synthesis"},
		{"agent-notification", queuev1.QueueType_SIMPLE, "1m", 5, "Delivery notifications"},
		{"agent-results", queuev1.QueueType_SIMPLE, "5m", 3, "Historical agent results storage"},
		{"agent-llm-writer", queuev1.QueueType_SIMPLE, "3m", 3, "LLM-powered creative writing"},
		{"agent-llm-researcher", queuev1.QueueType_SIMPLE, "5m", 3, "LLM-powered research and analysis"},
		{"agent-llm-coder", queuev1.QueueType_SIMPLE, "5m", 3, "LLM-powered code generation"},
	}

	fmt.Println("🚀 Initializing AI Agent Orchestrator...")
	fmt.Println()

	for _, q := range queues {
		leaseDuration, err := time.ParseDuration(q.leaseDuration)
		if err != nil {
			return fmt.Errorf("invalid lease duration for %s: %w", q.name, err)
		}

		opts := client.QueueOptions{
			Type:                int32(q.queueType),
			DequeueAttempts:     q.maxAttempts,
			LeaseDuration:       q.leaseDuration,
			AutoCreateDLQ:       true,
			DeadLetterQueueName: q.name + "-dlq",
		}

		_, err = c.CreateQueue(ctx, q.name, opts)
		if err != nil {
			fmt.Printf("⚠️  Queue '%s' already exists or error: %v\n", q.name, err)
		} else {
			fmt.Printf("✓ Created queue: %s\n", q.name)
			fmt.Printf("  Type: %s, Lease: %v, Max Attempts: %d\n",
				q.queueType.String(), leaseDuration, q.maxAttempts)
			fmt.Printf("  Use case: %s\n", q.description)
			fmt.Printf("  DLQ: %s-dlq\n", q.name)
			fmt.Println()
		}
	}

	fmt.Println("✓ Initialization complete!")
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Println("  1. Start agents:     ./ai-orchestrator agents --all")
	fmt.Println("  2. Submit a task:    ./ai-orchestrator submit tasks/competitor-analysis.json")
	fmt.Println("  3. Monitor progress: ./ai-orchestrator monitor --follow")

	return nil
}

// submitTask submits a task from a JSON file
func submitTask(taskFile string, priority int32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Read task file
	data, err := os.ReadFile(taskFile)
	if err != nil {
		return fmt.Errorf("failed to read task file: %w", err)
	}

	var task models.Task
	if err := json.Unmarshal(data, &task); err != nil {
		return fmt.Errorf("failed to parse task file: %w", err)
	}

	// Override priority if specified
	if priority != 5 {
		task.Priority = priority
	}

	// Create client
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	// Convert task to payload
	taskData := map[string]interface{}{
		"task_id":     task.TaskID,
		"task_type":   task.TaskType,
		"description": task.Description,
		"input":       task.Input,
		"priority":    task.Priority,
		"tenant_id":   task.TenantID,
		"created_at":  time.Now().Unix(),
	}

	payloadStruct, err := structpb.NewStruct(taskData)
	if err != nil {
		return fmt.Errorf("failed to create payload: %w", err)
	}

	// Submit to coordinator queue
	_, err = c.PostMessage(ctx, "agent-coordinator", task.TaskID, client.MessageOptions{
		Priority: int64(task.Priority),
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: "application/json",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to submit task: %w", err)
	}

	fmt.Println("✓ Task submitted successfully!")
	fmt.Printf("  Task ID: %s\n", task.TaskID)
	fmt.Printf("  Type: %s\n", task.TaskType)
	fmt.Printf("  Priority: %d\n", task.Priority)
	fmt.Println()
	fmt.Printf("Check status: ./ai-orchestrator status %s\n", task.TaskID)

	return nil
}

// getLLMConfig builds LLM configuration from CLI flags
func getLLMConfig(provider, model, baseURL string) *llm.LLMConfig {
	if provider == "" || provider == "mock" {
		return nil // nil config defaults to mock
	}

	config := &llm.LLMConfig{
		DefaultProvider: llm.ProviderType(provider),
		Providers:       llm.ProvidersConfig{},
	}

	switch llm.ProviderType(provider) {
	case llm.ProviderOllama:
		ollamaConfig := llm.DefaultOllamaConfig()
		if baseURL != "" {
			ollamaConfig.BaseURL = baseURL
		}
		if model != "" {
			// Override all models with the specified one
			for agentType := range ollamaConfig.Models {
				ollamaConfig.Models[agentType] = model
			}
		}
		config.Providers.Ollama = ollamaConfig
	case llm.ProviderOpenAI:
		// Will be implemented in future phases
	case llm.ProviderAnthropic:
		// Will be implemented in future phases
	}

	return config
}

// runCoordinator starts the coordinator agent
func runCoordinator(workers int, llmProvider, llmModel, llmBaseURL string) error {
	ctx := context.Background()

	// Create ChronoQueue client
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Create LLM config from CLI flags
	llmConfig := getLLMConfig(llmProvider, llmModel, llmBaseURL)

	// Create LLM client using factory
	llmClient, err := llm.NewLLMClient(llmConfig, verbose)
	if err != nil {
		return fmt.Errorf("failed to create LLM client: %w", err)
	}

	// Create coordinator
	coord := coordinator.NewCoordinator(c, llmClient, workers, verbose)

	// Start coordinator in background so we can react to OS signals
	errCh := make(chan error, 1)
	go func() {
		errCh <- coord.Start(ctx)
	}()

	fmt.Println("✓ Coordinator started")
	fmt.Printf("  Workers: %d\n", workers)
	fmt.Printf("  LLM Provider: %s\n", llmProvider)
	if llmModel != "" {
		fmt.Printf("  LLM Model: %s\n", llmModel)
	}
	if llmBaseURL != "" {
		fmt.Printf("  LLM Base URL: %s\n", llmBaseURL)
	}
	fmt.Println("  Press Ctrl+C to stop")

	// Wait for either a signal or coordinator error
	select {
	case <-sigChan:
		fmt.Println("\nShutting down coordinator...")
		coord.Stop()
		if err := <-errCh; err != nil {
			return fmt.Errorf("coordinator stopped with error: %w", err)
		}
		fmt.Println("✓ Coordinator stopped")
		return nil
	case err := <-errCh:
		return fmt.Errorf("coordinator terminated: %w", err)
	}
}

// checkTaskStatus checks the status of a task
func checkTaskStatus(taskID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create ChronoQueue client
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	// Initialize monitor using ChronoQueue API
	redisMonitor := monitoring.NewRedisMonitor(c)
	defer func() {
		if err := redisMonitor.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to close monitor: %v\n", err)
		}
	}()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Printf("║  TASK STATUS: %-62s  ║\n", taskID)
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Check coordinator queue state
	coordinatorState, err := redisMonitor.GetQueueState(ctx, "agent-coordinator")
	if err == nil {
		fmt.Printf("📋 Coordinator Queue:\n")
		fmt.Printf("  Pending:   %d\n", coordinatorState.Pending)
		fmt.Printf("  Running:   %d\n", coordinatorState.Running)
		fmt.Printf("  Completed: %d\n", coordinatorState.Completed)
		fmt.Printf("  Errored:   %d\n", coordinatorState.Errored)
	} else {
		fmt.Printf("📋 Coordinator Status: UNAVAILABLE\n")
	}

	// Check for subtasks
	fmt.Println()
	fmt.Println("🔍 Subtask Queue Status:")

	subtaskQueues := []string{
		"agent-web-search",
		"agent-code-analyzer",
		"agent-data-processor",
		"agent-aggregator",
	}

	for _, queueName := range subtaskQueues {
		state, err := redisMonitor.GetQueueState(ctx, queueName)
		if err == nil {
			total := state.Pending + state.Running
			if total > 0 || state.Completed > 0 || state.Errored > 0 {
				fmt.Printf("  • %s:\n", queueName)
				fmt.Printf("    Active: %d (Pending: %d, Running: %d)\n", total, state.Pending, state.Running)
				fmt.Printf("    Done: %d (Completed: %d, Errored: %d)\n", state.Completed+state.Errored, state.Completed, state.Errored)
			}
		}
	}

	fmt.Println()
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Println("💡 Tip: Use './ai-orchestrator monitor --follow' for real-time monitoring")
	fmt.Println()

	return nil
}

// monitorSystem displays monitoring dashboard
func monitorSystem(follow bool) error {
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	// Create monitor with 3-second refresh rate
	monitor := monitoring.NewMonitor(c, 3*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\n👋 Stopping monitor...")
		cancel()
	}()

	if follow {
		// Continuous monitoring
		return monitor.WatchSystem(ctx, false)
	}

	// Single snapshot
	stats, err := monitor.GetSystemStats(ctx)
	if err != nil {
		return fmt.Errorf("failed to get system stats: %w", err)
	}

	monitor.DisplaySystemStats(stats)
	return nil
}

// viewLogs displays agent logs with filtering
func viewLogs(lines int, follow bool, filter string, sinceStr string) error {
	// Parse duration
	since, err := time.ParseDuration(sinceStr)
	if err != nil {
		return fmt.Errorf("invalid duration '%s': %w", sinceStr, err)
	}

	// Create log viewer with standard log paths
	logPaths := []string{
		"coordinator.log",
		"agents.log",
	}

	viewer := monitoring.NewLogViewer(logPaths...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n\n👋 Stopping log viewer...")
		cancel()
	}()

	if follow {
		// Continuous streaming
		return viewer.FollowLogs(ctx, filter)
	}

	// Show summary first
	if err := viewer.DisplayLogSummary(ctx, since); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not display summary: %v\n", err)
	}

	// Then tail recent logs
	return viewer.TailLogs(ctx, lines, filter)
}

// viewResults displays stored agent results
func viewResults(taskID, agentType string, exportJSON bool) error {
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  AGENT RESULTS VIEWER                                                         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Note: Results are currently stored in the running agents' in-memory cache
	// To view results, check the agents.log file or monitor output during execution
	fmt.Println("ℹ️  RESULT STORAGE STATUS")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────")
	fmt.Println()
	fmt.Println("Results are currently stored in-memory within the running agent processes.")
	fmt.Println("To view agent results in real-time:")
	fmt.Println()
	fmt.Println("  1. Check agent logs:")
	fmt.Println("     ./ai-orchestrator logs --filter \"Subtask\" --lines 100")
	fmt.Println()
	fmt.Println("  2. Monitor live processing:")
	fmt.Println("     ./ai-orchestrator monitor --follow")
	fmt.Println()
	fmt.Println("  3. View raw agent output:")
	fmt.Println("     tail -f agents.log | grep \"completed\"")
	fmt.Println()
	fmt.Println("  4. Check coordinator for task decomposition:")
	fmt.Println("     tail -f coordinator.log | grep \"decomposed\\|routed\"")
	fmt.Println()

	if taskID != "" {
		fmt.Printf("📋 For task '%s', check the logs above to see subtask results.\n", taskID)
		fmt.Println()
		fmt.Println("💡 Future Enhancement: Results will be persistently stored and queryable")
		fmt.Println("   after implementing database backing or Redis-based result retrieval.")
	} else {
		fmt.Println("💡 Specify a task ID to get targeted guidance:")
		fmt.Println("   ./ai-orchestrator results --task <task-id>")
	}

	fmt.Println()
	return nil
}

// runAgents runs specialized agents
func runAgents(all, webSearch, codeAnalyzer, dataProcessor, aggregator, notification, llmWriter, llmResearcher, llmCoder bool,
	llmProvider, llmModel, llmBaseURL string, workers int,
) error {
	if all {
		webSearch = true
		codeAnalyzer = true
		dataProcessor = true
		aggregator = true
		notification = true
	}

	if !webSearch && !codeAnalyzer && !dataProcessor && !aggregator && !notification && !llmWriter && !llmResearcher && !llmCoder {
		return fmt.Errorf("no agents specified. Use --all or specify individual agents")
	}

	// Create client
	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	// Create LLM config from CLI flags
	llmConfig := getLLMConfig(llmProvider, llmModel, llmBaseURL)

	// Create LLM client for aggregator and LLM agents
	llmClient, err := llm.NewLLMClient(llmConfig, verbose)
	if err != nil {
		return fmt.Errorf("failed to create LLM client: %w", err)
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create shared result store and initialize it
	resultStore := agents.NewResultStore(c, verbose)
	if err := resultStore.Initialize(ctx); err != nil {
		if verbose {
			fmt.Printf("Warning: Could not initialize result store: %v\n", err)
		}
	}

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup for all agents
	var wg sync.WaitGroup
	errChan := make(chan error, 4)

	fmt.Println("🚀 Starting agents:")

	// Start web search agent
	if webSearch {
		fmt.Printf("  • Web Search Agent (%d workers)\n", workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewWebSearchAgent(c, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("web search agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start code analyzer agent
	if codeAnalyzer {
		fmt.Printf("  • Code Analyzer Agent (%d workers)\n", workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewCodeAnalyzerAgent(c, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("code analyzer agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start data processor agent
	if dataProcessor {
		fmt.Printf("  • Data Processor Agent (%d workers)\n", workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewDataProcessorAgent(c, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("data processor agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start aggregator agent
	if aggregator {
		fmt.Printf("  • Aggregator Agent (%d workers)\n", workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewAggregatorAgent(c, llmClient, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("aggregator agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start notification agent
	if notification {
		fmt.Printf("  • Notification Agent (%d workers)\n", workers)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewNotificationAgent(c, workers, verbose)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("notification agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start LLM writer agent
	if llmWriter {
		fmt.Printf("  • LLM Writer Agent (%d workers) [LLM: %s]\n", workers, llmProvider)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewLLMWriterAgent(c, llmClient, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("llm writer agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start LLM researcher agent
	if llmResearcher {
		fmt.Printf("  • LLM Researcher Agent (%d workers) [LLM: %s]\n", workers, llmProvider)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewLLMResearcherAgent(c, llmClient, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("llm researcher agent error: %w", err):
				default:
				}
			}
		}()
	}

	// Start LLM coder agent
	if llmCoder {
		fmt.Printf("  • LLM Coder Agent (%d workers) [LLM: %s]\n", workers, llmProvider)
		wg.Add(1)
		go func() {
			defer wg.Done()
			agent := agents.NewLLMCoderAgent(c, llmClient, workers, verbose)
			agent.SetResultStore(resultStore)
			if err := agent.Start(ctx); err != nil {
				select {
				case errChan <- fmt.Errorf("llm coder agent error: %w", err):
				default:
				}
			}
		}()
	}

	fmt.Println("\n✓ All agents started. Press Ctrl+C to stop.")

	// Wait for signal or error
	select {
	case sig := <-sigChan:
		fmt.Printf("\n\n📡 Received signal: %v\n", sig)
		fmt.Println("🛑 Shutting down agents gracefully...")
		cancel()
	case err := <-errChan:
		fmt.Printf("\n⚠️  Agent error: %v\n", err)
		cancel()
	}

	// Wait for all agents to stop
	wg.Wait()
	fmt.Println("✓ All agents stopped")

	return nil
}

// cleanup removes all queues
func cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := createClient()
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer c.Close()

	queues := []string{
		"agent-coordinator",
		"agent-web-search",
		"agent-code-analyzer",
		"agent-data-processor",
		"agent-aggregator",
		"agent-notification",
		"agent-results",
		"agent-llm-writer",
		"agent-llm-researcher",
		"agent-llm-coder",
		"agent-coordinator-dlq",
		"agent-web-search-dlq",
		"agent-code-analyzer-dlq",
		"agent-data-processor-dlq",
		"agent-aggregator-dlq",
		"agent-notification-dlq",
		"agent-results-dlq",
		"agent-llm-writer-dlq",
		"agent-llm-researcher-dlq",
		"agent-llm-coder-dlq",
	}

	fmt.Println("🧹 Cleaning up AI Agent Orchestrator...")
	fmt.Println()

	for _, queueName := range queues {
		_, err := c.DeleteQueue(ctx, queueName)
		if err != nil {
			fmt.Printf("⚠️  Failed to delete queue '%s': %v\n", queueName, err)
		} else {
			fmt.Printf("✓ Deleted queue: %s\n", queueName)
		}
	}

	fmt.Println()
	fmt.Println("✓ Cleanup complete!")

	return nil
}

// createClient creates a ChronoQueue client
func createClient() (*client.ChronoQueueClient, error) {
	opts := client.ClientOptions{
		MaxRetries:          client.DefaultMaxRetries,
		InitialBackoff:      client.DefaultInitialBackoff,
		MaxBackoff:          client.DefaultMaxBackoff,
		MaxHeartBeatWorkers: client.DefaultMaxHeartBeatWorkers,
	}

	if verbose {
		fmt.Printf("Connecting to %s (insecure: %v)\n", serverAddr, insecure)
	}

	return client.NewChronoQueueClient(serverAddr, opts)
}
