package llm

// Task decomposition prompts for different task types

const (
	// SystemPrompt is the base system prompt for the LLM
	SystemPrompt = `You are an AI task decomposition expert. Your role is to break down complex tasks into smaller, manageable subtasks that can be executed by specialized agents.

Available agents:
- web-search: Performs web research, API calls, and data gathering
- code-analyzer: Analyzes code repositories, performs code reviews
- data-processor: Processes data, generates statistics and visualizations
- aggregator: Synthesizes results from multiple agents into a final report

For each task, identify the necessary subtasks and assign them to the appropriate agents. Consider:
1. Which agents are needed for this task
2. Dependencies between subtasks (some must complete before others)
3. Priority levels (inherit from parent task or adjust as needed)
4. Specific inputs each agent needs

Return a structured decomposition with subtasks, their agent assignments, and dependencies.`

	// CompetitorAnalysisPrompt is the prompt template for competitor analysis tasks
	CompetitorAnalysisPrompt = `Decompose this competitor analysis task into subtasks:

Task: {{.Description}}
Company URL: {{.CompanyURL}}
Include Code Analysis: {{.IncludeCodeAnalysis}}
Include Traffic Analysis: {{.IncludeTrafficAnalysis}}

Create subtasks for:
1. Web search to gather competitor information
2. Code analysis if requested (repository analysis)
3. Traffic and SEO analysis
4. Social media presence analysis
5. Final aggregation of all findings`

	// MarketResearchPrompt is the prompt template for market research tasks
	MarketResearchPrompt = `Decompose this market research task into subtasks:

Task: {{.Description}}
Market Segment: {{.MarketSegment}}
Geographic Focus: {{.GeographicFocus}}

Create subtasks for:
1. Market size and trends research
2. Key player identification
3. Customer segment analysis
4. Pricing and business model analysis
5. Final market research report aggregation`

	// CodeReviewPrompt is the prompt template for code review tasks
	CodeReviewPrompt = `Decompose this code review task into subtasks:

Task: {{.Description}}
Repository URL: {{.RepositoryURL}}
Focus Areas: {{.FocusAreas}}

Create subtasks for:
1. Repository structure analysis
2. Code quality and best practices review
3. Security vulnerability scanning
4. Performance analysis
5. Final code review report aggregation`

	// SynthesisPrompt is the prompt for result synthesis
	SynthesisPrompt = `Synthesize the following agent results into a comprehensive report:

Task: {{.Description}}
Results from {{.NumResults}} agents:
{{range .Results}}
Agent: {{.AgentType}}
Success: {{.Success}}
Data: {{.Data}}
{{end}}

Create a well-structured report with:
1. Executive summary
2. Key findings organized by topic
3. Detailed sections for each agent's contributions
4. Conclusions and recommendations`
)
