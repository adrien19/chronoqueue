"use client";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import {
    Activity,
    CheckCircle,
    Clock,
    AlertCircle,
    TrendingUp,
    Zap,
    Loader2,
} from "lucide-react";
import { format } from "date-fns";
import { useQueues, useRecentMessages } from "@/lib/hooks/useQueues";

// Mock data for recent messages (will be replaced when backend implements message history)
const mockQueues = [
    {
        name: "interview-scheduler",
        description: "Handles interview scheduling and notifications",
        messagesInQueue: 12,
        messagesProcessing: 2,
        messagesCompleted: 145,
        messagesFailed: 3,
        averageProcessingTime: 1.2, // seconds
        status: "active",
        lastProcessed: "2025-10-21T12:30:00Z",
    },
    {
        name: "evaluation-processor",
        description: "Processes evaluation submissions and calculations",
        messagesInQueue: 8,
        messagesProcessing: 1,
        messagesCompleted: 89,
        messagesFailed: 1,
        averageProcessingTime: 2.5,
        status: "active",
        lastProcessed: "2025-10-21T12:28:00Z",
    },
    {
        name: "report-generator",
        description: "Generates consolidated interview reports",
        messagesInQueue: 3,
        messagesProcessing: 1,
        messagesCompleted: 42,
        messagesFailed: 0,
        averageProcessingTime: 5.8,
        status: "active",
        lastProcessed: "2025-10-21T12:25:00Z",
    },
    {
        name: "notification-sender",
        description: "Sends email and SMS notifications",
        messagesInQueue: 15,
        messagesProcessing: 3,
        messagesCompleted: 312,
        messagesFailed: 5,
        averageProcessingTime: 0.8,
        status: "active",
        lastProcessed: "2025-10-21T12:31:00Z",
    },
];

const statusColors = {
    pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    processing: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    completed: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    failed: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
};

const priorityLabels = {
    1: "Low",
    2: "Normal",
    3: "High",
    4: "Urgent",
};

export default function QueuesPage() {
    const { data: queues, isLoading: queuesLoading } = useQueues();
    const { data: recentMessages } = useRecentMessages();

    if (queuesLoading) {
        return (
            <div className="flex items-center justify-center min-h-[400px]">
                <Loader2 className="h-8 w-8 animate-spin text-gray-500" />
            </div>
        );
    }

    const queueList = queues || [];
    const totalMessagesInQueue = queueList.reduce((sum, q) => sum + q.messagesInQueue, 0);
    const totalProcessing = queueList.reduce((sum, q) => sum + q.messagesProcessing, 0);
    const totalCompleted = queueList.reduce((sum, q) => sum + q.messagesCompleted, 0);
    const totalFailed = queueList.reduce((sum, q) => sum + q.messagesFailed, 0);

    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Queue Monitor</h1>
                <p className="text-gray-500">Real-time ChronoQueue status and message tracking</p>
            </div>

            {/* Overall Stats */}
            <div className="grid gap-4 md:grid-cols-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">In Queue</CardTitle>
                        <Clock className="h-4 w-4 text-yellow-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalMessagesInQueue}</div>
                        <p className="text-xs text-gray-500">Waiting to be processed</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Processing</CardTitle>
                        <Activity className="h-4 w-4 text-blue-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalProcessing}</div>
                        <p className="text-xs text-gray-500">Currently being processed</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Completed</CardTitle>
                        <CheckCircle className="h-4 w-4 text-green-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalCompleted}</div>
                        <p className="text-xs text-gray-500">Successfully processed</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Failed</CardTitle>
                        <AlertCircle className="h-4 w-4 text-red-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{totalFailed}</div>
                        <p className="text-xs text-gray-500">Moved to DLQ</p>
                    </CardContent>
                </Card>
            </div>

            {/* Queue Details */}
            <div className="grid gap-6 md:grid-cols-2">
                {queueList.map((queue) => {
                    const total = queue.messagesInQueue + queue.messagesProcessing + queue.messagesCompleted;
                    const successRate = total > 0 ? ((queue.messagesCompleted / total) * 100).toFixed(1) : 0;

                    return (
                        <Card key={queue.name}>
                            <CardHeader>
                                <div className="flex items-center justify-between">
                                    <div className="flex items-center space-x-2">
                                        <Zap className="h-5 w-5 text-blue-600" />
                                        <CardTitle className="text-lg">{queue.name}</CardTitle>
                                    </div>
                                    <Badge variant="default" className="bg-green-500">
                                        {queue.status}
                                    </Badge>
                                </div>
                                <CardDescription>{queue.description}</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-4">
                                <div className="grid grid-cols-2 gap-4">
                                    <div className="space-y-1">
                                        <p className="text-sm text-gray-500">In Queue</p>
                                        <p className="text-2xl font-bold">{queue.messagesInQueue}</p>
                                    </div>
                                    <div className="space-y-1">
                                        <p className="text-sm text-gray-500">Processing</p>
                                        <p className="text-2xl font-bold text-blue-600">{queue.messagesProcessing}</p>
                                    </div>
                                    <div className="space-y-1">
                                        <p className="text-sm text-gray-500">Completed</p>
                                        <p className="text-xl font-semibold text-green-600">{queue.messagesCompleted}</p>
                                    </div>
                                    <div className="space-y-1">
                                        <p className="text-sm text-gray-500">Failed</p>
                                        <p className="text-xl font-semibold text-red-600">{queue.messagesFailed}</p>
                                    </div>
                                </div>

                                <div className="space-y-2">
                                    <div className="flex items-center justify-between text-sm">
                                        <span className="text-gray-500">Success Rate</span>
                                        <span className="font-medium">{successRate}%</span>
                                    </div>
                                    <Progress value={Number(successRate)} className="h-2" />
                                </div>

                                <div className="flex items-center justify-between pt-2 text-xs text-gray-500">
                                    <span>Avg: {queue.averageProcessingTime}s</span>
                                    <span>Last: {format(new Date(queue.lastProcessed), "HH:mm:ss")}</span>
                                </div>
                            </CardContent>
                        </Card>
                    );
                })}
            </div>

            {/* Recent Messages */}
            <Card>
                <CardHeader>
                    <CardTitle>Recent Messages</CardTitle>
                    <CardDescription>Latest messages across all queues</CardDescription>
                </CardHeader>
                <CardContent>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Message ID</TableHead>
                                <TableHead>Queue</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead>Subject</TableHead>
                                <TableHead>Priority</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Created</TableHead>
                                <TableHead>Processed</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {(recentMessages && recentMessages.length > 0) ? (
                                recentMessages.map((message) => (
                                    <TableRow key={message.id}>
                                        <TableCell className="font-mono text-xs">{message.id}</TableCell>
                                        <TableCell className="text-sm">{message.queueName}</TableCell>
                                        <TableCell className="text-sm">{message.type.replace(/_/g, " ")}</TableCell>
                                        <TableCell className="text-sm">{message.subject || "N/A"}</TableCell>
                                        <TableCell>
                                            <Badge
                                                variant={message.priority >= 3 ? "destructive" : "secondary"}
                                            >
                                                {priorityLabels[message.priority as keyof typeof priorityLabels] || "Normal"}
                                            </Badge>
                                        </TableCell>
                                        <TableCell>
                                            <Badge className={statusColors[message.status as keyof typeof statusColors]}>
                                                {message.status}
                                            </Badge>
                                        </TableCell>
                                        <TableCell className="text-xs text-gray-500">
                                            {format(new Date(message.createdAt), "HH:mm:ss")}
                                        </TableCell>
                                        <TableCell className="text-xs text-gray-500">
                                            {message.processedAt
                                                ? format(new Date(message.processedAt), "HH:mm:ss")
                                                : "-"}
                                        </TableCell>
                                    </TableRow>
                                ))
                            ) : (
                                <TableRow>
                                    <TableCell colSpan={8} className="text-center text-gray-500">
                                        No recent messages
                                    </TableCell>
                                </TableRow>
                            )}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>

            {/* ChronoQueue Features Highlight */}
            <Card className="border-blue-200 bg-blue-50 dark:border-blue-900 dark:bg-blue-950">
                <CardHeader>
                    <div className="flex items-center space-x-2">
                        <TrendingUp className="h-5 w-5 text-blue-600" />
                        <CardTitle>ChronoQueue Features in Action</CardTitle>
                    </div>
                </CardHeader>
                <CardContent>
                    <div className="grid gap-4 md:grid-cols-2">
                        <div className="space-y-2">
                            <h4 className="font-semibold text-blue-900 dark:text-blue-100">
                                Priority Processing
                            </h4>
                            <p className="text-sm text-blue-800 dark:text-blue-200">
                                Urgent messages (priority 4) are processed before normal priority messages,
                                ensuring critical interviews are handled first.
                            </p>
                        </div>
                        <div className="space-y-2">
                            <h4 className="font-semibold text-blue-900 dark:text-blue-100">
                                Scheduled Delivery
                            </h4>
                            <p className="text-sm text-blue-800 dark:text-blue-200">
                                Interview reminders and notifications are scheduled for specific times using
                                ChronoQueue's time-based scheduling.
                            </p>
                        </div>
                        <div className="space-y-2">
                            <h4 className="font-semibold text-blue-900 dark:text-blue-100">
                                Automatic Retries
                            </h4>
                            <p className="text-sm text-blue-800 dark:text-blue-200">
                                Failed messages are automatically retried up to 3 times before being moved
                                to the Dead Letter Queue (DLQ).
                            </p>
                        </div>
                        <div className="space-y-2">
                            <h4 className="font-semibold text-blue-900 dark:text-blue-100">
                                Message Persistence
                            </h4>
                            <p className="text-sm text-blue-800 dark:text-blue-200">
                                All messages are persisted in Redis, ensuring no data loss even if workers
                                restart or crash.
                            </p>
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
