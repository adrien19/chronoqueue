"use client";

import { useMemo } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Calendar, Users, FileText, Clock, TrendingUp } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import {
    Line,
    LineChart,
    Bar,
    BarChart,
    Pie,
    PieChart,
    Cell,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";
import {
    getDashboardStats,
    listInterviews,
    getPendingEvaluations,
    listReports,
    type DashboardStats,
    type Interview,
    type Evaluation,
    type Report,
} from "@/lib/api/api";
import { evaluationsApi } from "@/lib/api/evaluations";
import { interviewsApi } from "@/lib/api/interviews";

export default function DashboardPage() {
    // Fetch dashboard stats
    const { data: stats, isLoading: statsLoading, error: statsError } = useQuery({
        queryKey: ['dashboardStats'],
        queryFn: getDashboardStats,
        refetchInterval: 30000, // Refetch every 30 seconds
    });

    // Fetch recent interviews
    const { data: interviews, isLoading: interviewsLoading } = useQuery({
        queryKey: ['interviews', { limit: 5 }],
        queryFn: () => listInterviews({ limit: 5 }),
        refetchInterval: 30000,
    });

    // Fetch all interviews for status distribution
    const { data: allInterviewsResponse } = useQuery({
        queryKey: ['allInterviews'],
        queryFn: () => interviewsApi.getAll({ pageSize: 1000 }),
        refetchInterval: 30000,
    });

    // Fetch all evaluations for average scores calculation
    const { data: allEvaluationsResponse } = useQuery({
        queryKey: ['allEvaluations'],
        queryFn: () => evaluationsApi.list(undefined, 1, 1000),
        refetchInterval: 30000,
    });

    // Fetch pending evaluations
    const { data: evaluations, isLoading: evaluationsLoading } = useQuery({
        queryKey: ['pendingEvaluations'],
        queryFn: getPendingEvaluations,
        refetchInterval: 30000,
    });

    // Fetch recent reports
    const { data: reports, isLoading: reportsLoading } = useQuery({
        queryKey: ['reports', { limit: 5 }],
        queryFn: () => listReports({ limit: 5 }),
        refetchInterval: 30000,
    });

    const recentInterviews = interviews?.slice(0, 3) || [];
    const pendingEvaluations = evaluations?.slice(0, 2) || [];

    // Calculate status distribution from real data
    const statusDistribution = useMemo(() => {
        if (!stats?.interviewsByStatus) return [];

        const statusMap = stats.interviewsByStatus;
        return [
            { name: "Scheduled", value: statusMap.scheduled || 0, color: "#3b82f6" },
            { name: "Completed", value: statusMap.completed || 0, color: "#10b981" },
            { name: "Cancelled", value: statusMap.cancelled || 0, color: "#ef4444" },
        ].filter(item => item.value > 0); // Only show statuses with data
    }, [stats]);

    // Calculate average evaluation scores from real data
    const evaluationScores = useMemo(() => {
        const evals = allEvaluationsResponse?.data || [];
        if (evals.length === 0) return [];

        const completedEvals = evals.filter((e: any) => e.status === 'completed');
        if (completedEvals.length === 0) return [];

        const avgTechnical = completedEvals.reduce((sum: number, e: any) => sum + (e.technicalScore || 0), 0) / completedEvals.length;
        const avgCommunication = completedEvals.reduce((sum: number, e: any) => sum + (e.communicationScore || 0), 0) / completedEvals.length;
        const avgCultureFit = completedEvals.reduce((sum: number, e: any) => sum + (e.cultureFitScore || 0), 0) / completedEvals.length;
        const avgOverall = completedEvals.reduce((sum: number, e: any) => sum + (e.overallScore || 0), 0) / completedEvals.length;

        return [
            { category: "Technical", score: parseFloat(avgTechnical.toFixed(1)) },
            { category: "Communication", score: parseFloat(avgCommunication.toFixed(1)) },
            { category: "Culture Fit", score: parseFloat(avgCultureFit.toFixed(1)) },
            { category: "Overall", score: parseFloat(avgOverall.toFixed(1)) },
        ];
    }, [allEvaluationsResponse]);

    // Calculate interview trends from real data (last 6 months)
    const interviewTrendData = useMemo(() => {
        const allInterviews = allInterviewsResponse?.data || [];
        if (allInterviews.length === 0) return [];

        const allEvals = allEvaluationsResponse?.data || [];

        // Get last 6 months
        const now = new Date();
        const months: Array<{
            date: Date;
            month: string;
            interviews: number;
            evaluations: number;
        }> = [];

        for (let i = 5; i >= 0; i--) {
            const date = new Date(now.getFullYear(), now.getMonth() - i, 1);
            months.push({
                date: date,
                month: date.toLocaleDateString('en-US', { month: 'short' }),
                interviews: 0,
                evaluations: 0,
            });
        }

        // Count interviews and evaluations per month
        allInterviews.forEach((interview: any) => {
            const interviewDate = new Date(interview.scheduledAt);
            const monthData = months.find(m =>
                m.date.getMonth() === interviewDate.getMonth() &&
                m.date.getFullYear() === interviewDate.getFullYear()
            );
            if (monthData) {
                monthData.interviews++;
            }
        });

        allEvals.forEach((evaluation: any) => {
            const evalDate = new Date(evaluation.createdAt);
            const monthData = months.find(m =>
                m.date.getMonth() === evalDate.getMonth() &&
                m.date.getFullYear() === evalDate.getFullYear()
            );
            if (monthData) {
                monthData.evaluations++;
            }
        });

        return months.map(({ month, interviews, evaluations }) => ({
            month,
            interviews,
            evaluations,
        }));
    }, [allInterviewsResponse, allEvaluationsResponse]);

    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
                <p className="text-gray-500">Welcome back! Here's your interview overview.</p>
            </div>

            {/* Stats Grid */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Total Interviews</CardTitle>
                        <Calendar className="h-4 w-4 text-gray-500" />
                    </CardHeader>
                    <CardContent>
                        {statsLoading ? (
                            <Skeleton className="h-8 w-20" />
                        ) : (
                            <>
                                <div className="text-2xl font-bold">{stats?.totalInterviews || 0}</div>
                                <p className="text-xs text-muted-foreground">
                                    Across all statuses
                                </p>
                            </>
                        )}
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Pending Evaluations</CardTitle>
                        <Users className="h-4 w-4 text-gray-500" />
                    </CardHeader>
                    <CardContent>
                        {statsLoading ? (
                            <Skeleton className="h-8 w-20" />
                        ) : (
                            <>
                                <div className="text-2xl font-bold">{stats?.pendingEvaluations || 0}</div>
                                <p className="text-xs text-yellow-600">
                                    Awaiting submission
                                </p>
                            </>
                        )}
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Completed Reports</CardTitle>
                        <FileText className="h-4 w-4 text-gray-500" />
                    </CardHeader>
                    <CardContent>
                        {statsLoading ? (
                            <Skeleton className="h-8 w-20" />
                        ) : (
                            <>
                                <div className="text-2xl font-bold">{stats?.completedReports || 0}</div>
                                <p className="text-xs text-green-600">
                                    Ready to send
                                </p>
                            </>
                        )}
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Completed Evaluations</CardTitle>
                        <Clock className="h-4 w-4 text-gray-500" />
                    </CardHeader>
                    <CardContent>
                        {statsLoading ? (
                            <Skeleton className="h-8 w-20" />
                        ) : (
                            <>
                                <div className="text-2xl font-bold">{stats?.completedEvaluations || stats?.evaluationsByStatus?.completed || 0}</div>
                                <p className="text-xs text-green-600">
                                    Submitted
                                </p>
                            </>
                        )}
                    </CardContent>
                </Card>
            </div>

            {/* Charts Grid */}
            <div className="grid gap-4 md:grid-cols-2">
                {/* Interview Trend */}
                <Card>
                    <CardHeader>
                        <CardTitle>Interview Trends</CardTitle>
                        <CardDescription>Monthly interview and evaluation progress</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <LineChart data={interviewTrendData}>
                                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                                <XAxis dataKey="month" className="text-xs" />
                                <YAxis className="text-xs" />
                                <Tooltip
                                    contentStyle={{
                                        backgroundColor: "hsl(var(--background))",
                                        border: "1px solid hsl(var(--border))",
                                        borderRadius: "6px",
                                    }}
                                />
                                <Legend />
                                <Line
                                    type="monotone"
                                    dataKey="interviews"
                                    stroke="#3b82f6"
                                    strokeWidth={2}
                                    name="Interviews"
                                />
                                <Line
                                    type="monotone"
                                    dataKey="evaluations"
                                    stroke="#10b981"
                                    strokeWidth={2}
                                    name="Evaluations"
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>

                {/* Status Distribution */}
                <Card>
                    <CardHeader>
                        <CardTitle>Interview Status</CardTitle>
                        <CardDescription>Distribution of interview states</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <PieChart>
                                <Pie
                                    data={statusDistribution}
                                    cx="50%"
                                    cy="50%"
                                    labelLine={false}
                                    label={({ name, percent }: any) => `${name} ${(percent * 100).toFixed(0)}%`}
                                    outerRadius={80}
                                    fill="#8884d8"
                                    dataKey="value"
                                >
                                    {statusDistribution.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={entry.color} />
                                    ))}
                                </Pie>
                                <Tooltip
                                    contentStyle={{
                                        backgroundColor: "hsl(var(--background))",
                                        border: "1px solid hsl(var(--border))",
                                        borderRadius: "6px",
                                    }}
                                />
                            </PieChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>

                {/* Average Evaluation Scores */}
                <Card className="md:col-span-2">
                    <CardHeader>
                        <CardTitle>Average Evaluation Scores</CardTitle>
                        <CardDescription>Performance across key evaluation categories</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <ResponsiveContainer width="100%" height={300}>
                            <BarChart data={evaluationScores}>
                                <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                                <XAxis dataKey="category" className="text-xs" />
                                <YAxis domain={[0, 10]} className="text-xs" />
                                <Tooltip
                                    contentStyle={{
                                        backgroundColor: "hsl(var(--background))",
                                        border: "1px solid hsl(var(--border))",
                                        borderRadius: "6px",
                                    }}
                                />
                                <Bar dataKey="score" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                            </BarChart>
                        </ResponsiveContainer>
                    </CardContent>
                </Card>
            </div>

            {/* Two Column Layout */}
            <div className="grid gap-6 md:grid-cols-2">
                {/* Upcoming Interviews */}
                <Card>
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <div>
                                <CardTitle>Upcoming Interviews</CardTitle>
                                <CardDescription>Scheduled for the next 48 hours</CardDescription>
                            </div>
                            <Link href="/dashboard/interviews">
                                <Button variant="ghost" size="sm">View All</Button>
                            </Link>
                        </div>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-4">
                            {interviewsLoading ? (
                                <div className="space-y-4">
                                    <Skeleton className="h-24 w-full" />
                                    <Skeleton className="h-24 w-full" />
                                    <Skeleton className="h-24 w-full" />
                                </div>
                            ) : recentInterviews.length === 0 ? (
                                <div className="text-center text-muted-foreground py-8">
                                    No interviews scheduled yet
                                </div>
                            ) : (
                                recentInterviews.map((interview) => (
                                    <div
                                        key={interview.id}
                                        className="flex items-center justify-between rounded-lg border p-4"
                                    >
                                        <div className="flex-1">
                                            <div className="flex items-center space-x-2">
                                                <p className="font-medium">{interview.candidateName}</p>
                                                <Badge
                                                    variant={interview.status === "completed" ? "secondary" : "default"}
                                                >
                                                    {interview.status}
                                                </Badge>
                                            </div>
                                            <p className="text-sm text-gray-500">{interview.position}</p>
                                            <p className="text-xs text-gray-400">
                                                {new Date(interview.scheduledAt).toLocaleString()}
                                            </p>
                                        </div>
                                        <Button variant="outline" size="sm" asChild>
                                            <Link href={`/dashboard/interviews/${interview.id}`}>View</Link>
                                        </Button>
                                    </div>
                                ))
                            )}
                        </div>
                    </CardContent>
                </Card>

                {/* Pending Evaluations */}
                <Card>
                    <CardHeader>
                        <div className="flex items-center justify-between">
                            <div>
                                <CardTitle>Pending Evaluations</CardTitle>
                                <CardDescription>Requires your feedback</CardDescription>
                            </div>
                            <Link href="/dashboard/evaluations">
                                <Button variant="ghost" size="sm">View All</Button>
                            </Link>
                        </div>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-4">
                            {evaluationsLoading ? (
                                <div className="space-y-4">
                                    <Skeleton className="h-24 w-full" />
                                    <Skeleton className="h-24 w-full" />
                                </div>
                            ) : pendingEvaluations.length === 0 ? (
                                <div className="text-center text-muted-foreground py-8">
                                    No pending evaluations
                                </div>
                            ) : (
                                pendingEvaluations.map((evaluation) => (
                                    <div
                                        key={evaluation.id}
                                        className="flex items-center justify-between rounded-lg border p-4"
                                    >
                                        <div className="flex-1">
                                            <div className="flex items-center space-x-2">
                                                <p className="font-medium">{evaluation.evaluator_name}</p>
                                                {evaluation.status === "pending" && (
                                                    <Badge variant="outline">Pending</Badge>
                                                )}
                                            </div>
                                            <p className="text-sm text-gray-500">Interview ID: {evaluation.interview_id}</p>
                                            <p className="text-xs text-gray-400">
                                                Evaluator: {evaluation.evaluator_email}
                                            </p>
                                        </div>
                                        <Button size="sm" asChild>
                                            <Link href={`/dashboard/interviews/${evaluation.interview_id}/evaluate`}>
                                                Evaluate
                                            </Link>
                                        </Button>
                                    </div>
                                ))
                            )}
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Activity Chart Section */}
            <Card>
                <CardHeader>
                    <CardTitle>Interview Activity</CardTitle>
                    <CardDescription>Overview of interviews over the last 30 days</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="flex h-64 items-center justify-center rounded-lg border-2 border-dashed">
                        <div className="text-center">
                            <TrendingUp className="mx-auto h-12 w-12 text-gray-400" />
                            <p className="mt-2 text-sm text-gray-500">
                                Chart will be added with Recharts integration
                            </p>
                        </div>
                    </div>
                </CardContent>
            </Card>

            {/* Queue Status Section */}
            <Card>
                <CardHeader>
                    <div className="flex items-center justify-between">
                        <div>
                            <CardTitle>ChronoQueue Status</CardTitle>
                            <CardDescription>Real-time queue monitoring</CardDescription>
                        </div>
                        <Link href="/dashboard/queues">
                            <Button variant="outline" size="sm">View Details</Button>
                        </Link>
                    </div>
                </CardHeader>
                <CardContent>
                    <div className="grid gap-4 md:grid-cols-3">
                        <div className="rounded-lg border p-4">
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium text-gray-500">Interview Scheduler</span>
                                <Badge variant="default" className="bg-green-500">Active</Badge>
                            </div>
                            <p className="mt-2 text-2xl font-bold">12</p>
                            <p className="text-xs text-gray-500">Messages in queue</p>
                        </div>
                        <div className="rounded-lg border p-4">
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium text-gray-500">Evaluation Processor</span>
                                <Badge variant="default" className="bg-green-500">Active</Badge>
                            </div>
                            <p className="mt-2 text-2xl font-bold">8</p>
                            <p className="text-xs text-gray-500">Messages in queue</p>
                        </div>
                        <div className="rounded-lg border p-4">
                            <div className="flex items-center justify-between">
                                <span className="text-sm font-medium text-gray-500">Report Generator</span>
                                <Badge variant="default" className="bg-green-500">Active</Badge>
                            </div>
                            <p className="mt-2 text-2xl font-bold">3</p>
                            <p className="text-xs text-gray-500">Messages in queue</p>
                        </div>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
