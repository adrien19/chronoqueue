"use client";

import { useMemo } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Calendar, Users, FileText, Clock } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { format, parseISO } from "date-fns";
import { apiClient } from "@/lib/api/client";
import { LoadingState } from "@/components/ui/loading-state";
import { ErrorState } from "@/components/ui/error-state";
import { EmptyState } from "@/components/ui/empty-state";

function StatCard({
    title,
    value,
    description,
    icon: Icon,
    isLoading
}: {
    title: string;
    value: number | string;
    description: string;
    icon: any;
    isLoading: boolean;
}) {
    if (isLoading) {
        return (
            <Card>
                <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">{title}</CardTitle>
                    <Icon className="h-4 w-4 text-muted-foreground" />
                </CardHeader>
                <CardContent>
                    <LoadingState rows={1} />
                </CardContent>
            </Card>
        );
    }

    return (
        <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">{title}</CardTitle>
                <Icon className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
                <div className="text-2xl font-bold">{value}</div>
                <p className="text-xs text-muted-foreground">{description}</p>
            </CardContent>
        </Card>
    );
}

export default function DashboardPage() {
    // Fetch dashboard stats
    const { data: stats, isLoading: statsLoading, error: statsError } = useQuery({
        queryKey: ['dashboardStats'],
        queryFn: () => apiClient.getDashboardStats(),
        refetchInterval: 30000,
    });

    // Fetch recent interviews
    const { data: interviewsResponse, isLoading: interviewsLoading } = useQuery({
        queryKey: ['recentInterviews'],
        queryFn: () => apiClient.listInterviews({ page: 1, pageSize: 5 }),
        refetchInterval: 30000,
    });

    // Fetch pending evaluations
    const { data: pendingEvals, isLoading: evalsLoading } = useQuery({
        queryKey: ['pendingEvaluations'],
        queryFn: () => apiClient.getPendingEvaluations(),
        refetchInterval: 30000,
    });

    // Fetch recent reports
    const { data: reportsResponse, isLoading: reportsLoading } = useQuery({
        queryKey: ['recentReports'],
        queryFn: () => apiClient.listReports({ page: 1, pageSize: 5 }),
        refetchInterval: 30000,
    });

    const recentInterviews = interviewsResponse?.data || [];
    const pendingEvaluations = pendingEvals || [];
    const recentReports = reportsResponse?.data || [];

    // Format date safely
    const formatDate = (dateString: string) => {
        try {
            return format(parseISO(dateString), "MMM dd, yyyy 'at' HH:mm");
        } catch {
            return "Invalid date";
        }
    };

    const getStatusColor = (status: string) => {
        const colors: Record<string, string> = {
            pending: "bg-yellow-100 text-yellow-800",
            scheduled: "bg-blue-100 text-blue-800",
            in_progress: "bg-purple-100 text-purple-800",
            completed: "bg-green-100 text-green-800",
            cancelled: "bg-red-100 text-red-800",
        };
        return colors[status] || "bg-gray-100 text-gray-800";
    };

    if (statsError) {
        return (
            <div className="p-6">
                <ErrorState
                    message="Failed to load dashboard data. Please try again."
                    retry={() => window.location.reload()}
                />
            </div>
        );
    }

    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
                <p className="text-gray-500">Overview of interview platform activity</p>
            </div>

            {/* Stats Cards */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                <StatCard
                    title="Total Interviews"
                    value={stats?.totalInterviews || 0}
                    description="All scheduled interviews"
                    icon={Calendar}
                    isLoading={statsLoading}
                />
                <StatCard
                    title="Pending Evaluations"
                    value={stats?.pendingEvaluations || 0}
                    description="Awaiting review"
                    icon={Users}
                    isLoading={statsLoading}
                />
                <StatCard
                    title="Completed Reports"
                    value={stats?.completedReports || 0}
                    description="Generated reports"
                    icon={FileText}
                    isLoading={statsLoading}
                />
                <StatCard
                    title="Avg Evaluation Time"
                    value={`${stats?.averageEvaluationTime?.toFixed(1) || 0}h`}
                    description="Average processing time"
                    icon={Clock}
                    isLoading={statsLoading}
                />
            </div>

            {/* Recent Activity */}
            <div className="grid gap-4 md:grid-cols-2">
                {/* Recent Interviews */}
                <Card>
                    <CardHeader>
                        <CardTitle>Recent Interviews</CardTitle>
                        <CardDescription>Latest scheduled interviews</CardDescription>
                    </CardHeader>
                    <CardContent>
                        {interviewsLoading ? (
                            <LoadingState rows={3} type="list" />
                        ) : recentInterviews.length === 0 ? (
                            <EmptyState
                                title="No interviews yet"
                                description="Schedule your first interview to get started"
                                action={{
                                    label: "Schedule Interview",
                                    onClick: () => window.location.href = "/dashboard/interviews/new"
                                }}
                            />
                        ) : (
                            <div className="space-y-4">
                                {recentInterviews.slice(0, 3).map((interview) => (
                                    <div key={interview.id} className="flex items-start justify-between border-b pb-3 last:border-0 last:pb-0">
                                        <div className="space-y-1">
                                            <p className="font-medium">{interview.candidateName}</p>
                                            <p className="text-sm text-gray-500">{interview.position}</p>
                                            <p className="text-xs text-gray-400">{formatDate(interview.scheduledAt)}</p>
                                        </div>
                                        <Badge className={getStatusColor(interview.status)}>
                                            {interview.status}
                                        </Badge>
                                    </div>
                                ))}
                                <Button variant="outline" className="w-full" asChild>
                                    <Link href="/dashboard/interviews">View All Interviews</Link>
                                </Button>
                            </div>
                        )}
                    </CardContent>
                </Card>

                {/* Pending Evaluations */}
                <Card>
                    <CardHeader>
                        <CardTitle>Pending Evaluations</CardTitle>
                        <CardDescription>Waiting for review</CardDescription>
                    </CardHeader>
                    <CardContent>
                        {evalsLoading ? (
                            <LoadingState rows={3} type="list" />
                        ) : pendingEvaluations.length === 0 ? (
                            <EmptyState
                                title="No pending evaluations"
                                description="All evaluations are up to date"
                            />
                        ) : (
                            <div className="space-y-4">
                                {pendingEvaluations.slice(0, 3).map((evaluation) => (
                                    <div key={evaluation.id} className="flex items-start justify-between border-b pb-3 last:border-0 last:pb-0">
                                        <div className="space-y-1">
                                            <p className="font-medium">Interview #{evaluation.interviewId.slice(0, 8)}</p>
                                            <p className="text-sm text-gray-500">{evaluation.evaluatorName}</p>
                                            <p className="text-xs text-gray-400">{formatDate(evaluation.createdAt)}</p>
                                        </div>
                                        <Badge variant="outline">{evaluation.status}</Badge>
                                    </div>
                                ))}
                                <Button variant="outline" className="w-full" asChild>
                                    <Link href="/dashboard/evaluations">View All Evaluations</Link>
                                </Button>
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>

            {/* Recent Reports */}
            <Card>
                <CardHeader>
                    <CardTitle>Recent Reports</CardTitle>
                    <CardDescription>Latest generated reports</CardDescription>
                </CardHeader>
                <CardContent>
                    {reportsLoading ? (
                        <LoadingState rows={3} type="list" />
                    ) : recentReports.length === 0 ? (
                        <EmptyState
                            title="No reports yet"
                            description="Reports will appear here once generated"
                        />
                    ) : (
                        <div className="space-y-4">
                            {recentReports.slice(0, 5).map((report) => (
                                <div key={report.id} className="flex items-start justify-between border-b pb-3 last:border-0 last:pb-0">
                                    <div className="space-y-1 flex-1">
                                        <p className="font-medium">{report.candidateName}</p>
                                        <p className="text-sm text-gray-500">{report.position}</p>
                                        <div className="flex gap-2 text-xs text-gray-400">
                                            <span>{report.evaluationCount} evaluations</span>
                                            <span>•</span>
                                            <span>Overall: {report.averageOverall?.toFixed(1) || 0}/10</span>
                                        </div>
                                    </div>
                                    <Badge className={getStatusColor(report.status)}>
                                        {report.status}
                                    </Badge>
                                </div>
                            ))}
                            <Button variant="outline" className="w-full" asChild>
                                <Link href="/dashboard/reports">View All Reports</Link>
                            </Button>
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
