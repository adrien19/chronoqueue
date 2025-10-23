"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { AlertCircle, CheckCircle, Clock, Loader2 } from "lucide-react";
import { format } from "date-fns";
import { useEvaluationStats } from "@/lib/hooks/useEvaluations";

const recommendationColors = {
    strong_hire: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    hire: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    maybe: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    no_hire: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
};

const recommendationLabels = {
    strong_hire: "Strong Hire",
    hire: "Hire",
    maybe: "Maybe",
    no_hire: "No Hire",
};

export default function EvaluationsPage() {
    const [isMounted, setIsMounted] = useState(false);
    const { data: stats, isLoading, error } = useEvaluationStats();

    useEffect(() => {
        setIsMounted(true);
    }, []);

    if (isLoading) {
        return (
            <div className="flex h-[50vh] items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-gray-500" />
            </div>
        );
    }

    if (error) {
        return (
            <div className="flex h-[50vh] items-center justify-center">
                <p className="text-red-500">Failed to load evaluations</p>
            </div>
        );
    }

    const pendingEvaluations = stats?.pending || [];
    const completedEvaluations = stats?.completed || [];
    const avgTimeHours = stats?.avgTimeHours || 0;

    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Evaluations</h1>
                <p className="text-gray-500">Review and submit interview evaluations</p>
            </div>

            {/* Stats */}
            <div className="grid gap-4 md:grid-cols-3">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Pending</CardTitle>
                        <AlertCircle className="h-4 w-4 text-yellow-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{pendingEvaluations.length}</div>
                        <p className="text-xs text-gray-500">Awaiting your feedback</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Completed</CardTitle>
                        <CheckCircle className="h-4 w-4 text-green-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{completedEvaluations.length}</div>
                        <p className="text-xs text-gray-500">This week</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Avg. Time</CardTitle>
                        <Clock className="h-4 w-4 text-blue-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">2.5h</div>
                        <p className="text-xs text-gray-500">From interview to submission</p>
                    </CardContent>
                </Card>
            </div>

            {/* Pending Evaluations */}
            <Card>
                <CardHeader>
                    <CardTitle>Pending Evaluations ({pendingEvaluations.length})</CardTitle>
                    <CardDescription>Please complete these evaluations as soon as possible</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="space-y-4">
                        {pendingEvaluations.map((evaluation) => (
                            <div
                                key={evaluation.id}
                                className="flex items-center justify-between rounded-lg border p-4 transition-colors hover:bg-gray-50 dark:hover:bg-gray-800"
                            >
                                <div className="flex-1 space-y-2">
                                    <div className="flex items-center space-x-2">
                                        <h3 className="font-semibold">{evaluation.candidateName || "Unknown Candidate"}</h3>
                                        <Badge variant="secondary" className="flex items-center gap-1">
                                            <AlertCircle className="h-3 w-3" />
                                            Pending
                                        </Badge>
                                    </div>
                                    <p className="text-sm text-gray-600">{evaluation.position || "Unknown Position"}</p>
                                    <div className="flex items-center space-x-4 text-xs text-gray-500">
                                        {evaluation.interviewDate && (
                                            <span>
                                                Interview: {isMounted ? format(new Date(evaluation.interviewDate), "MMM dd, yyyy HH:mm") : "Loading..."}
                                            </span>
                                        )}
                                        <span>
                                            Created: {isMounted ? format(new Date(evaluation.createdAt), "MMM dd, yyyy") : "Loading..."}
                                        </span>
                                    </div>
                                </div>
                                <div className="flex items-center space-x-2">
                                    <Button variant="outline" size="sm" asChild>
                                        <Link href={`/dashboard/interviews/${evaluation.interviewId}`}>
                                            View Interview
                                        </Link>
                                    </Button>
                                    <Button size="sm" asChild>
                                        <Link href={`/dashboard/interviews/${evaluation.interviewId}/evaluate`}>
                                            Submit Evaluation
                                        </Link>
                                    </Button>
                                </div>
                            </div>
                        ))}
                        {pendingEvaluations.length === 0 && (
                            <div className="flex h-32 items-center justify-center rounded-lg border-2 border-dashed">
                                <p className="text-sm text-gray-500">No pending evaluations</p>
                            </div>
                        )}
                    </div>
                </CardContent>
            </Card>

            {/* Completed Evaluations */}
            <Card>
                <CardHeader>
                    <CardTitle>Recently Completed ({completedEvaluations.length})</CardTitle>
                    <CardDescription>Your recently submitted evaluations</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="space-y-4">
                        {completedEvaluations.map((evaluation) => (
                            <div
                                key={evaluation.id}
                                className="flex items-center justify-between rounded-lg border p-4"
                            >
                                <div className="flex-1 space-y-2">
                                    <div className="flex items-center space-x-2">
                                        <h3 className="font-semibold">{evaluation.candidateName || "Unknown Candidate"}</h3>
                                        {evaluation.recommendation && (
                                            <Badge
                                                className={
                                                    recommendationColors[evaluation.recommendation as keyof typeof recommendationColors]
                                                }
                                            >
                                                {recommendationLabels[evaluation.recommendation as keyof typeof recommendationLabels]}
                                            </Badge>
                                        )}
                                    </div>
                                    <p className="text-sm text-gray-600">{evaluation.position || "Unknown Position"}</p>
                                    <div className="flex items-center space-x-4">
                                        <div className="flex-1">
                                            <div className="mb-1 flex items-center justify-between text-xs">
                                                <span className="text-gray-500">Overall Score</span>
                                                <span className="font-medium">
                                                    {evaluation.overallScore ? `${evaluation.overallScore}/100` : "N/A"}
                                                </span>
                                            </div>
                                            {evaluation.overallScore && (
                                                <Progress value={evaluation.overallScore} className="h-2" />
                                            )}
                                        </div>
                                    </div>
                                    <p className="text-xs text-gray-500">
                                        Submitted: {isMounted ? format(new Date(evaluation.updatedAt), "MMM dd, yyyy HH:mm") : "Loading..."}
                                    </p>
                                </div>
                                <div className="flex items-center space-x-2">
                                    <Button variant="ghost" size="sm" asChild>
                                        <Link href={`/dashboard/interviews/${evaluation.interviewId}`}>View Details</Link>
                                    </Button>
                                </div>
                            </div>
                        ))}
                        {completedEvaluations.length === 0 && (
                            <div className="flex h-32 items-center justify-center rounded-lg border-2 border-dashed">
                                <p className="text-sm text-gray-500">No completed evaluations</p>
                            </div>
                        )}
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
