"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { FileText, Download, Send, CheckCircle, Clock, Loader2 } from "lucide-react";
import { format } from "date-fns";
import { useReportsStats, useSendReport, useDownloadReport } from "@/lib/hooks/useReports";
import { toast } from "sonner";

const statusColors = {
    pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    ready: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    sent: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
};

const recommendationColors = {
    strong_hire: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    hire: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    maybe: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    no_hire: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
};

const recommendationLabels = {
    strong_hire: "strong hire",
    hire: "hire",
    maybe: "maybe",
    no_hire: "no hire",
};

export default function ReportsPage() {
    const [isMounted, setIsMounted] = useState(false);
    const { data, isLoading, error } = useReportsStats();
    const sendReportMutation = useSendReport();
    const downloadReportMutation = useDownloadReport();

    useEffect(() => {
        setIsMounted(true);
    }, []);

    if (isLoading) {
        return (
            <div className="flex h-96 items-center justify-center">
                <Loader2 className="h-8 w-8 animate-spin text-gray-500" />
            </div>
        );
    }

    if (error) {
        return (
            <div className="flex h-96 items-center justify-center">
                <p className="text-red-500">Error loading reports. Please try again.</p>
            </div>
        );
    }

    const reports = data?.all || [];
    const readyCount = data?.ready.length || 0;
    const pendingCount = data?.pending.length || 0;

    const handleSendReport = async (reportId: string) => {
        try {
            await sendReportMutation.mutateAsync(reportId);
        } catch (error) {
            // Error handled by mutation
        }
    };

    const handleDownloadReport = async (reportId: string) => {
        try {
            await downloadReportMutation.mutateAsync(reportId);
        } catch (error) {
            // Error handled by mutation
        }
    };
    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Reports</h1>
                <p className="text-gray-500">View and manage interview evaluation reports</p>
            </div>

            {/* Stats */}
            <div className="grid gap-4 md:grid-cols-3">
                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Total Reports</CardTitle>
                        <FileText className="h-4 w-4 text-gray-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{reports.length}</div>
                        <p className="text-xs text-gray-500">All time</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Ready to Send</CardTitle>
                        <CheckCircle className="h-4 w-4 text-green-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{readyCount}</div>
                        <p className="text-xs text-gray-500">Available for distribution</p>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                        <CardTitle className="text-sm font-medium">Pending</CardTitle>
                        <Clock className="h-4 w-4 text-yellow-500" />
                    </CardHeader>
                    <CardContent>
                        <div className="text-2xl font-bold">{pendingCount}</div>
                        <p className="text-xs text-gray-500">Waiting for evaluations</p>
                    </CardContent>
                </Card>
            </div>

            {/* Reports Table */}
            <Card>
                <CardHeader>
                    <CardTitle>All Reports</CardTitle>
                    <CardDescription>Comprehensive evaluation reports for all interviews</CardDescription>
                </CardHeader>
                <CardContent>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Candidate</TableHead>
                                <TableHead>Position</TableHead>
                                <TableHead>Interview Date</TableHead>
                                <TableHead>Evaluations</TableHead>
                                <TableHead>Avg Score</TableHead>
                                <TableHead>Recommendation</TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead className="text-right">Actions</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {reports.map((report) => (
                                <TableRow key={report.id}>
                                    <TableCell className="font-medium">{report.candidateName}</TableCell>
                                    <TableCell>{report.position}</TableCell>
                                    <TableCell>
                                        {isMounted ? format(new Date(report.interviewDate), "MMM dd, yyyy") : "Loading..."}
                                    </TableCell>
                                    <TableCell>
                                        <Badge variant="outline">{report.evaluationCount} evaluators</Badge>
                                    </TableCell>
                                    <TableCell>
                                        {report.averageOverall > 0 ? (
                                            <span className="font-medium">{Math.round(report.averageOverall)}/100</span>
                                        ) : (
                                            <span className="text-gray-400">-</span>
                                        )}
                                    </TableCell>
                                    <TableCell>
                                        {report.finalRecommendation ? (
                                            <Badge
                                                className={
                                                    recommendationColors[report.finalRecommendation as keyof typeof recommendationColors] || "bg-gray-100 text-gray-800"
                                                }
                                            >
                                                {recommendationLabels[report.finalRecommendation as keyof typeof recommendationLabels] || report.finalRecommendation}
                                            </Badge>
                                        ) : (
                                            <span className="text-gray-400">-</span>
                                        )}
                                    </TableCell>
                                    <TableCell>
                                        <Badge className={statusColors[report.status as keyof typeof statusColors]}>
                                            {report.status}
                                        </Badge>
                                    </TableCell>
                                    <TableCell className="text-right">
                                        <div className="flex justify-end space-x-2">
                                            {report.status === "ready" && (
                                                <>
                                                    <Button
                                                        variant="ghost"
                                                        size="sm"
                                                        onClick={() => handleDownloadReport(report.id)}
                                                        disabled={downloadReportMutation.isPending}
                                                    >
                                                        <Download className="mr-1 h-3 w-3" />
                                                        PDF
                                                    </Button>
                                                    <Button
                                                        variant="ghost"
                                                        size="sm"
                                                        onClick={() => handleSendReport(report.id)}
                                                        disabled={sendReportMutation.isPending}
                                                    >
                                                        <Send className="mr-1 h-3 w-3" />
                                                        Send
                                                    </Button>
                                                </>
                                            )}
                                            {report.status === "sent" && (
                                                <Button
                                                    variant="ghost"
                                                    size="sm"
                                                    onClick={() => handleDownloadReport(report.id)}
                                                    disabled={downloadReportMutation.isPending}
                                                >
                                                    <Download className="mr-1 h-3 w-3" />
                                                    PDF
                                                </Button>
                                            )}
                                            {report.status === "pending" && (
                                                <Button variant="ghost" size="sm" disabled>
                                                    Waiting...
                                                </Button>
                                            )}
                                            <Button variant="outline" size="sm" asChild>
                                                <Link href={`/dashboard/reports/${report.id}`}>View</Link>
                                            </Button>
                                        </div>
                                    </TableCell>
                                </TableRow>
                            ))}
                            {reports.length === 0 && (
                                <TableRow>
                                    <TableCell colSpan={8} className="h-32 text-center">
                                        <p className="text-gray-500">No reports found</p>
                                    </TableCell>
                                </TableRow>
                            )}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>
        </div>
    );
}
