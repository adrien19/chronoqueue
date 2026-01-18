"use client";

import { useState } from "react";
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
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Plus, Search, Calendar } from "lucide-react";
import { format, parseISO } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { apiClient } from "@/lib/api/client";
import { LoadingState } from "@/components/ui/loading-state";
import { EmptyState } from "@/components/ui/empty-state";
import { ErrorState } from "@/components/ui/error-state";
import { InterviewStatus } from "@/lib/types/api";

const statusColors: Record<InterviewStatus, string> = {
    pending: "bg-yellow-100 text-yellow-800",
    scheduled: "bg-blue-100 text-blue-800",
    in_progress: "bg-purple-100 text-purple-800",
    completed: "bg-green-100 text-green-800",
    cancelled: "bg-red-100 text-red-800",
};

export default function InterviewsPage() {
    const [statusFilter, setStatusFilter] = useState<string>("all");
    const [searchQuery, setSearchQuery] = useState("");

    // Fetch interviews using React Query
    const { data: response, isLoading, error, refetch } = useQuery({
        queryKey: ["interviews", statusFilter],
        queryFn: () => apiClient.listInterviews({
            status: statusFilter === "all" ? undefined : statusFilter,
            page: 1,
            pageSize: 100
        }),
        refetchInterval: 30000,
    });

    // Extract interviews from paginated response
    const interviews = response?.data || [];

    // Filter interviews based on search
    const filteredInterviews = interviews.filter((interview) => {
        const matchesSearch =
            interview.candidateName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
            interview.position?.toLowerCase().includes(searchQuery.toLowerCase()) ||
            interview.candidateEmail?.toLowerCase().includes(searchQuery.toLowerCase());
        return matchesSearch;
    });

    // Format date safely
    const formatDate = (dateString: string) => {
        try {
            return format(parseISO(dateString), "MMM dd, yyyy");
        } catch {
            return "Invalid date";
        }
    };

    const formatTime = (dateString: string) => {
        try {
            return format(parseISO(dateString), "HH:mm");
        } catch {
            return "";
        }
    };

    return (
        <div className="space-y-6">
            {/* Page Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Interviews</h1>
                    <p className="text-gray-500">Manage and schedule candidate interviews</p>
                </div>
                <Button asChild>
                    <Link href="/dashboard/interviews/new">
                        <Plus className="mr-2 h-4 w-4" />
                        Schedule Interview
                    </Link>
                </Button>
            </div>

            {/* Filters */}
            <Card>
                <CardHeader>
                    <CardTitle>Filters</CardTitle>
                </CardHeader>
                <CardContent>
                    <div className="flex flex-col gap-4 md:flex-row">
                        <div className="relative flex-1">
                            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
                            <Input
                                type="search"
                                placeholder="Search by candidate, position, or email..."
                                className="pl-10"
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                            />
                        </div>
                        <Select value={statusFilter} onValueChange={setStatusFilter}>
                            <SelectTrigger className="w-full md:w-48">
                                <SelectValue placeholder="Filter by status" />
                            </SelectTrigger>
                            <SelectContent>
                                <SelectItem value="all">All Statuses</SelectItem>
                                <SelectItem value="pending">Pending</SelectItem>
                                <SelectItem value="scheduled">Scheduled</SelectItem>
                                <SelectItem value="in_progress">In Progress</SelectItem>
                                <SelectItem value="completed">Completed</SelectItem>
                                <SelectItem value="cancelled">Cancelled</SelectItem>
                            </SelectContent>
                        </Select>
                    </div>
                </CardContent>
            </Card>

            {/* Interviews Table */}
            <Card>
                <CardHeader>
                    <CardTitle>All Interviews ({filteredInterviews.length})</CardTitle>
                    <CardDescription>
                        {statusFilter === "all"
                            ? "Showing all interviews"
                            : `Showing ${statusFilter} interviews`}
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    {error ? (
                        <ErrorState
                            message="Failed to load interviews. Please try again."
                            retry={() => refetch()}
                        />
                    ) : isLoading ? (
                        <LoadingState rows={5} type="table" />
                    ) : interviews.length === 0 ? (
                        <EmptyState
                            icon={Calendar}
                            title="No interviews yet"
                            description="Get started by scheduling your first interview"
                            action={{
                                label: "Schedule Interview",
                                onClick: () => window.location.href = "/dashboard/interviews/new"
                            }}
                        />
                    ) : filteredInterviews.length === 0 ? (
                        <EmptyState
                            icon={Search}
                            title="No matching interviews"
                            description="Try adjusting your search or filter criteria"
                        />
                    ) : (
                        <div className="overflow-x-auto">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>Candidate</TableHead>
                                        <TableHead>Position</TableHead>
                                        <TableHead>Scheduled</TableHead>
                                        <TableHead>Duration</TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Interviewers</TableHead>
                                        <TableHead className="text-right">Actions</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {filteredInterviews.map((interview) => (
                                        <TableRow key={interview.id}>
                                            <TableCell>
                                                <div>
                                                    <p className="font-medium">{interview.candidateName || "Unknown"}</p>
                                                    <p className="text-sm text-gray-500">{interview.candidateEmail || "No email"}</p>
                                                </div>
                                            </TableCell>
                                            <TableCell>{interview.position || "N/A"}</TableCell>
                                            <TableCell>
                                                <div>
                                                    <p>{formatDate(interview.scheduledAt)}</p>
                                                    <p className="text-sm text-gray-500">
                                                        {formatTime(interview.scheduledAt)}
                                                    </p>
                                                </div>
                                            </TableCell>
                                            <TableCell>{interview.duration || 0} min</TableCell>
                                            <TableCell>
                                                <Badge className={statusColors[interview.status as InterviewStatus] || ""}>
                                                    {interview.status?.replace("_", " ") || "unknown"}
                                                </Badge>
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline">
                                                    {interview.interviewerIds?.length || 0} assigned
                                                </Badge>
                                            </TableCell>
                                            <TableCell className="text-right">
                                                <Button variant="ghost" size="sm" asChild>
                                                    <Link href={`/dashboard/interviews/${interview.id}`}>View</Link>
                                                </Button>
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
