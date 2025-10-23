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
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Plus, Search, Filter } from "lucide-react";
import { format } from "date-fns";
import { useQuery } from "@tanstack/react-query";
import { listInterviews, type Interview } from "@/lib/api/api";
import { Skeleton } from "@/components/ui/skeleton";

const statusColors = {
    pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
    scheduled: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    in_progress: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200",
    completed: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    cancelled: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
};

export default function InterviewsPage() {
    const [statusFilter, setStatusFilter] = useState("all");
    const [searchQuery, setSearchQuery] = useState("");
    const [isMounted, setIsMounted] = useState(false);

    useEffect(() => {
        setIsMounted(true);
    }, []);

    // Fetch interviews using React Query
    const { data: interviews, isLoading } = useQuery({
        queryKey: ["interviews", statusFilter],
        queryFn: () => listInterviews({
            status: statusFilter === "all" ? undefined : statusFilter,
            page: 1,
            limit: 100
        }),
        refetchInterval: 30000, // Refresh every 30 seconds
    });

    // Filter interviews based on search
    const filteredInterviews = (interviews || []).filter((interview: Interview) => {
        const matchesSearch =
            interview.candidateName.toLowerCase().includes(searchQuery.toLowerCase()) ||
            interview.position.toLowerCase().includes(searchQuery.toLowerCase());
        return matchesSearch;
    });

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
                                placeholder="Search by candidate or position..."
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
                    {isLoading ? (
                        <div className="space-y-2">
                            <Skeleton className="h-12 w-full" />
                            <Skeleton className="h-12 w-full" />
                            <Skeleton className="h-12 w-full" />
                        </div>
                    ) : (
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
                                {filteredInterviews.length === 0 ? (
                                    <TableRow>
                                        <TableCell colSpan={7} className="text-center text-gray-500">
                                            No interviews found
                                        </TableCell>
                                    </TableRow>
                                ) : (
                                    filteredInterviews.map((interview: Interview) => (
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
                                                    <p>{isMounted ? format(new Date(interview.scheduledAt), "MMM dd, yyyy") : "Loading..."}</p>
                                                    <p className="text-sm text-gray-500">
                                                        {isMounted ? format(new Date(interview.scheduledAt), "HH:mm") : ""}
                                                    </p>
                                                </div>
                                            </TableCell>
                                            <TableCell>{interview.duration} min</TableCell>
                                            <TableCell>
                                                <Badge className={statusColors[interview.status as keyof typeof statusColors]}>
                                                    {interview.status}
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
                                    ))
                                )}
                            </TableBody>
                        </Table>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
