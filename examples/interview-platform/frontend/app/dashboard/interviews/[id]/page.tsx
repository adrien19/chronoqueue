"use client"

import { useState } from "react"
import { useParams, useRouter } from "next/navigation"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import Link from "next/link"
import { ArrowLeft, Calendar, Clock, Mail, MapPin, Tag, User, Users } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
} from "@/components/ui/dialog"
import { useToast } from "@/hooks/use-toast"
import {
    getInterview,
    cancelInterview,
    startInterview,
    completeInterview,
    type Interview
} from "@/lib/api/api"

export default function InterviewDetailPage() {
    const params = useParams()
    const router = useRouter()
    const { toast } = useToast()
    const queryClient = useQueryClient()
    const interviewId = params.id as string

    const [showCancelDialog, setShowCancelDialog] = useState(false)
    const [showStartDialog, setShowStartDialog] = useState(false)
    const [showCompleteDialog, setShowCompleteDialog] = useState(false)
    const [isActionLoading, setIsActionLoading] = useState(false)

    const { data: interview, isLoading, error } = useQuery<Interview>({
        queryKey: ["interview", interviewId],
        queryFn: () => getInterview(interviewId),
    })

    const handleCancel = async () => {
        if (!interview) return

        setIsActionLoading(true)
        try {
            await cancelInterview(interview.id)

            // Invalidate queries to refresh data
            await queryClient.invalidateQueries({ queryKey: ['interview', interviewId] })
            await queryClient.invalidateQueries({ queryKey: ['interviews'] })

            setShowCancelDialog(false)

            toast({
                title: "Interview Cancelled",
                description: "The interview has been successfully cancelled.",
            })
        } catch (error) {
            console.error('Failed to cancel interview:', error)
            toast({
                title: "Error",
                description: error instanceof Error ? error.message : "Failed to cancel interview",
                variant: "destructive"
            })
        } finally {
            setIsActionLoading(false)
        }
    }

    const handleStartInterview = async () => {
        if (!interview) return

        setIsActionLoading(true)
        try {
            await startInterview(interview.id)

            toast({
                title: "Interview Started",
                description: `Interview with ${interview.candidateName} is now in progress.`,
            })

            // Refresh the interview data
            await queryClient.invalidateQueries({ queryKey: ["interview", interviewId] })
            await queryClient.invalidateQueries({ queryKey: ["interviews"] })

            setShowStartDialog(false)
        } catch (error) {
            console.error("Failed to start interview:", error)
            toast({
                title: "Error",
                description: error instanceof Error ? error.message : "Failed to start interview",
                variant: "destructive",
            })
        } finally {
            setIsActionLoading(false)
        }
    }

    const handleCompleteInterview = async () => {
        if (!interview) return

        setIsActionLoading(true)
        try {
            await completeInterview(interview.id)

            toast({
                title: "Interview Completed",
                description: `Interview with ${interview.candidateName} has been marked as completed.`,
            })

            // Refresh the interview data
            await queryClient.invalidateQueries({ queryKey: ["interview", interviewId] })
            await queryClient.invalidateQueries({ queryKey: ["interviews"] })

            setShowCompleteDialog(false)
        } catch (error) {
            console.error("Failed to complete interview:", error)
            toast({
                title: "Error",
                description: error instanceof Error ? error.message : "Failed to complete interview",
                variant: "destructive",
            })
        } finally {
            setIsActionLoading(false)
        }
    }

    const handleUpdateInterview = () => {
        toast({
            title: "Feature Coming Soon",
            description: "Interview editing functionality will be available soon.",
        })
    }

    const handleAddEvaluation = () => {
        router.push(`/dashboard/interviews/${interviewId}/evaluate`)
    }

    if (isLoading) {
        return (
            <div className="container py-8">
                <Skeleton className="h-8 w-64 mb-6" />
                <div className="grid gap-6">
                    <Skeleton className="h-64" />
                    <Skeleton className="h-48" />
                </div>
            </div>
        )
    }

    if (!interview) {
        return (
            <div className="container py-8">
                <div className="text-center">
                    <h1 className="text-2xl font-bold">Interview not found</h1>
                    <p className="text-muted-foreground mt-2">
                        The interview you're looking for doesn't exist.
                    </p>
                    <Button asChild className="mt-4">
                        <Link href="/dashboard/interviews">
                            <ArrowLeft className="mr-2 h-4 w-4" />
                            Back to Interviews
                        </Link>
                    </Button>
                </div>
            </div>
        )
    }

    const statusColors = {
        pending: "bg-yellow-500",
        scheduled: "bg-blue-500",
        in_progress: "bg-purple-500",
        completed: "bg-green-500",
        cancelled: "bg-red-500",
    }

    return (
        <div className="container py-8">
            <div className="mb-6">
                <Button variant="ghost" asChild>
                    <Link href="/dashboard/interviews">
                        <ArrowLeft className="mr-2 h-4 w-4" />
                        Back to Interviews
                    </Link>
                </Button>
            </div>

            <div className="grid gap-6">
                {/* Header Card */}
                <Card>
                    <CardHeader>
                        <div className="flex items-start justify-between">
                            <div>
                                <CardTitle className="text-3xl">{interview.candidateName}</CardTitle>
                                <CardDescription className="text-lg mt-1">
                                    {interview.position}
                                </CardDescription>
                            </div>
                            <Badge
                                className={`${statusColors[interview.status as keyof typeof statusColors]} text-white`}
                            >
                                {interview.status.replace("_", " ").toUpperCase()}
                            </Badge>
                        </div>
                    </CardHeader>
                    <CardContent>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="flex items-center gap-2 text-sm">
                                <Mail className="h-4 w-4 text-muted-foreground" />
                                <span>{interview.candidateEmail}</span>
                            </div>
                            <div className="flex items-center gap-2 text-sm">
                                <Calendar className="h-4 w-4 text-muted-foreground" />
                                <span>{new Date(interview.scheduledAt).toLocaleDateString()}</span>
                            </div>
                            <div className="flex items-center gap-2 text-sm">
                                <Clock className="h-4 w-4 text-muted-foreground" />
                                <span>
                                    {new Date(interview.scheduledAt).toLocaleTimeString()} ({interview.duration} min)
                                </span>
                            </div>
                        </div>

                        {interview.tags && interview.tags.length > 0 && (
                            <>
                                <Separator className="my-4" />
                                <div className="flex items-center gap-2 flex-wrap">
                                    <Tag className="h-4 w-4 text-muted-foreground" />
                                    {interview.tags.map((tag: string) => (
                                        <Badge key={tag} variant="outline">
                                            {tag}
                                        </Badge>
                                    ))}
                                </div>
                            </>
                        )}
                    </CardContent>
                </Card>

                {/* Interviewers Card */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Users className="h-5 w-5" />
                            Interviewers
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-2">
                            {interview.interviewerIds.map((interviewer: string) => (
                                <div
                                    key={interviewer}
                                    className="flex items-center gap-2 p-2 rounded-lg border"
                                >
                                    <User className="h-4 w-4 text-muted-foreground" />
                                    <span className="text-sm">{interviewer}</span>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>

                {/* Actions */}
                <Card>
                    <CardHeader>
                        <CardTitle>Actions</CardTitle>
                        <CardDescription>
                            Manage interview status and add evaluations
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="flex flex-wrap gap-4">
                        {interview.status === "cancelled" && (
                            <div className="w-full p-4 border border-red-200 bg-red-50 dark:bg-red-900/10 rounded-lg">
                                <p className="text-sm text-red-800 dark:text-red-200">
                                    This interview has been cancelled. No further actions are available.
                                </p>
                            </div>
                        )}
                        {interview.status !== "cancelled" && interview.status !== "completed" && (
                            <Button onClick={handleUpdateInterview}>
                                Update Interview
                            </Button>
                        )}
                        {interview.status === "scheduled" && (
                            <Button
                                variant="default"
                                onClick={() => setShowStartDialog(true)}
                                disabled={new Date(interview.scheduledAt) > new Date()}
                            >
                                {new Date(interview.scheduledAt) > new Date()
                                    ? "Start Interview (Not Time Yet)"
                                    : "Start Interview"}
                            </Button>
                        )}
                        {(interview.status === "completed" || interview.status === "in_progress") && (
                            <Button variant="outline" onClick={handleAddEvaluation}>
                                Add Evaluation
                            </Button>
                        )}
                        {interview.status === "in_progress" && (
                            <Button
                                variant="default"
                                onClick={() => setShowCompleteDialog(true)}
                            >
                                End Interview
                            </Button>
                        )}
                        {interview.status === "scheduled" && (
                            <Button
                                variant="destructive"
                                onClick={() => setShowCancelDialog(true)}
                            >
                                Cancel Interview
                            </Button>
                        )}
                    </CardContent>
                </Card>

                {/* Metadata */}
                <Card>
                    <CardHeader>
                        <CardTitle>Metadata</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <dl className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                            <div>
                                <dt className="font-medium text-muted-foreground">Interview ID</dt>
                                <dd className="mt-1 font-mono">{interview.id}</dd>
                            </div>
                            <div>
                                <dt className="font-medium text-muted-foreground">Created At</dt>
                                <dd className="mt-1">
                                    {new Date(interview.createdAt).toLocaleString()}
                                </dd>
                            </div>
                            <div>
                                <dt className="font-medium text-muted-foreground">Last Updated</dt>
                                <dd className="mt-1">
                                    {new Date(interview.updatedAt).toLocaleString()}
                                </dd>
                            </div>
                        </dl>
                    </CardContent>
                </Card>
            </div>

            {/* Cancel Interview Dialog */}
            <Dialog open={showCancelDialog} onOpenChange={setShowCancelDialog}>
                <DialogContent>
                    <DialogHeader>
                        <DialogTitle>Cancel Interview</DialogTitle>
                        <DialogDescription>
                            Are you sure you want to cancel this interview with {interview?.candidateName}?
                            This action cannot be undone.
                        </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                        <Button
                            variant="outline"
                            onClick={() => setShowCancelDialog(false)}
                            disabled={isActionLoading}
                        >
                            No, Keep Interview
                        </Button>
                        <Button
                            variant="destructive"
                            onClick={handleCancel}
                            disabled={isActionLoading}
                        >
                            {isActionLoading ? "Cancelling..." : "Yes, Cancel Interview"}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>

            {/* Start Interview Dialog */}
            <Dialog open={showStartDialog} onOpenChange={setShowStartDialog}>
                <DialogContent>
                    <DialogHeader>
                        <DialogTitle>Start Interview</DialogTitle>
                        <DialogDescription>
                            Are you ready to start the interview with {interview?.candidateName}?
                            The interview status will be marked as "In Progress".
                        </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                        <Button
                            variant="outline"
                            onClick={() => setShowStartDialog(false)}
                            disabled={isActionLoading}
                        >
                            Not Yet
                        </Button>
                        <Button
                            onClick={handleStartInterview}
                            disabled={isActionLoading}
                        >
                            {isActionLoading ? "Starting..." : "Yes, Start Interview"}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>

            {/* Complete Interview Dialog */}
            <Dialog open={showCompleteDialog} onOpenChange={setShowCompleteDialog}>
                <DialogContent>
                    <DialogHeader>
                        <DialogTitle>End Interview</DialogTitle>
                        <DialogDescription>
                            Are you ready to end this interview with {interview?.candidateName}?
                            The interview will be marked as completed and you can then add evaluations.
                        </DialogDescription>
                    </DialogHeader>
                    <DialogFooter>
                        <Button
                            variant="outline"
                            onClick={() => setShowCompleteDialog(false)}
                            disabled={isActionLoading}
                        >
                            Not Yet
                        </Button>
                        <Button
                            onClick={handleCompleteInterview}
                            disabled={isActionLoading}
                        >
                            {isActionLoading ? "Ending..." : "Yes, End Interview"}
                        </Button>
                    </DialogFooter>
                </DialogContent>
            </Dialog>
        </div>
    )
}
