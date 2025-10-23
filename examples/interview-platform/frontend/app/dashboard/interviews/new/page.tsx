"use client"

import { useState } from "react"
import { useRouter } from "next/navigation"
import Link from "next/link"
import { ArrowLeft } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import { ScheduleInterviewForm } from "@/components/forms/schedule-interview-form"
import { useToast } from "@/hooks/use-toast"
import { createInterview, type CreateInterviewRequest } from "@/lib/api/api"

export default function NewInterviewPage() {
    const router = useRouter()
    const { toast } = useToast()
    const [isSubmitting, setIsSubmitting] = useState(false)

    const handleSubmit = async (data: any) => {
        setIsSubmitting(true)
        try {
            // Transform form data to API format
            const interviewData: CreateInterviewRequest = {
                candidateName: data.candidateName,
                candidateEmail: data.candidateEmail,
                position: data.position,
                scheduledAt: data.scheduledDate.toISOString(),
                duration: data.duration,
                interviewerIds: Array.isArray(data.interviewers)
                    ? data.interviewers
                    : data.interviewers.split(',').map((s: string) => s.trim()).filter(Boolean),
                tags: Array.isArray(data.tags)
                    ? data.tags
                    : data.tags ? data.tags.split(',').map((s: string) => s.trim()).filter(Boolean) : undefined,
            }

            // Call the API
            const interview = await createInterview(interviewData)

            toast({
                title: "Interview Scheduled",
                description: `Interview with ${interview.candidateName} has been scheduled successfully.`,
            })

            router.push("/dashboard/interviews")
        } catch (error) {
            console.error('Failed to schedule interview:', error)
            toast({
                title: "Error",
                description: error instanceof Error ? error.message : "Failed to schedule interview. Please try again.",
                variant: "destructive",
            })
        } finally {
            setIsSubmitting(false)
        }
    }

    return (
        <div className="container max-w-4xl py-8">
            <div className="mb-6">
                <Button variant="ghost" asChild>
                    <Link href="/dashboard/interviews">
                        <ArrowLeft className="mr-2 h-4 w-4" />
                        Back to Interviews
                    </Link>
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle className="text-2xl">Schedule New Interview</CardTitle>
                    <CardDescription>
                        Fill out the form below to schedule a new interview with a candidate.
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <ScheduleInterviewForm
                        onSubmit={handleSubmit}
                        onCancel={() => router.push("/dashboard/interviews")}
                    />
                </CardContent>
            </Card>
        </div>
    )
}
