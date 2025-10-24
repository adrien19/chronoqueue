"use client"

import { useState } from "react"
import { useParams, useRouter } from "next/navigation"
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
import { SubmitEvaluationForm } from "@/components/forms/submit-evaluation-form"
import { useToast } from "@/hooks/use-toast"
import { createEvaluation, type CreateEvaluationRequest } from "@/lib/api/api"

export default function NewEvaluationPage() {
    const params = useParams()
    const router = useRouter()
    const { toast } = useToast()
    const interviewId = params.id as string
    const [isSubmitting, setIsSubmitting] = useState(false)

    const handleSubmit = async (data: any) => {
        setIsSubmitting(true)
        try {
            // Transform form data to API format
            const evaluationData: CreateEvaluationRequest = {
                interview_id: interviewId,
                evaluator_name: data.evaluatorName,
                evaluator_email: data.evaluatorEmail,
                technical_score: data.technicalScore,
                communication_score: data.communicationScore,
                problem_solving_score: data.problemSolvingScore,
                cultural_fit_score: data.culturalFitScore,
                strengths: Array.isArray(data.strengths)
                    ? data.strengths
                    : (data.strengths || '').split(',').map((s: string) => s.trim()).filter(Boolean),
                weaknesses: Array.isArray(data.weaknesses)
                    ? data.weaknesses
                    : (data.weaknesses || '').split(',').map((s: string) => s.trim()).filter(Boolean),
                recommendation: data.recommendation,
                comments: data.comments || undefined,
            }

            // Call the API
            const evaluation = await createEvaluation(evaluationData)

            toast({
                title: "Evaluation Submitted",
                description: `Your evaluation has been submitted successfully with an overall score of ${evaluation.overall_score.toFixed(1)}.`,
            })

            router.push(`/dashboard/interviews/${interviewId}`)
        } catch (error) {
            console.error('Failed to submit evaluation:', error)
            toast({
                title: "Error",
                description: error instanceof Error ? error.message : "Failed to submit evaluation. Please try again.",
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
                    <Link href={`/dashboard/interviews/${interviewId}`}>
                        <ArrowLeft className="mr-2 h-4 w-4" />
                        Back to Interview
                    </Link>
                </Button>
            </div>

            <Card>
                <CardHeader>
                    <CardTitle className="text-2xl">Submit Evaluation</CardTitle>
                    <CardDescription>
                        Provide your feedback and scores for this interview.
                    </CardDescription>
                </CardHeader>
                <CardContent>
                    <SubmitEvaluationForm
                        interviewId={interviewId}
                        onSubmit={handleSubmit}
                        onCancel={() => router.push(`/dashboard/interviews/${interviewId}`)}
                    />
                </CardContent>
            </Card>
        </div>
    )
}
