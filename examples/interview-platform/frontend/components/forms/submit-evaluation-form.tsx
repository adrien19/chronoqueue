"use client"

import { useState } from "react"
import { zodResolver } from "@hookform/resolvers/zod"
import { useForm } from "react-hook-form"
import * as z from "zod"
import { Loader2, Plus, X } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
    Form,
    FormControl,
    FormDescription,
    FormField,
    FormItem,
    FormLabel,
    FormMessage,
} from "@/components/ui/form"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select"
import { Slider } from "@/components/ui/slider"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

const formSchema = z.object({
    interviewId: z.string().min(1, "Interview ID is required"),
    evaluatorName: z.string().min(2, "Evaluator name must be at least 2 characters"),
    evaluatorEmail: z.string().email("Please enter a valid email address"),
    technicalScore: z.number().min(0).max(10),
    communicationScore: z.number().min(0).max(10),
    problemSolvingScore: z.number().min(0).max(10),
    cultureFitScore: z.number().min(0).max(10),
    strengths: z.string().optional(),
    weaknesses: z.string().optional(),
    recommendation: z.enum(["strong_yes", "yes", "maybe", "no", "strong_no"]),
    comments: z.string().optional(),
})

type FormValues = z.infer<typeof formSchema>

interface SubmitEvaluationFormProps {
    interviewId: string
    onSubmit: (data: FormValues) => Promise<void>
    onCancel?: () => void
}

export function SubmitEvaluationForm({ interviewId, onSubmit, onCancel }: SubmitEvaluationFormProps) {
    const [isSubmitting, setIsSubmitting] = useState(false)
    const [strengths, setStrengths] = useState<string[]>([])
    const [weaknesses, setWeaknesses] = useState<string[]>([])
    const [strengthInput, setStrengthInput] = useState("")
    const [weaknessInput, setWeaknessInput] = useState("")

    const form = useForm<FormValues>({
        resolver: zodResolver(formSchema),
        defaultValues: {
            interviewId,
            evaluatorName: "",
            evaluatorEmail: "",
            technicalScore: 5,
            communicationScore: 5,
            problemSolvingScore: 5,
            cultureFitScore: 5,
            strengths: "",
            weaknesses: "",
            recommendation: "maybe",
            comments: "",
        },
    })

    const handleSubmit = async (values: FormValues) => {
        setIsSubmitting(true)
        try {
            const formData = {
                ...values,
                strengths,
                weaknesses,
            }
            await onSubmit(formData as any)
        } finally {
            setIsSubmitting(false)
        }
    }

    const addStrength = () => {
        if (strengthInput.trim() && !strengths.includes(strengthInput.trim())) {
            const newStrengths = [...strengths, strengthInput.trim()]
            setStrengths(newStrengths)
            form.setValue("strengths", newStrengths.join(","))
            setStrengthInput("")
        }
    }

    const removeStrength = (strength: string) => {
        const newStrengths = strengths.filter((s) => s !== strength)
        setStrengths(newStrengths)
        form.setValue("strengths", newStrengths.join(","))
    }

    const addWeakness = () => {
        if (weaknessInput.trim() && !weaknesses.includes(weaknessInput.trim())) {
            const newWeaknesses = [...weaknesses, weaknessInput.trim()]
            setWeaknesses(newWeaknesses)
            form.setValue("weaknesses", newWeaknesses.join(","))
            setWeaknessInput("")
        }
    }

    const removeWeakness = (weakness: string) => {
        const newWeaknesses = weaknesses.filter((w) => w !== weakness)
        setWeaknesses(newWeaknesses)
        form.setValue("weaknesses", newWeaknesses.join(","))
    }

    return (
        <Form {...form}>
            <form onSubmit={form.handleSubmit(handleSubmit)} className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <FormField
                        control={form.control}
                        name="evaluatorName"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Your Name</FormLabel>
                                <FormControl>
                                    <Input placeholder="Jane Smith" {...field} />
                                </FormControl>
                                <FormMessage />
                            </FormItem>
                        )}
                    />

                    <FormField
                        control={form.control}
                        name="evaluatorEmail"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Your Email</FormLabel>
                                <FormControl>
                                    <Input type="email" placeholder="jane.smith@company.com" {...field} />
                                </FormControl>
                                <FormMessage />
                            </FormItem>
                        )}
                    />
                </div>

                <Card>
                    <CardHeader>
                        <CardTitle>Evaluation Scores</CardTitle>
                        <CardDescription>Rate the candidate on a scale of 0-10 for each category</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-6">
                        <FormField
                            control={form.control}
                            name="technicalScore"
                            render={({ field: { value, onChange } }) => (
                                <FormItem>
                                    <div className="flex justify-between">
                                        <FormLabel>Technical Skills</FormLabel>
                                        <span className="text-sm font-medium">{value}/10</span>
                                    </div>
                                    <FormControl>
                                        <Slider
                                            min={0}
                                            max={10}
                                            step={1}
                                            value={[value]}
                                            onValueChange={(vals) => onChange(vals[0])}
                                            className="w-full"
                                        />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name="communicationScore"
                            render={({ field: { value, onChange } }) => (
                                <FormItem>
                                    <div className="flex justify-between">
                                        <FormLabel>Communication</FormLabel>
                                        <span className="text-sm font-medium">{value}/10</span>
                                    </div>
                                    <FormControl>
                                        <Slider
                                            min={0}
                                            max={10}
                                            step={1}
                                            value={[value]}
                                            onValueChange={(vals) => onChange(vals[0])}
                                            className="w-full"
                                        />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name="problemSolvingScore"
                            render={({ field: { value, onChange } }) => (
                                <FormItem>
                                    <div className="flex justify-between">
                                        <FormLabel>Problem Solving</FormLabel>
                                        <span className="text-sm font-medium">{value}/10</span>
                                    </div>
                                    <FormControl>
                                        <Slider
                                            min={0}
                                            max={10}
                                            step={1}
                                            value={[value]}
                                            onValueChange={(vals) => onChange(vals[0])}
                                            className="w-full"
                                        />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />

                        <FormField
                            control={form.control}
                            name="cultureFitScore"
                            render={({ field: { value, onChange } }) => (
                                <FormItem>
                                    <div className="flex justify-between">
                                        <FormLabel>Culture Fit</FormLabel>
                                        <span className="text-sm font-medium">{value}/10</span>
                                    </div>
                                    <FormControl>
                                        <Slider
                                            min={0}
                                            max={10}
                                            step={1}
                                            value={[value]}
                                            onValueChange={(vals) => onChange(vals[0])}
                                            className="w-full"
                                        />
                                    </FormControl>
                                    <FormMessage />
                                </FormItem>
                            )}
                        />
                    </CardContent>
                </Card>

                <FormField
                    control={form.control}
                    name="strengths"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Key Strengths</FormLabel>
                            <div className="flex gap-2">
                                <Input
                                    placeholder="e.g., Strong problem-solving skills"
                                    value={strengthInput}
                                    onChange={(e) => setStrengthInput(e.target.value)}
                                    onKeyPress={(e) => {
                                        if (e.key === "Enter") {
                                            e.preventDefault()
                                            addStrength()
                                        }
                                    }}
                                />
                                <Button type="button" onClick={addStrength} variant="secondary" size="icon">
                                    <Plus className="h-4 w-4" />
                                </Button>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-2">
                                {strengths.map((strength) => (
                                    <Badge key={strength} variant="default" className="pl-2 pr-1">
                                        {strength}
                                        <button
                                            type="button"
                                            onClick={() => removeStrength(strength)}
                                            className="ml-1 hover:bg-primary/80 rounded-full p-0.5"
                                        >
                                            <X className="h-3 w-3" />
                                        </button>
                                    </Badge>
                                ))}
                            </div>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <FormField
                    control={form.control}
                    name="weaknesses"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Areas for Improvement</FormLabel>
                            <div className="flex gap-2">
                                <Input
                                    placeholder="e.g., Could improve time management"
                                    value={weaknessInput}
                                    onChange={(e) => setWeaknessInput(e.target.value)}
                                    onKeyPress={(e) => {
                                        if (e.key === "Enter") {
                                            e.preventDefault()
                                            addWeakness()
                                        }
                                    }}
                                />
                                <Button type="button" onClick={addWeakness} variant="secondary" size="icon">
                                    <Plus className="h-4 w-4" />
                                </Button>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-2">
                                {weaknesses.map((weakness) => (
                                    <Badge key={weakness} variant="destructive" className="pl-2 pr-1">
                                        {weakness}
                                        <button
                                            type="button"
                                            onClick={() => removeWeakness(weakness)}
                                            className="ml-1 hover:bg-destructive/80 rounded-full p-0.5"
                                        >
                                            <X className="h-3 w-3" />
                                        </button>
                                    </Badge>
                                ))}
                            </div>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <FormField
                    control={form.control}
                    name="recommendation"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Overall Recommendation</FormLabel>
                            <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                    <SelectTrigger>
                                        <SelectValue placeholder="Select your recommendation" />
                                    </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                    <SelectItem value="strong_yes">Strong Yes - Exceptional candidate</SelectItem>
                                    <SelectItem value="yes">Yes - Would hire</SelectItem>
                                    <SelectItem value="maybe">Maybe - On the fence</SelectItem>
                                    <SelectItem value="no">No - Would not hire</SelectItem>
                                    <SelectItem value="strong_no">Strong No - Not a fit</SelectItem>
                                </SelectContent>
                            </Select>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <FormField
                    control={form.control}
                    name="comments"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Additional Comments</FormLabel>
                            <FormControl>
                                <Textarea
                                    placeholder="Any additional feedback or observations about the candidate..."
                                    className="resize-none"
                                    rows={6}
                                    {...field}
                                />
                            </FormControl>
                            <FormDescription>
                                Include any specific examples or notable moments from the interview
                            </FormDescription>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <div className="flex justify-end gap-4">
                    {onCancel && (
                        <Button type="button" variant="outline" onClick={onCancel}>
                            Cancel
                        </Button>
                    )}
                    <Button type="submit" disabled={isSubmitting}>
                        {isSubmitting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Submit Evaluation
                    </Button>
                </div>
            </form>
        </Form>
    )
}
