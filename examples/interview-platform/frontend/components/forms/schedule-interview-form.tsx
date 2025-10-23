"use client"

import { useState } from "react"
import { zodResolver } from "@hookform/resolvers/zod"
import { useForm } from "react-hook-form"
import * as z from "zod"
import { CalendarIcon, Loader2, Clock } from "lucide-react"
import { format } from "date-fns"

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
import { Calendar } from "@/components/ui/calendar"
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "@/components/ui/popover"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"
import { X } from "lucide-react"

const formSchema = z.object({
    candidateName: z.string().min(2, {
        message: "Candidate name must be at least 2 characters.",
    }),
    candidateEmail: z.string().email({
        message: "Please enter a valid email address.",
    }),
    position: z.string().min(2, {
        message: "Position must be at least 2 characters.",
    }),
    scheduledDate: z.date({
        message: "Please select a date and time.",
    }),
    duration: z.number().int().min(15).max(480, {
        message: "Duration must be between 15 and 480 minutes.",
    }),
    interviewers: z.string().min(1, {
        message: "Please add at least one interviewer.",
    }),
    notes: z.string().optional(),
    tags: z.string().optional(),
})

type FormValues = z.infer<typeof formSchema>

interface ScheduleInterviewFormProps {
    onSubmit: (data: FormValues) => Promise<void>
    onCancel?: () => void
}

export function ScheduleInterviewForm({ onSubmit, onCancel }: ScheduleInterviewFormProps) {
    const [isSubmitting, setIsSubmitting] = useState(false)
    const [interviewers, setInterviewers] = useState<string[]>([])
    const [tags, setTags] = useState<string[]>([])
    const [interviewerInput, setInterviewerInput] = useState("")
    const [tagInput, setTagInput] = useState("")

    const form = useForm<FormValues>({
        resolver: zodResolver(formSchema),
        defaultValues: {
            candidateName: "",
            candidateEmail: "",
            position: "",
            duration: 60,
            interviewers: "",
            notes: "",
            tags: "",
        },
    })

    const handleSubmit = async (values: FormValues) => {
        setIsSubmitting(true)
        try {
            // Add interviewers and tags to the form data
            const formData = {
                ...values,
                interviewers,
                tags,
            }
            await onSubmit(formData as any)
        } finally {
            setIsSubmitting(false)
        }
    }

    const addInterviewer = () => {
        if (interviewerInput.trim() && !interviewers.includes(interviewerInput.trim())) {
            const newInterviewers = [...interviewers, interviewerInput.trim()]
            setInterviewers(newInterviewers)
            form.setValue("interviewers", newInterviewers.join(","))
            setInterviewerInput("")
        }
    }

    const removeInterviewer = (interviewer: string) => {
        const newInterviewers = interviewers.filter((i) => i !== interviewer)
        setInterviewers(newInterviewers)
        form.setValue("interviewers", newInterviewers.join(","))
    }

    const addTag = () => {
        if (tagInput.trim() && !tags.includes(tagInput.trim())) {
            const newTags = [...tags, tagInput.trim()]
            setTags(newTags)
            form.setValue("tags", newTags.join(","))
            setTagInput("")
        }
    }

    const removeTag = (tag: string) => {
        const newTags = tags.filter((t) => t !== tag)
        setTags(newTags)
        form.setValue("tags", newTags.join(","))
    }

    return (
        <Form {...form}>
            <form onSubmit={form.handleSubmit(handleSubmit)} className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <FormField
                        control={form.control}
                        name="candidateName"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Candidate Name</FormLabel>
                                <FormControl>
                                    <Input placeholder="John Doe" {...field} />
                                </FormControl>
                                <FormMessage />
                            </FormItem>
                        )}
                    />

                    <FormField
                        control={form.control}
                        name="candidateEmail"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Email Address</FormLabel>
                                <FormControl>
                                    <Input type="email" placeholder="john.doe@example.com" {...field} />
                                </FormControl>
                                <FormMessage />
                            </FormItem>
                        )}
                    />
                </div>

                <FormField
                    control={form.control}
                    name="position"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Position</FormLabel>
                            <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                    <SelectTrigger>
                                        <SelectValue placeholder="Select a position" />
                                    </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                    <SelectItem value="software-engineer">Software Engineer</SelectItem>
                                    <SelectItem value="senior-software-engineer">Senior Software Engineer</SelectItem>
                                    <SelectItem value="staff-engineer">Staff Engineer</SelectItem>
                                    <SelectItem value="engineering-manager">Engineering Manager</SelectItem>
                                    <SelectItem value="product-manager">Product Manager</SelectItem>
                                    <SelectItem value="designer">Designer</SelectItem>
                                    <SelectItem value="data-scientist">Data Scientist</SelectItem>
                                </SelectContent>
                            </Select>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <FormField
                        control={form.control}
                        name="scheduledDate"
                        render={({ field }) => (
                            <FormItem className="flex flex-col">
                                <FormLabel>Interview Date & Time</FormLabel>
                                <Popover>
                                    <PopoverTrigger asChild>
                                        <FormControl>
                                            <Button
                                                variant={"outline"}
                                                className={cn(
                                                    "w-full pl-3 text-left font-normal",
                                                    !field.value && "text-muted-foreground"
                                                )}
                                            >
                                                {field.value ? (
                                                    format(field.value, "PPP 'at' HH:mm:ss")
                                                ) : (
                                                    <span>Pick a date and time</span>
                                                )}
                                                <CalendarIcon className="ml-auto h-4 w-4 opacity-50" />
                                            </Button>
                                        </FormControl>
                                    </PopoverTrigger>
                                    <PopoverContent className="w-auto p-0" align="start">
                                        <div className="p-3 space-y-3">
                                            <Calendar
                                                mode="single"
                                                selected={field.value}
                                                onSelect={(date) => {
                                                    if (date) {
                                                        // Preserve existing time when changing date
                                                        const currentTime = field.value || new Date()
                                                        date.setHours(currentTime.getHours())
                                                        date.setMinutes(currentTime.getMinutes())
                                                        date.setSeconds(currentTime.getSeconds())
                                                    }
                                                    field.onChange(date)
                                                }}
                                                initialFocus
                                            />
                                            <div className="border-t pt-3">
                                                <div className="flex items-center gap-2 mb-2">
                                                    <Clock className="h-4 w-4 opacity-50" />
                                                    <span className="text-sm font-medium">Time</span>
                                                </div>
                                                <div className="flex gap-2">
                                                    <div className="flex-1">
                                                        <label className="text-xs text-muted-foreground">Hour</label>
                                                        <Input
                                                            type="number"
                                                            min="0"
                                                            max="23"
                                                            value={field.value?.getHours() ?? 0}
                                                            onChange={(e) => {
                                                                const date = field.value || new Date()
                                                                date.setHours(parseInt(e.target.value) || 0)
                                                                field.onChange(new Date(date))
                                                            }}
                                                            className="text-center"
                                                        />
                                                    </div>
                                                    <div className="flex-1">
                                                        <label className="text-xs text-muted-foreground">Min</label>
                                                        <Input
                                                            type="number"
                                                            min="0"
                                                            max="59"
                                                            value={field.value?.getMinutes() ?? 0}
                                                            onChange={(e) => {
                                                                const date = field.value || new Date()
                                                                date.setMinutes(parseInt(e.target.value) || 0)
                                                                field.onChange(new Date(date))
                                                            }}
                                                            className="text-center"
                                                        />
                                                    </div>
                                                    <div className="flex-1">
                                                        <label className="text-xs text-muted-foreground">Sec</label>
                                                        <Input
                                                            type="number"
                                                            min="0"
                                                            max="59"
                                                            value={field.value?.getSeconds() ?? 0}
                                                            onChange={(e) => {
                                                                const date = field.value || new Date()
                                                                date.setSeconds(parseInt(e.target.value) || 0)
                                                                field.onChange(new Date(date))
                                                            }}
                                                            className="text-center"
                                                        />
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </PopoverContent>
                                </Popover>
                                <FormDescription>
                                    Select date and specific time (including same-day scheduling)
                                </FormDescription>
                                <FormMessage />
                            </FormItem>
                        )}
                    />

                    <FormField
                        control={form.control}
                        name="duration"
                        render={({ field }) => (
                            <FormItem>
                                <FormLabel>Duration (minutes)</FormLabel>
                                <Select
                                    onValueChange={(value) => field.onChange(parseInt(value, 10))}
                                    defaultValue={field.value?.toString()}
                                >
                                    <FormControl>
                                        <SelectTrigger>
                                            <SelectValue placeholder="Select duration" />
                                        </SelectTrigger>
                                    </FormControl>
                                    <SelectContent>
                                        <SelectItem value="30">30 minutes</SelectItem>
                                        <SelectItem value="45">45 minutes</SelectItem>
                                        <SelectItem value="60">1 hour</SelectItem>
                                        <SelectItem value="90">1.5 hours</SelectItem>
                                        <SelectItem value="120">2 hours</SelectItem>
                                    </SelectContent>
                                </Select>
                                <FormMessage />
                            </FormItem>
                        )}
                    />
                </div>

                <FormField
                    control={form.control}
                    name="interviewers"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Interviewers</FormLabel>
                            <div className="flex gap-2">
                                <Input
                                    placeholder="Enter interviewer email"
                                    value={interviewerInput}
                                    onChange={(e) => setInterviewerInput(e.target.value)}
                                    onKeyPress={(e) => {
                                        if (e.key === "Enter") {
                                            e.preventDefault()
                                            addInterviewer()
                                        }
                                    }}
                                />
                                <Button type="button" onClick={addInterviewer} variant="secondary">
                                    Add
                                </Button>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-2">
                                {interviewers.map((interviewer) => (
                                    <Badge key={interviewer} variant="secondary" className="pl-2 pr-1">
                                        {interviewer}
                                        <button
                                            type="button"
                                            onClick={() => removeInterviewer(interviewer)}
                                            className="ml-1 hover:bg-muted rounded-full p-0.5"
                                        >
                                            <X className="h-3 w-3" />
                                        </button>
                                    </Badge>
                                ))}
                            </div>
                            <FormDescription>
                                Add email addresses of interviewers (press Enter or click Add)
                            </FormDescription>
                            <FormMessage />
                        </FormItem>
                    )}
                />

                <FormField
                    control={form.control}
                    name="tags"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Tags (Optional)</FormLabel>
                            <div className="flex gap-2">
                                <Input
                                    placeholder="e.g., technical, behavioral"
                                    value={tagInput}
                                    onChange={(e) => setTagInput(e.target.value)}
                                    onKeyPress={(e) => {
                                        if (e.key === "Enter") {
                                            e.preventDefault()
                                            addTag()
                                        }
                                    }}
                                />
                                <Button type="button" onClick={addTag} variant="secondary">
                                    Add
                                </Button>
                            </div>
                            <div className="flex flex-wrap gap-2 mt-2">
                                {tags.map((tag) => (
                                    <Badge key={tag} variant="outline" className="pl-2 pr-1">
                                        {tag}
                                        <button
                                            type="button"
                                            onClick={() => removeTag(tag)}
                                            className="ml-1 hover:bg-muted rounded-full p-0.5"
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
                    name="notes"
                    render={({ field }) => (
                        <FormItem>
                            <FormLabel>Additional Notes (Optional)</FormLabel>
                            <FormControl>
                                <Textarea
                                    placeholder="Any special requirements or notes about the interview..."
                                    className="resize-none"
                                    rows={4}
                                    {...field}
                                />
                            </FormControl>
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
                        Schedule Interview
                    </Button>
                </div>
            </form>
        </Form>
    )
}
