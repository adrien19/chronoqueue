import { AlertCircle } from "lucide-react";
import { Button } from "./button";

interface ErrorStateProps {
    title?: string;
    message: string;
    retry?: () => void;
}

export function ErrorState({
    title = "Something went wrong",
    message,
    retry
}: ErrorStateProps) {
    return (
        <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
            <div className="mb-4 rounded-full bg-red-100 p-3">
                <AlertCircle className="h-6 w-6 text-red-600" />
            </div>
            <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
            <p className="mt-2 text-sm text-gray-500 max-w-sm">{message}</p>
            {retry && (
                <Button onClick={retry} variant="outline" className="mt-6">
                    Try Again
                </Button>
            )}
        </div>
    );
}
