import { LucideIcon } from "lucide-react";
import { Button } from "./button";

interface EmptyStateProps {
    icon?: LucideIcon;
    title: string;
    description?: string;
    action?: {
        label: string;
        onClick: () => void;
    };
}

export function EmptyState({ icon: Icon, title, description, action }: EmptyStateProps) {
    return (
        <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
            {Icon && (
                <div className="mb-4 rounded-full bg-gray-100 p-3">
                    <Icon className="h-6 w-6 text-gray-600" />
                </div>
            )}
            <h3 className="text-lg font-semibold text-gray-900">{title}</h3>
            {description && (
                <p className="mt-2 text-sm text-gray-500 max-w-sm">{description}</p>
            )}
            {action && (
                <Button onClick={action.onClick} className="mt-6">
                    {action.label}
                </Button>
            )}
        </div>
    );
}
