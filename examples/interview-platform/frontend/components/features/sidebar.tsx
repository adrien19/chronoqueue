"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { UserButton } from "@clerk/nextjs";
import { cn } from "@/lib/utils";
import {
    Calendar,
    FileText,
    LayoutDashboard,
    MessageSquare,
    Clock,
    Settings,
    Bell,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";

const navigation = [
    {
        name: "Dashboard",
        href: "/dashboard",
        icon: LayoutDashboard,
    },
    {
        name: "Interviews",
        href: "/dashboard/interviews",
        icon: Calendar,
    },
    {
        name: "Evaluations",
        href: "/dashboard/evaluations",
        icon: MessageSquare,
        badge: "3", // This would come from React Query in real app
    },
    {
        name: "Reports",
        href: "/dashboard/reports",
        icon: FileText,
    },
    {
        name: "Queue Monitor",
        href: "/dashboard/queues",
        icon: Clock,
    },
];

export function Sidebar() {
    const pathname = usePathname();

    return (
        <div className="flex h-full w-64 flex-col border-r bg-gray-50 dark:bg-gray-900">
            {/* Logo */}
            <div className="flex h-16 items-center border-b px-6">
                <Link href="/dashboard" className="flex items-center space-x-2">
                    <Clock className="h-6 w-6 text-blue-600" />
                    <span className="text-lg font-bold">Interview Platform</span>
                </Link>
            </div>

            {/* Navigation */}
            <nav className="flex-1 space-y-1 px-3 py-4">
                {navigation.map((item) => {
                    const isActive = pathname === item.href || pathname?.startsWith(`${item.href}/`);
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={cn(
                                "flex items-center justify-between rounded-lg px-3 py-2 text-sm font-medium transition-colors",
                                isActive
                                    ? "bg-blue-100 text-blue-900 dark:bg-blue-900 dark:text-blue-100"
                                    : "text-gray-700 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-gray-800"
                            )}
                        >
                            <div className="flex items-center space-x-3">
                                <item.icon className="h-5 w-5" />
                                <span>{item.name}</span>
                            </div>
                            {item.badge && (
                                <Badge variant="secondary" className="ml-auto">
                                    {item.badge}
                                </Badge>
                            )}
                        </Link>
                    );
                })}
            </nav>

            {/* Bottom Section */}
            <div className="border-t p-4">
                <div className="flex items-center justify-between">
                    <div className="flex items-center space-x-3">
                        <UserButton afterSignOutUrl="/" />
                        <div className="flex flex-col">
                            <span className="text-sm font-medium">Your Account</span>
                            <span className="text-xs text-gray-500">Manage profile</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
