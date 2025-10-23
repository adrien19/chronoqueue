"use client";

import { Bell, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Badge } from "@/components/ui/badge";

export function Header() {
    return (
        <header className="sticky top-0 z-10 flex h-16 items-center justify-between border-b bg-white px-6 dark:bg-gray-900">
            {/* Search */}
            <div className="flex flex-1 items-center space-x-4">
                <div className="relative w-full max-w-md">
                    <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
                    <Input
                        type="search"
                        placeholder="Search interviews, candidates..."
                        className="pl-10"
                    />
                </div>
            </div>

            {/* Actions */}
            <div className="flex items-center space-x-4">
                {/* Notifications */}
                <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                        <Button variant="ghost" size="icon" className="relative">
                            <Bell className="h-5 w-5" />
                            <Badge
                                variant="destructive"
                                className="absolute -right-1 -top-1 h-5 w-5 rounded-full p-0 text-xs"
                            >
                                3
                            </Badge>
                        </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end" className="w-80">
                        <DropdownMenuLabel>Notifications</DropdownMenuLabel>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="flex flex-col items-start space-y-1 py-3">
                            <div className="flex w-full items-center justify-between">
                                <span className="font-medium">Interview Starting Soon</span>
                                <span className="text-xs text-gray-500">5 min ago</span>
                            </div>
                            <p className="text-sm text-gray-600">
                                Interview with John Doe for Senior Developer position starts in 30 minutes
                            </p>
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="flex flex-col items-start space-y-1 py-3">
                            <div className="flex w-full items-center justify-between">
                                <span className="font-medium">Evaluation Pending</span>
                                <span className="text-xs text-gray-500">1 hour ago</span>
                            </div>
                            <p className="text-sm text-gray-600">
                                Please submit your evaluation for Jane Smith's interview
                            </p>
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="flex flex-col items-start space-y-1 py-3">
                            <div className="flex w-full items-center justify-between">
                                <span className="font-medium">Report Ready</span>
                                <span className="text-xs text-gray-500">2 hours ago</span>
                            </div>
                            <p className="text-sm text-gray-600">
                                Consolidated report for Michael Johnson is ready for review
                            </p>
                        </DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem className="text-center text-sm text-blue-600">
                            View all notifications
                        </DropdownMenuItem>
                    </DropdownMenuContent>
                </DropdownMenu>
            </div>
        </header>
    );
}
