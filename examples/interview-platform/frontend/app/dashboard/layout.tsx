import { Sidebar } from "@/components/features/sidebar";
import { Header } from "@/components/features/header";
import { SSEProvider } from "@/components/SSEProvider";
import { Toaster } from "@/components/ui/sonner";

export default function DashboardLayout({
    children,
}: {
    children: React.ReactNode;
}) {
    return (
        <SSEProvider>
            <div className="flex h-screen overflow-hidden">
                {/* Sidebar */}
                <Sidebar />

                {/* Main Content */}
                <div className="flex flex-1 flex-col overflow-hidden">
                    {/* Header */}
                    <Header />

                    {/* Page Content */}
                    <main className="flex-1 overflow-y-auto bg-gray-50 p-6 dark:bg-gray-800">
                        {children}
                    </main>
                </div>
            </div>
            <Toaster />
        </SSEProvider>
    );
}
