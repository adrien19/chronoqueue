import axios, { AxiosInstance } from "axios";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8080";

class ApiClient {
    private client: AxiosInstance;

    constructor() {
        this.client = axios.create({
            baseURL: API_URL,
            headers: {
                "Content-Type": "application/json",
            },
            timeout: 10000,
        });

        // Request interceptor to add auth token
        this.client.interceptors.request.use(
            (config) => {
                // Get Clerk session token from cookie or context
                // This will be populated by Clerk middleware
                return config;
            },
            (error) => Promise.reject(error)
        );

        // Response interceptor for error handling
        this.client.interceptors.response.use(
            (response) => response,
            (error) => {
                console.error("API Error:", error.response?.data || error.message);
                return Promise.reject(error);
            }
        );
    }

    public getClient(): AxiosInstance {
        return this.client;
    }
}

export const apiClient = new ApiClient().getClient();
