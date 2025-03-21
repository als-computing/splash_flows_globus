import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { FlowDashboard } from "./components/FlowDashboard"
import "./App.css"
import { ThemeProvider } from "./components/ThemeProvider"

const queryClient = new QueryClient()

function AppWithProvider() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <QueryClientProvider client={queryClient}>
        <FlowDashboard />
      </QueryClientProvider>
    </ThemeProvider>
  )
}

export default AppWithProvider
