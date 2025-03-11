import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { FlowDashboard } from './components/FlowDashboard'
import './App.css'

const queryClient = new QueryClient()

function AppWithProvider() {
  return (
    <QueryClientProvider client={queryClient}>
      <FlowDashboard />
    </QueryClientProvider>
  )
}

export default AppWithProvider