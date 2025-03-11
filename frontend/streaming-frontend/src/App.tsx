import { useState } from 'react'
import './App.css'
import axios from 'axios'
import { QueryClient, QueryClientProvider, useMutation, useQuery } from '@tanstack/react-query'

// Create a client
const queryClient = new QueryClient()

// Convert to string enum
enum StateType {
  SCHEDULED = "SCHEDULED",
  PENDING = "PENDING",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  CANCELLED = "CANCELLED",
  CRASH = "CRASH"
}

type FlowRunInfo = {
  id: string
  state: StateType | null
}

// Wrap the app with QueryClientProvider
function AppWithProvider() {
  return (
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  )
}

function App() {
  const [flowId, setFlowId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Fetch running flows with useQuery
  const {
    data: flowRunInfos = [],
    isLoading: isFetchingFlows,
    refetch: refetchFlowRuns,
  } = useQuery({
    queryKey: ['flowRuns'],
    queryFn: async () => {
      const response = await axios.get('http://localhost:8000/flows/running')
      return response.data.flow_run_infos as FlowRunInfo[]
    },
    refetchInterval: 5000, // Auto-refresh every 5 seconds
  })

  // Launch flow mutation
  const launchFlowMutation = useMutation({
    mutationFn: async () => {
      const response = await axios.post('http://localhost:8000/flows/launch', {
        walltime_minutes: 5
      })
      return response.data
    },
    onSuccess: (data) => {
      setFlowId(data.flow_run_id)
      // Refetch flow runs after launching a new one
      queryClient.invalidateQueries({ queryKey: ['flowRuns'] })
    },
    onError: (err) => {
      if (axios.isAxiosError(err)) {
        const errorMessage = err.response?.data?.detail || 
                          (typeof err.message === 'string' ? err.message : 'Failed to launch flow')
        setError(errorMessage)
      } else if (err instanceof Error) {
        setError(err.message)
      } else {
        setError('Unexpected error occurred')
      }
      console.error('Error launching flow:', err)
    },
  })

  // Cancel flow mutation
  const cancelFlowMutation = useMutation({
    mutationFn: async (flowId: string) => {
      const response = await axios.delete(`http://localhost:8000/flows/${flowId}`)
      return response.data
    },
    onSuccess: () => {
      // Refetch flow runs after cancelling
      queryClient.invalidateQueries({ queryKey: ['flowRuns'] })
    },
    onError: (err) => {
      if (axios.isAxiosError(err)) {
        const errorMessage = err.response?.data?.detail || 
                          (typeof err.message === 'string' ? err.message : 'Failed to cancel flow')
        setError(errorMessage)
      } else if (err instanceof Error) {
        setError(err.message)
      } else {
        setError('Unexpected error occurred while cancelling flow')
      }
      console.error('Error cancelling flow:', err)
    },
  })

  const handleLaunchFlow = () => {
    setError(null)
    launchFlowMutation.mutate()
  }

  const handleCancelFlow = () => {
    if (!flowId) return
    setError(null)
    cancelFlowMutation.mutate(flowId)
  }

  return (
    <>
      <h1>Streaming Flow Launcher</h1>
      <div className="card">
        <button 
          onClick={handleLaunchFlow}
          disabled={launchFlowMutation.isPending}
        >
          {launchFlowMutation.isPending ? 'Launching...' : 'Launch Streaming Flow'}
        </button>
        
        {flowId && (
          <>
            <p>Flow ID: {flowId}</p>
            <button
              onClick={handleCancelFlow}
              disabled={cancelFlowMutation.isPending || !!cancelFlowMutation.data?.message}
              style={{ marginTop: '10px', backgroundColor: cancelFlowMutation.data?.message ? '#555' : '#aa3333' }}
            >
              {cancelFlowMutation.isPending ? 'Cancelling...' : cancelFlowMutation.data?.message ? 'Cancelled' : 'Cancel Flow'}
            </button>
          </>
        )}
        
        <div style={{ marginTop: '20px', borderTop: '1px solid #444', paddingTop: '20px' }}>
          <button 
            onClick={() => refetchFlowRuns()}
            disabled={isFetchingFlows}
            style={{ backgroundColor: '#2a6495' }}
          >
            {isFetchingFlows ? 'Fetching...' : 'Refresh Running Flow Runs'}
          </button>
          
          {flowRunInfos.length > 0 && (
            <div style={{ marginTop: '10px', textAlign: 'left' }}>
              <h3>Flow Run IDs:</h3>
              <ul>
                {flowRunInfos.map((info, index) => (
                  <li key={index}>
                    {info.id} - State: {info.state || 'Unknown'}
                  </li>
                ))}
              </ul>
            </div>
          )}
          
          {flowRunInfos.length === 0 && !isFetchingFlows && (
            <p>No flow runs found.</p>
          )}
        </div>
        
        {error && (
          <p style={{ color: 'red' }}>{error}</p>
        )}
        
        {cancelFlowMutation.data?.message && (
          <p style={{ color: 'green' }}>{cancelFlowMutation.data.message}</p>
        )}
      </div>
    </>
  )
}

export default AppWithProvider