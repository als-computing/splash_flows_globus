import { useState } from 'react'
import { useFlowAPI } from '../hooks/useFlowAPI'
import { FlowList } from './FlowList'
import { FlowControls } from './FlowControls'
import axios from 'axios'

export function FlowDashboard() {
  const [flowId, setFlowId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  
  const {
    flowRunInfos,
    isFetchingFlows,
    refetchFlowRuns,
    launchFlowMutation,
    cancelFlowMutation
  } = useFlowAPI()

  const handleLaunchFlow = () => {
    setError(null)
    launchFlowMutation.mutate(undefined, {
      onSuccess: (data) => {
        setFlowId(data.flow_run_id)
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
      }
    })
  }

  const handleCancelFlow = () => {
    if (!flowId) return
    setError(null)
    cancelFlowMutation.mutate(flowId, {
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
      }
    })
  }

  return (
    <>
      <h1>Streaming Flow Launcher</h1>
      <div className="card">
        <FlowControls 
          flowId={flowId}
          launchFlowMutation={launchFlowMutation}
          cancelFlowMutation={cancelFlowMutation}
          handleLaunchFlow={handleLaunchFlow}
          handleCancelFlow={handleCancelFlow}
        />
        
        <div style={{ marginTop: '20px', borderTop: '1px solid #444', paddingTop: '20px' }}>
          <FlowList 
            flowRunInfos={flowRunInfos}
            isFetchingFlows={isFetchingFlows}
            refetchFlowRuns={refetchFlowRuns}
          />
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