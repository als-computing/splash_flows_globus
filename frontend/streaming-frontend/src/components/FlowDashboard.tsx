import { useState } from 'react'
import { useFlowAPI } from '../hooks/useFlowAPI'
import { FlowList } from './FlowList'
import { LuanchFlowButton } from './LaunchFlowButton'
import axios from 'axios'
import { Card, CardContent } from "@/components/ui/card"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { ExclamationTriangleIcon } from "@radix-ui/react-icons"

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
                          (typeof err.message === 'string' ? err.message : 'Failed to cancel session')
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
    <div className="container mx-auto py-8 max-w-2xl">
      <h1 className="text-3xl font-bold mb-6 text-center">Streaming Sessions</h1>
      <Card>
        <CardContent className="space-y-6">
          <LuanchFlowButton 
            flowId={flowId}
            launchFlowMutation={launchFlowMutation}
            cancelFlowMutation={cancelFlowMutation}
            handleLaunchFlow={handleLaunchFlow}
            handleCancelFlow={handleCancelFlow}
          />
          
          <div className="pt-6 border-t">
            <FlowList 
              flowRunInfos={flowRunInfos}
              isFetchingFlows={isFetchingFlows}
              refetchFlowRuns={refetchFlowRuns}
            />
          </div>
          
          {error && (
            <Alert variant="destructive">
              <ExclamationTriangleIcon className="h-4 w-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}
          
        </CardContent>
      </Card>
    </div>
  )
}