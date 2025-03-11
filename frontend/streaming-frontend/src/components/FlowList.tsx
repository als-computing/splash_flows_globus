import { FlowRunInfo, StateType } from '../types/flowTypes'
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { UseMutationResult } from '@tanstack/react-query'

type FlowListProps = {
  flowRunInfos: FlowRunInfo[]
  isFetchingFlows: boolean
  refetchFlowRuns: () => void
  cancelFlowMutation?: UseMutationResult<any, Error, string>
  handleCancelFlow?: (flowId: string) => void
}

export function FlowList({ 
  flowRunInfos, 
  isFetchingFlows, 
  cancelFlowMutation,
  handleCancelFlow
}: FlowListProps) {
  return (
    <div className="space-y-4">
      
      {flowRunInfos.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-xl">Streaming Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {flowRunInfos.map((info, index) => {
                const isRunning = info.state === StateType.RUNNING;
                const isCancelling = cancelFlowMutation?.isPending && cancelFlowMutation.variables === info.id;
                const isCancelled = cancelFlowMutation?.data?.message && cancelFlowMutation.variables === info.id;
                
                return (
                  <li key={index} className="flex flex-col border-b border-border pb-2 last:border-0 last:pb-0">
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex flex-col">
                        <span className="font-mono text-sm">{info.id}</span>
                        {info.job_id && (
                          <span className="font-mono text-xs text-muted-foreground">
                            Slurm Job ID: {info.job_id}
                          </span>
                        )}
                      </div>
                      <span className={`text-sm font-medium ${isRunning ? 'text-green-600' : 'text-muted-foreground'}`}>
                        {info.state || 'Unknown'}
                      </span>
                    </div>
                    {isRunning && info.job_id && handleCancelFlow && (
                      <Button
                        onClick={() => handleCancelFlow(info.id)}
                        disabled={isCancelling || isCancelled}
                        variant="destructive"
                        size="sm"
                        className="w-full mt-1"
                      >
                        {isCancelling ? 'Cancelling...' : 'Cancel Session'}
                      </Button>
                    )}
                  </li>
                );
              })}
            </ul>
          </CardContent>
        </Card>
      )}
      
      {flowRunInfos.length === 0 && !isFetchingFlows && (
        <p className="text-center text-muted-foreground">No flow runs found.</p>
      )}
    </div>
  )
}