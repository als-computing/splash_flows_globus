import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import axios from "axios"
import { useState } from "react"
import { PrefectState } from "../types/flowTypes"
import { ErrorAlert } from "./ErrorAlert"
import { StatusBadge } from "./StatusBadge"

export function FlowList() {
  const [error, setError] = useState<string | null>(null)
  const { cancelFlowMutation, flowRunInfos, isFetchingFlows } = useFlowAPI()

  const handleCancelFlow = (flowId: string) => {
    if (!flowId) return
    setError(null)
    cancelFlowMutation.mutate(flowId, {
      onError: (err) => {
        if (axios.isAxiosError(err)) {
          const errorMessage =
            err.response?.data?.detail ||
            (typeof err.message === "string"
              ? err.message
              : "Failed to cancel session")
          setError(errorMessage)
        } else if (err instanceof Error) {
          setError(err.message)
        } else {
          setError("Unexpected error occurred while cancelling flow")
        }
        console.error("Error cancelling flow:", err)
      },
    })
  }

  return (
    <div className="space-y-4">
      <ErrorAlert error={error} />

      {flowRunInfos.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-xl">Running Streaming Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {flowRunInfos.map((info, index) => {
                const isRunning = info.state === PrefectState.RUNNING
                const isCancelling =
                  cancelFlowMutation?.isPending &&
                  cancelFlowMutation.variables === info.id
                const isCancelled =
                  cancelFlowMutation?.data?.message &&
                  cancelFlowMutation.variables === info.id

                return (
                  <li
                    key={index}
                    className="flex flex-col border-b border-border pb-2 last:border-0 last:pb-0"
                  >
                    <div className="flex items-center justify-between mb-1">
                      <div className="flex flex-col">
                        <span className="font-mono text-sm">{info.name}</span>
                      </div>
                      <div className="flex items-center gap-2">
                        {info.slurm_job_info?.job_id && (
                          <StatusBadge 
                            status={info.slurm_job_info.job_state} 
                            type="slurm"
                            jobId={info.slurm_job_info.job_id}
                          />
                        )}
                        <StatusBadge 
                          status={info.state} 
                          type="prefect"
                          flowId={info.id}
                        />
                      </div>
                    </div>
                    
                    {isRunning && info.slurm_job_info?.job_id && handleCancelFlow && (
                      <Button
                        onClick={() => handleCancelFlow(info.id)}
                        disabled={isCancelling || isCancelled}
                        variant="destructive"
                        size="sm"
                        className="w-full mt-1"
                      >
                        {isCancelling ? "Cancelling..." : "Cancel Session"}
                      </Button>
                    )}
                  </li>
                )
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