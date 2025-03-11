import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import axios from "axios"
import { useState } from "react"
import { StateType } from "../types/flowTypes"
import { ErrorAlert } from "./ErrorAlert"

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
            <CardTitle className="text-xl">Streaming Sessions</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {flowRunInfos.map((info, index) => {
                const isRunning = info.state === StateType.RUNNING
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
                        <span className="font-mono text-sm">{info.id}</span>
                        {info.slurm_job_info?.job_id && (
                          <>
                            <span className="font-mono text-xs text-muted-foreground">
                              Slurm Job ID: {info.slurm_job_info.job_id}
                            </span>
                            <span className="font-mono text-xs text-muted-foreground">
                              Slurm Job Status: {info.slurm_job_info.job_state}
                            </span>
                          </>
                        )}
                      </div>
                      <span
                        className={`text-sm font-medium ${isRunning ? "text-green-600" : "text-muted-foreground"}`}
                      >
                        {info.state || "Unknown"}
                      </span>
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
