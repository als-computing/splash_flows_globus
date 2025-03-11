import { Badge } from "@/components/ui/badge"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { PrefectState, SlurmJobState } from "@/types/flowTypes"

type StatusBadgeProps = {
  status: PrefectState | SlurmJobState | string | null
  type: "prefect" | "slurm"
  flowId?: string
  jobId?: string
}

export function StatusBadge({ status, type, flowId, jobId }: StatusBadgeProps) {
  // Helper function to determine badge variant based on status
  const getBadgeVariant = () => {
    if (!status) return "outline"
    
    if (type === "prefect") {
      switch (status) {
        case PrefectState.RUNNING:
          return "success"
        case PrefectState.COMPLETED:
          return "success"
        case PrefectState.FAILED:
        case PrefectState.CRASHED:
          return "destructive"
        case PrefectState.CANCELLED:
        case PrefectState.CANCELLING:
          return "warning"
        case PrefectState.PENDING:
        case PrefectState.SCHEDULED:
          return "secondary"
        default:
          return "outline"
      }
    } else {
      // Slurm states
      if (status === SlurmJobState.RUNNING) return "success"
      if (status === SlurmJobState.COMPLETED) return "success"
      if (
        [
          SlurmJobState.FAILED,
          SlurmJobState.BOOT_FAIL,
          SlurmJobState.NODE_FAIL,
          SlurmJobState.OUT_OF_MEMORY,
          SlurmJobState.TIMEOUT,
        ].includes(status as SlurmJobState)
      ) {
        return "destructive"
      }
      if (
        [
          SlurmJobState.CANCELLED,
          SlurmJobState.STOPPED,
          SlurmJobState.SUSPENDED,
        ].includes(status as SlurmJobState)
      ) {
        return "warning"
      }
      if (status === SlurmJobState.PENDING) return "secondary"
      return "outline"
    }
  }

  // Get tooltip content based on state type
  const getTooltipContent = () => {
    if (type === "prefect") {
      return (
        <div className="space-y-1">
          <p>Flow ID: {flowId || "Unknown"}</p>
          <p>Status: {status || "Unknown"}</p>
        </div>
      )
    }
    return (
      <div className="space-y-1">
        <p>Job ID: {jobId || "Unknown"}</p>
        <p>Status: {status || "Unknown"}</p>
      </div>
    )
  }

  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Badge variant={getBadgeVariant()} className="text-xs whitespace-nowrap uppercase">
            {type}
          </Badge>
        </TooltipTrigger>
        <TooltipContent>
          {getTooltipContent()}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}