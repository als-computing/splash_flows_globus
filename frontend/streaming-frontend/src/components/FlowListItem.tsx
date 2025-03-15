import { PrefectState, SlurmJobState, FlowRunInfo } from "../types/flowTypes"
import { SlurmJobTime } from "./SlurmJobTime"
import { StatusBadge } from "./StatusBadge"
import { CancelFlowDialog } from "./CancelFlowDialog"

type FlowListItemProps = {
  info: FlowRunInfo
}

export function FlowListItem({ info }: FlowListItemProps) {
  const isPrefectRunning = info.state === PrefectState.RUNNING || info.state === PrefectState.SCHEDULED || info.state === PrefectState.PENDING
  const isPrefectShuttingDown = info.state === PrefectState.CANCELLING
  const isSlurmRunning: boolean = Boolean(
    info.slurm_job_info?.job_id &&
      info.slurm_job_info.job_state === SlurmJobState.RUNNING
  )
  
  // Determine the appropriate title message
  let title = "Getting things set up. Please wait."
  
  if (isPrefectShuttingDown) {
    title = "Shutting things down. You can launch another session."
  } else if (isSlurmRunning) {
    title = "Streaming is ready. You can collect data."
  }

  return (
    <li className="flex flex-col border-b border-border pb-2 last:border-0 last:pb-0">
      {(isPrefectRunning || isPrefectShuttingDown) && info.slurm_job_info && (
        <SlurmJobTime
          elapsed={info.slurm_job_info.elapsed}
          timelimit={info.slurm_job_info.timelimit}
          display={isSlurmRunning}
        />
      )}

      <div className="flex items-center justify-between mb-1">
        <div className="flex flex-col">
          {(isPrefectRunning || isPrefectShuttingDown) && (
            <span className="font-mono text-sm">{title}</span>
          )}
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

      {isPrefectRunning && info.slurm_job_info?.job_id && (
        <CancelFlowDialog flowId={info.id} />
      )}
    </li>
  )
}