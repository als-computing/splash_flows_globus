import { Card, CardContent } from "@/components/ui/card"
import { FlowList } from "./FlowList"
import { LaunchFlowButton } from "./LaunchFlowButton"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import { PrefectState } from "@/types/flowTypes"

export function FlowDashboard() {
  const { flowRunInfos } = useFlowAPI()
  
  // Check if there's any running job
  const anyRunningJob = flowRunInfos.some(info => 
    info.state === PrefectState.RUNNING && info.slurm_job_info?.job_id
  )

  return (
    <div className="container mx-auto py-8 max-w-2xl">
      <h1 className="text-3xl font-bold mb-6 text-center">
        Streaming Dashboard
      </h1>
      <Card>
        <CardContent className="space-y-6">
          <FlowList />
          <LaunchFlowButton isJobRunning={anyRunningJob} />
        </CardContent>
      </Card>
    </div>
  )
}