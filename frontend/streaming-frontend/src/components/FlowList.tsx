import { FlowRunInfo } from '../types/flowTypes'
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Loader2 } from "lucide-react"

type FlowListProps = {
  flowRunInfos: FlowRunInfo[]
  isFetchingFlows: boolean
  refetchFlowRuns: () => void
}

export function FlowList({ flowRunInfos, isFetchingFlows, refetchFlowRuns }: FlowListProps) {
  return (
    <div className="space-y-4">
      <Button 
        onClick={() => refetchFlowRuns()}
        disabled={isFetchingFlows}
        variant="secondary"
        className="w-full"
      >
        {isFetchingFlows ? (
          <>
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            Fetching...
          </>
        ) : 'Refresh Running Flow Runs'}
      </Button>
      
      {flowRunInfos.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-xl">Flow Run IDs</CardTitle>
          </CardHeader>
          <CardContent>
            <ul className="space-y-2">
              {flowRunInfos.map((info, index) => (
                <li key={index} className="flex items-center border-b border-border pb-2 last:border-0 last:pb-0">
                  <div className="flex-1">
                    <span className="font-mono text-sm">{info.id}</span>
                    <p className="text-sm text-muted-foreground">
                      State: <span className="font-medium">{info.state || 'Unknown'}</span>
                    </p>
                  </div>
                </li>
              ))}
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