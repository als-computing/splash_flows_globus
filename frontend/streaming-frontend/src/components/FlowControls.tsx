import { UseMutationResult } from '@tanstack/react-query'
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"

type FlowControlsProps = {
  flowId: string | null
  launchFlowMutation: UseMutationResult<any, Error, void>
  cancelFlowMutation: UseMutationResult<any, Error, string>
  handleLaunchFlow: () => void
  handleCancelFlow: () => void
}

export function FlowControls({
  flowId,
  launchFlowMutation,
  cancelFlowMutation,
  handleLaunchFlow,
  handleCancelFlow
}: FlowControlsProps) {
  return (
    <div className="space-y-4">
      <Button 
        onClick={handleLaunchFlow}
        disabled={launchFlowMutation.isPending}
        variant="default"
        className="w-full"
      >
        {launchFlowMutation.isPending ? 'Launching...' : 'Launch Streaming Flow'}
      </Button>
      
      {flowId && (
        <Card>
          <CardContent className="pt-4">
            <p className="text-sm text-muted-foreground mb-2">Flow ID: {flowId}</p>
            <Button
              onClick={handleCancelFlow}
              disabled={cancelFlowMutation.isPending || !!cancelFlowMutation.data?.message}
              variant={cancelFlowMutation.data?.message ? "outline" : "destructive"}
              className="w-full"
            >
              {cancelFlowMutation.isPending ? 'Cancelling...' : cancelFlowMutation.data?.message ? 'Cancelled' : 'Cancel Flow'}
            </Button>
          </CardContent>
        </Card>
      )}
    </div>
  )
}