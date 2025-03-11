import { UseMutationResult } from '@tanstack/react-query'
import { Button } from "@/components/ui/button"

type LaunchFlowButtonProps = {
  flowId: string | null
  launchFlowMutation: UseMutationResult<any, Error, void>
  cancelFlowMutation: UseMutationResult<any, Error, string>
  handleLaunchFlow: () => void
  handleCancelFlow: () => void
}

export function LuanchFlowButton({
  launchFlowMutation,
  handleLaunchFlow,
}: LaunchFlowButtonProps) {
  return (
    <div className="space-y-4">
      <Button 
        onClick={handleLaunchFlow}
        disabled={launchFlowMutation.isPending}
        variant="default"
        className="w-full"
      >
        {launchFlowMutation.isPending ? 'Launching...' : 'Launch Streaming Session'}
      </Button>
    </div>
  )
}