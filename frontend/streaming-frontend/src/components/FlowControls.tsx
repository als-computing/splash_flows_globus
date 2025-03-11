import { UseMutationResult } from '@tanstack/react-query'

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
    <>
      <button 
        onClick={handleLaunchFlow}
        disabled={launchFlowMutation.isPending}
      >
        {launchFlowMutation.isPending ? 'Launching...' : 'Launch Streaming Flow'}
      </button>
      
      {flowId && (
        <>
          <p>Flow ID: {flowId}</p>
          <button
            onClick={handleCancelFlow}
            disabled={cancelFlowMutation.isPending || !!cancelFlowMutation.data?.message}
            style={{ marginTop: '10px', backgroundColor: cancelFlowMutation.data?.message ? '#555' : '#aa3333' }}
          >
            {cancelFlowMutation.isPending ? 'Cancelling...' : cancelFlowMutation.data?.message ? 'Cancelled' : 'Cancel Flow'}
          </button>
        </>
      )}
    </>
  )
}