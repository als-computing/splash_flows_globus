import { FlowRunInfo } from '../types/flowTypes'

type FlowListProps = {
  flowRunInfos: FlowRunInfo[]
  isFetchingFlows: boolean
  refetchFlowRuns: () => void
}

export function FlowList({ flowRunInfos, isFetchingFlows, refetchFlowRuns }: FlowListProps) {
  return (
    <>
      <button 
        onClick={() => refetchFlowRuns()}
        disabled={isFetchingFlows}
        style={{ backgroundColor: '#2a6495' }}
      >
        {isFetchingFlows ? 'Fetching...' : 'Refresh Running Flow Runs'}
      </button>
      
      {flowRunInfos.length > 0 && (
        <div style={{ marginTop: '10px', textAlign: 'left' }}>
          <h3>Flow Run IDs:</h3>
          <ul>
            {flowRunInfos.map((info, index) => (
              <li key={index}>
                {info.id} - State: {info.state || 'Unknown'}
              </li>
            ))}
          </ul>
        </div>
      )}
      
      {flowRunInfos.length === 0 && !isFetchingFlows && (
        <p>No flow runs found.</p>
      )}
    </>
  )
}