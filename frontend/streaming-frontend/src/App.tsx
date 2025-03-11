import { useState } from 'react'
import './App.css'
import axios from 'axios'


enum StateType { SCHEDULED, PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, CRASH }
type FlowRunInfo = {
  id: string
  state: StateType | null
}

function App() {
  const [flowId, setFlowId] = useState<string | null>(null)
  const [isLaunching, setIsLaunching] = useState(false)
  const [isCancelling, setIsCancelling] = useState(false)
  const [isFetchingFlows, setIsFetchingFlows] = useState(false)
  const [flowRunInfos, setFlowRunInfos] = useState<FlowRunInfo[]>([])
  const [error, setError] = useState<string | null>(null)
  const [cancelMessage, setCancelMessage] = useState<string | null>(null)

  const handleLaunchFlow = async () => {
    setIsLaunching(true)
    setError(null)
    setCancelMessage(null)
    
    try {
      const response = await axios.post('http://localhost:8000/flows/launch', {
        walltime_minutes: 5
      })
      setFlowId(response.data.flow_run_id)
    } catch (err) {
      // Improved error handling
      if (axios.isAxiosError(err)) {
        const errorMessage = err.response?.data?.detail || 
                            (typeof err.message === 'string' ? err.message : 'Failed to launch flow');
        setError(errorMessage);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('Unexpected error occurred');
      }
      console.error('Error launching flow:', err)
    } finally {
      setIsLaunching(false)
    }
  }

  const handleCancelFlow = async () => {
    if (!flowId) return;
    
    setIsCancelling(true)
    setError(null)
    setCancelMessage(null)
    
    try {
      const response = await axios.delete(`http://localhost:8000/flows/${flowId}`)
      setCancelMessage(response.data.message)
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const errorMessage = err.response?.data?.detail || 
                            (typeof err.message === 'string' ? err.message : 'Failed to cancel flow');
        setError(errorMessage);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('Unexpected error occurred while cancelling flow');
      }
      console.error('Error cancelling flow:', err)
    } finally {
      setIsCancelling(false)
    }
  }

  const handleFetchFlowRuns = async () => {
    setIsFetchingFlows(true)
    setError(null)
    
    try {
      const response = await axios.get('http://localhost:8000/flows/running')
      setFlowRunInfos(response.data.flow_run_infos)
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const errorMessage = err.response?.data?.detail || 
                            (typeof err.message === 'string' ? err.message : 'Failed to fetch flow runs');
        setError(errorMessage);
      } else if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('Unexpected error occurred while fetching flow runs');
      }
      console.error('Error fetching flow runs:', err)
    } finally {
      setIsFetchingFlows(false)
    }
  }

  return (
    <>
      <h1>Streaming Flow Launcher</h1>
      <div className="card">
        <button 
          onClick={handleLaunchFlow}
          disabled={isLaunching}
        >
          {isLaunching ? 'Launching...' : 'Launch Streaming Flow'}
        </button>
        
        {flowId && (
          <>
            <p>Flow ID: {flowId}</p>
            <button
              onClick={handleCancelFlow}
              disabled={isCancelling || cancelMessage !== null}
              style={{ marginTop: '10px', backgroundColor: cancelMessage ? '#555' : '#aa3333' }}
            >
              {isCancelling ? 'Cancelling...' : cancelMessage ? 'Cancelled' : 'Cancel Flow'}
            </button>
          </>
        )}
        
        <div style={{ marginTop: '20px', borderTop: '1px solid #444', paddingTop: '20px' }}>
          <button 
            onClick={handleFetchFlowRuns}
            disabled={isFetchingFlows}
            style={{ backgroundColor: '#2a6495' }}
          >
            {isFetchingFlows ? 'Fetching...' : 'Fetch Running Flow Runs'}
          </button>
          
          {flowRunInfos.length > 0 && (
            <div style={{ marginTop: '10px', textAlign: 'left' }}>
              <h3>Flow Run IDs:</h3>
              <ul>
                {flowRunInfos.map((info, index) => (
                  <li key={index}>
                    {info.id} - State: {info.state ? info.state : 'Unknown'}
                  </li>
                ))}
              </ul>
            </div>
          )}
          
          {flowRunInfos.length === 0 && !isFetchingFlows && flowRunInfos.length !== undefined && (
            <p>No flow runs found.</p>
          )}
        </div>
        
        {error && (
          <p style={{ color: 'red' }}>{error}</p>
        )}
        
        {cancelMessage && (
          <p style={{ color: 'green' }}>{cancelMessage}</p>
        )}
      </div>
    </>
  )
}

export default App