import { useState } from 'react'
import './App.css'
import axios from 'axios'

function App() {
  const [flowId, setFlowId] = useState<string | null>(null)
  const [isLaunching, setIsLaunching] = useState(false)
  const [isCancelling, setIsCancelling] = useState(false)
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