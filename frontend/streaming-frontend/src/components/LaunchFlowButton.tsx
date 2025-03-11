import { Button } from "@/components/ui/button"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import axios from "axios"
import { useState } from "react"
import { ErrorAlert } from "./ErrorAlert"

export function LaunchFlowButton() {
  const [error, setError] = useState<string | null>(null)
  const { launchFlowMutation } = useFlowAPI()

  const handleLaunchFlow = () => {
    setError(null)
    launchFlowMutation.mutate(undefined, {
      onError: (err) => {
        if (axios.isAxiosError(err)) {
          const errorMessage =
            err.response?.data?.detail ||
            (typeof err.message === "string"
              ? err.message
              : "Failed to launch flow")
          setError(errorMessage)
        } else if (err instanceof Error) {
          setError(err.message)
        } else {
          setError("Unexpected error occurred")
        }
        console.error("Error launching flow:", err)
      },
    })
  }

  return (
    <div className="space-y-4">
      <Button
        onClick={handleLaunchFlow}
        disabled={launchFlowMutation.isPending}
        variant="default"
        className="w-full"
      >
        {launchFlowMutation.isPending
          ? "Launching..."
          : "Launch Streaming Session"}
      </Button>
      <ErrorAlert error={error} />
    </div>
  )
}
