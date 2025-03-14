import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import axios from "axios"
import { useState } from "react"
import { ErrorAlert } from "./ErrorAlert"

interface LaunchFlowButtonProps {
  isJobRunning?: boolean
}

export function LaunchFlowButton({
  isJobRunning = false,
}: LaunchFlowButtonProps) {
  const [error, setError] = useState<string | null>(null)
  const [walltime, setWalltime] = useState<number>(60) // Default 60 minutes
  const [isDialogOpen, setIsDialogOpen] = useState(false)
  const { launchFlowMutation } = useFlowAPI()

  const handleLaunchFlow = () => {
    setError(null)
    setIsDialogOpen(false)

    // Pass the walltime to the mutation
    launchFlowMutation.mutate(
      { walltime },
      {
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
      },
    )
  }

  // Don't render the button if a job is running
  if (isJobRunning) {
    return null
  }

  return (
    <div className="space-y-4">
      <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
        <DialogTrigger asChild>
          <Button
            variant="default"
            className="w-full"
            disabled={launchFlowMutation.isPending}
          >
            {launchFlowMutation.isPending
              ? "Launching..."
              : "Launch Streaming Session"}
          </Button>
        </DialogTrigger>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Set Session Duration</DialogTitle>
            <DialogDescription>
              Specify the walltime (in minutes) for your streaming session.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-4 items-center gap-4">
              <Label htmlFor="walltime" className="text-right">
                Duration
              </Label>
              <Input
                id="walltime"
                type="number"
                min="1"
                value={walltime}
                onChange={(e) =>
                  setWalltime(Number.parseInt(e.target.value) || 60)
                }
                className="col-span-3"
              />
            </div>
          </div>
          <DialogFooter>
            <Button
              onClick={handleLaunchFlow}
              disabled={launchFlowMutation.isPending}
            >
              {launchFlowMutation.isPending ? "Launching..." : "Launch"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <ErrorAlert error={error} />
    </div>
  )
}
