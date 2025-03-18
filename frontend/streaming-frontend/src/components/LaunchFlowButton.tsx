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
import { PrefectState } from "@/types/flowTypes"
import axios from "axios"
import { useEffect, useState } from "react"
import { ErrorAlert } from "./ErrorAlert"

export function LaunchFlowButton() {
  const [error, setError] = useState<string | null>(null)
  const [walltime, setWalltime] = useState<number>(60)
  const [inputWalltime, setInputWalltime] = useState<number>(60)
  const [isDialogOpen, setIsDialogOpen] = useState(false)
  const { launchFlowMutation, flowRunInfos } = useFlowAPI()

  // Sync input walltime with the main walltime when dialog opens
  useEffect(() => {
    if (isDialogOpen) {
      setInputWalltime(walltime)
    }
  }, [isDialogOpen, walltime])

  const handleLaunchFlow = () => {
    setError(null)
    setIsDialogOpen(false)
    // Update the main walltime value from input when launching
    setWalltime(inputWalltime)

    // Pass the walltime to the mutation
    launchFlowMutation.mutate(
      { walltime: inputWalltime },
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

  // Don't render the button if a prefect flow is active and hasn't been canceled
  if (
    flowRunInfos.some(
      (info) =>
        info.state !== null &&
        [
          PrefectState.RUNNING,
          PrefectState.SCHEDULED,
          PrefectState.PENDING,
        ].includes(info.state),
    )
  ) {
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
                value={inputWalltime}
                onChange={(e) =>
                  setInputWalltime(Number.parseInt(e.target.value) || 60)
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