import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Button } from "@/components/ui/button"
import { useFlowAPI } from "@/hooks/useFlowAPI"
import axios from "axios"
import { useState } from "react"

type CancelFlowDialogProps = {
  flowId: string
}

export function CancelFlowDialog({ flowId }: CancelFlowDialogProps) {
  const [isOpen, setIsOpen] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const { cancelFlowMutation } = useFlowAPI()

  const isCancelling =
    cancelFlowMutation.isPending && cancelFlowMutation.variables === flowId
  const isCancelled =
    cancelFlowMutation.data?.message && cancelFlowMutation.variables === flowId

  const handleCancelClick = () => {
    setIsOpen(true)
  }

  const handleCancelConfirm = () => {
    setError(null)
    cancelFlowMutation.mutate(flowId, {
      onError: (err) => {
        if (axios.isAxiosError(err)) {
          const errorMessage =
            err.response?.data?.detail ||
            (typeof err.message === "string"
              ? err.message
              : "Failed to cancel session")
          setError(errorMessage)
        } else if (err instanceof Error) {
          setError(err.message)
        } else {
          setError("Unexpected error occurred while cancelling flow")
        }
        console.error("Error cancelling flow:", err)
      },
      onSettled: () => {
        setIsOpen(false)
      },
    })
  }

  return (
    <>
      {error && <div className="text-red-500 text-sm mt-1 mb-2">{error}</div>}

      <AlertDialog open={isOpen} onOpenChange={setIsOpen}>
        <AlertDialogTrigger asChild>
          <Button
            disabled={isCancelling || isCancelled}
            size="sm"
            className="bg-red-700 hover:bg-red-900 text-white disabled:text-white/70 transition-all duration-200"
            onClick={handleCancelClick}
          >
            {isCancelling ? "Cancelling..." : "Cancel Session"}
          </Button>
        </AlertDialogTrigger>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Cancel Session</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to cancel this session? This action cannot
              be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={handleCancelConfirm}
              className="bg-red-700 hover:bg-red-900 text-white transition-all duration-200"
            >
              {isCancelling ? "Cancelling..." : "Confirm"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </>
  )
}
