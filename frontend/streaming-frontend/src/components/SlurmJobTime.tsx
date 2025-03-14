import { Badge } from "@/components/ui/badge"
import { Progress } from "@/components/ui/progress"
import { cn } from "@/lib/utils"

interface SlurmJobTimeProps {
  elapsed: string | null
  timelimit: string | null
  display: boolean
}

// Convert HH:MM:SS format to seconds
function timeToSeconds(timeStr: string | null): number {
  if (!timeStr) return 0

  const [hours, minutes, seconds] = timeStr.split(":").map(Number)
  return hours * 3600 + minutes * 60 + seconds
}

export function SlurmJobTime({
  elapsed,
  timelimit,
  display = true,
}: SlurmJobTimeProps) {
  if (!elapsed || !timelimit || !display) return null

  const elapsedSeconds = timeToSeconds(elapsed)
  const limitSeconds = timeToSeconds(timelimit)

  // Check if elapsed time exceeds the time limit
  const isOvertime = elapsedSeconds > limitSeconds

  // Calculate percentage of time used, capped at 100%
  const percentageUsed = Math.min(
    Math.round((elapsedSeconds / limitSeconds) * 100),
    100,
  )

  // Calculate time remaining or overtime
  const diffSeconds = Math.abs(limitSeconds - elapsedSeconds)
  const diffHours = Math.floor(diffSeconds / 3600)
  const diffMinutes = Math.floor((diffSeconds % 3600) / 60)

  let timeStatusText = ""
  if (isOvertime) {
    timeStatusText = `${diffHours > 0 ? `${diffHours}h ` : ""}${diffMinutes}m over limit`
  } else {
    timeStatusText = `${diffHours > 0 ? `${diffHours}h ` : ""}${diffMinutes}m remaining`
  }

  // Determine progress bar color based on percentage used
  const getProgressColor = () => {
    if (isOvertime) return "bg-destructive"
    if (percentageUsed < 50) return "bg-emerald-500"
    if (percentageUsed < 75) return "bg-amber-500"
    if (percentageUsed < 90) return "bg-red-600"
    return "bg-red-500"
  }

  return (
    <div className="w-full mb-2 space-y-1">
      <div className="flex justify-between text-xs text-muted-foreground">
        <span>{elapsed}</span>
        <span className="flex items-center">
          {isOvertime && (
            <Badge variant="destructive" className="mr-1 py-0 text-[10px]">
              OVERTIME
            </Badge>
          )}
          {timeStatusText}
        </span>
        <span>{timelimit}</span>
      </div>
      <Progress
        value={percentageUsed}
        className="h-1.5"
        indicatorClassName={cn(
          getProgressColor(),
          "transition-colors duration-300",
        )}
      />
    </div>
  )
}
