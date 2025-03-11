import { Card, CardContent } from "@/components/ui/card"
import { FlowList } from "./FlowList"
import { LaunchFlowButton } from "./LaunchFlowButton"

export function FlowDashboard() {
  return (
    <div className="container mx-auto py-8 max-w-2xl">
      <h1 className="text-3xl font-bold mb-6 text-center">
        Streaming Dashboard
      </h1>
      <Card>
        <CardContent className="space-y-6">
          <LaunchFlowButton />
          <div className="pt-6 border-t">
            <FlowList />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
