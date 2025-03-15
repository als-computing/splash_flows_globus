import { useFlowAPI } from "@/hooks/useFlowAPI"
import { FlowListItem } from "./FlowListItem"

export function FlowList() {
  const { flowRunInfos } = useFlowAPI()

  return (
    <div className="space-y-4">
      {flowRunInfos.length > 0 && (
        <ul className="space-y-2">
          {flowRunInfos.map((info, index) => (
            <FlowListItem key={index} info={info} />
          ))}
        </ul>
      )}
    </div>
  )
}