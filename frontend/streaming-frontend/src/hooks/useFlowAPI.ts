import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import axios from "axios"
import type { FlowRunInfo } from "../types/flowTypes"

export function useFlowAPI() {
  const queryClient = useQueryClient()

  // Fetch running flows with useQuery
  const { data: flowRunInfos = [], isLoading: isFetchingFlows } = useQuery({
    queryKey: ["flowRuns"],
    queryFn: async () => {
      const response = await axios.get("http://localhost:8000/flows/running")
      return response.data.flow_run_infos as FlowRunInfo[]
    },
    refetchInterval: 5000, // Auto-refresh every 5 seconds
  })

  // Launch flow mutation
  const launchFlowMutation = useMutation({
    mutationFn: async () => {
      const response = await axios.post("http://localhost:8000/flows/launch", {
        walltime_minutes: 5,
      })
      return response.data
    },
    onSuccess: () => {
      // Refetch flow runs after launching a new one
      queryClient.invalidateQueries({ queryKey: ["flowRuns"] })
    },
  })

  // Cancel flow mutation
  const cancelFlowMutation = useMutation({
    mutationFn: async (flowId: string) => {
      const response = await axios.delete(
        `http://localhost:8000/flows/${flowId}`,
      )
      return response.data
    },
    onSuccess: () => {
      // Refetch flow runs after cancelling
      queryClient.invalidateQueries({ queryKey: ["flowRuns"] })
    },
  })

  return {
    flowRunInfos,
    isFetchingFlows,
    launchFlowMutation,
    cancelFlowMutation,
  }
}
