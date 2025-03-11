export enum StateType {
  SCHEDULED = "SCHEDULED",
  PENDING = "PENDING",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  CANCELLED = "CANCELLED",
  CRASH = "CRASH"
}

export type FlowRunInfo = {
  id: string
  state: StateType | null
}