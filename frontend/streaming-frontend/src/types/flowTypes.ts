export enum StateType {
  SCHEDULED = "SCHEDULED",
  PENDING = "PENDING",
  RUNNING = "RUNNING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  CANCELLED = "CANCELLED",
  CRASH = "CRASH",
}

export enum SlurmJobState { 
    BOOT_FAIL = "BOOT_FAIL",
    CANCELLED = "CANCELLED",
    COMPLETED = "COMPLETED",
    CONFIGURING = "CONFIGURING",
    COMPLETING = "COMPLETING",
    DEADLINE = "DEADLINE",
    FAILED = "FAILED",
    NODE_FAIL = "NODE_FAIL",
    OUT_OF_MEMORY = "OUT_OF_MEMORY",
    PENDING = "PENDING",
    PREEMPTED = "PREEMPTED",
    RUNNING = "RUNNING",
    RESV_DEL_HOLD = "RESV_DEL_HOLD",
    REQUEUE_FED = "REQUEUE_FED",
    REQUEUE_HOLD = "REQUEUE_HOLD",
    REQUEUED = "REQUEUED",
    RESIZING = "RESIZING",
    REVOKED = "REVOKED",
    SIGNALING = "SIGNALING",
    SPECIAL_EXIT = "SPECIAL_EXIT",
    STAGE_OUT = "STAGE_OUT",
    STOPPED = "STOPPED",
    SUSPENDED = "SUSPENDED",
    TIMEOUT = "TIMEOUT"
}


export type SlurmJobInfo = {
    job_id: string | null
    job_state: SlurmJobState
}

export type FlowRunInfo = {
  id: string
  state: StateType | null
  slurm_job_info: SlurmJobInfo | null
}
