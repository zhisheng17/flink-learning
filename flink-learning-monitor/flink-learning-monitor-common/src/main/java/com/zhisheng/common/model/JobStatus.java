package com.zhisheng.common.model;


public enum JobStatus {
    /**
     * 	Job is newly created, no task has started to run.
     */
    CREATED,

    /**
     * Some tasks are scheduled or running, some may be pending, some may be finished.
     */
    RUNNING,

    /**
     * The job has failed and is currently waiting for the cleanup to complete
     */
    FAILING,

    /**
     * The job has failed with a non-recoverable task failure
     */
    FAILED,

    /**
     * Job is being cancelled
     */
    CANCELLING,

    /**
     * Job has been cancelled
     */
    CANCELED,

    /**
     * All of the job's tasks have successfully finished
     */
    FINISHED,

    /**
     * The job is currently undergoing a reset and total restart
     */
    RESTARTING,

    /**
     * The job has been suspended and is currently waiting for the cleanup to complete
     */
    SUSPENDING,

    /**
     * The job has been suspended which means that it has been stopped but not been removed from a
     * potential HA job store.
     */
    SUSPENDED,

    /**
     * The job is currently reconciling and waits for task execution report to recover state.
     */
    RECONCILING;
}
