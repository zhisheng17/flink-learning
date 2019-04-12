package com.zhisheng.common.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Job {

    /**
     * job id
     */
    private String jid;

    /**
     * job name
     */
    private String name;

    /**
     * job status
     */
    private JobStatus state;

    /**
     * job start time
     */
    private Long startTime;

    /**
     * job end time
     */
    private Long endTime;

    /**
     * job duration time
     */
    private Long duration;

    /**
     * job last modify time
     */
    private Long lastModification;

    /**
     * job tasks
     */
    private Task tasks;
}
