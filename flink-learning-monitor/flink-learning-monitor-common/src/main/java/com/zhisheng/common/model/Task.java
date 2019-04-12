package com.zhisheng.common.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Task {
    /**
     * task 总个数
     */
    private int total;

    /**
     * 处于 created 状态的 task 个数
     */
    private int created;

    /**
     * 处于 scheduled 状态的 task 个数
     */
    private int scheduled;

    /**
     * 处于 deploying 状态的 task 个数
     */
    private int deploying;

    /**
     * 处于 running 状态的 task 个数
     */
    private int running;

    /**
     * 处于 finished 状态的 task 个数
     */
    private int finished;

    /**
     * 处于 canceling 状态的 task 个数
     */
    private int canceling;

    /**
     * 处于 canceled 状态的 task 个数
     */
    private int canceled;

    /**
     * 处于 failed 状态的 task 个数
     */
    private int failed;

    /**
     * 处于 reconciling 状态的 task 个数
     */
    private int reconciling;
}
