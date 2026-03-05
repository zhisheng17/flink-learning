package com.zhisheng.project.log.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 日志统计结果模型
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LogStatistics {
    /** 统计维度：服务名称 */
    private String serviceName;
    /** 统计维度：日志级别 */
    private String level;
    /** 统计数量 */
    private Long count;
    /** 窗口开始时间 */
    private Long windowStart;
    /** 窗口结束时间 */
    private Long windowEnd;
}
