package com.zhisheng.project.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 服务器指标模型
 * 用于监控告警系统和实时大屏
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ServerMetric {
    /** 指标名称，如：cpu_usage, memory_usage, disk_io, network_in */
    private String metricName;
    /** 指标值 */
    private Double value;
    /** 主机名 */
    private String host;
    /** 应用名称 */
    private String application;
    /** 采集时间戳 */
    private Long timestamp;
    /** 标签，可用于多维分析 */
    private String tags;
}
