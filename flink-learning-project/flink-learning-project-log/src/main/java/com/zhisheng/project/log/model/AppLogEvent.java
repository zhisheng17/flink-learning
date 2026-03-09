package com.zhisheng.project.log.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 应用日志事件模型
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AppLogEvent {
    /** 日志 ID */
    private String logId;
    /** 日志级别：DEBUG, INFO, WARN, ERROR, FATAL */
    private String level;
    /** 日志消息 */
    private String message;
    /** 产生日志的服务名称 */
    private String serviceName;
    /** 产生日志的类名 */
    private String className;
    /** 线程名 */
    private String threadName;
    /** 异常堆栈信息（可选） */
    private String exception;
    /** 日志时间戳 */
    private Long timestamp;
    /** 主机名 */
    private String host;
    /** Trace ID，用于链路追踪 */
    private String traceId;
}
