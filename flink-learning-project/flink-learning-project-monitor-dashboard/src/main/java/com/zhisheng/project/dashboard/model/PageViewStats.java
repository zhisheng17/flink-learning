package com.zhisheng.project.dashboard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 页面访问统计结果
 * 用于实时大屏展示
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PageViewStats {
    /** 页面 URL */
    private String pageUrl;
    /** 页面类别 */
    private String pageCategory;
    /** PV（页面浏览量） */
    private Long pv;
    /** UV（独立访客数） */
    private Long uv;
    /** 平均停留时长（毫秒） */
    private Long avgStayDuration;
    /** 窗口开始时间 */
    private Long windowStart;
    /** 窗口结束时间 */
    private Long windowEnd;
}
