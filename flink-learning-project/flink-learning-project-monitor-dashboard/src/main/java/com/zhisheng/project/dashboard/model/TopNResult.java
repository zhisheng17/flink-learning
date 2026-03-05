package com.zhisheng.project.dashboard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * TopN 排行榜结果
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TopNResult {
    /** 排行榜名称 */
    private String rankName;
    /** 排行榜数据 */
    private List<RankItem> items;
    /** 窗口结束时间 */
    private Long windowEnd;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class RankItem {
        /** 排名 */
        private Integer rank;
        /** 页面/项目名称 */
        private String name;
        /** 访问量 */
        private Long count;
    }
}
