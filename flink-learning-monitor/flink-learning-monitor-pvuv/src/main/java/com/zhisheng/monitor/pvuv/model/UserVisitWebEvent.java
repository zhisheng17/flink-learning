package com.zhisheng.monitor.pvuv.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author fanrui
 * @date 2019-10-25 12:50:23
 * @desc 用户访问网页的日志
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserVisitWebEvent {

    /**
     * 日志的唯一 id
     */
    private String id;

    /**
     * 日期，如：20191025
      */
    private String date;

    /**
     * 页面 id
      */
    private Integer pageId;

    /**
     *  用户的唯一标识，用户 id
      */
    private String userId;

    /**
     * 页面的 url
      */
    private String url;

}