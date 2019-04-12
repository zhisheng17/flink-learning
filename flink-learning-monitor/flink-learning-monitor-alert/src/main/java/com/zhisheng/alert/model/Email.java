package com.zhisheng.alert.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Email {
    /**
     * 收件人
     */
    private Set<String> to;

    /**
     * 邮件主题
     */
    private String subject;

    /**
     * 邮件正文
     */
    private String content;

    /**
     * 正文是否是 HTML
     */
    private boolean isHtml;

    /**
     * 附件路径
     */
    private Map<String, File> attachments;

    /**
     * 是否有附件
     */
    private boolean isAttachment;
}
