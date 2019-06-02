package com.zhisheng.common.utils;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class StringUtil {
    /**
     * 判空
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }

    /**
     * 判非空
     *
     * @param str
     * @return
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isContains(String str1, String str2) {
        return str1.contains(str2);
    }

    /**
     * 不包含
     *
     * @param str1
     * @param str2
     * @return
     */
    public static boolean isNotContains(String str1, String str2) {
        return !isContains(str1, str2);
    }
}
