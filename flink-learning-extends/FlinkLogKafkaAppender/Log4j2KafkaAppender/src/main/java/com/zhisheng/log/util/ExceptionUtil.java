package com.zhisheng.log.util;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class ExceptionUtil {

    private static final char TAB = '	';
    private static final char CR = '\r';
    private static final char LF = '\n';
    private static final String SPACE = " ";
    private static final String EMPTY = "";

    /**
     * 堆栈转为单行完整字符串
     *
     * @param throwable 异常对象
     * @param limit     限制最大长度
     * @return 堆栈转为的字符串
     */
    public static String stacktraceToOneLineString(Throwable throwable, int limit) {
        Map<Character, String> replaceCharToStrMap = new HashMap<>();
        replaceCharToStrMap.put(CR, SPACE);
        replaceCharToStrMap.put(LF, SPACE);
        replaceCharToStrMap.put(TAB, SPACE);
        return stacktraceToString(throwable, limit, replaceCharToStrMap);
    }

    public static String stacktraceToString(Throwable throwable) {
        final OutputStream baos = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(baos));
        return baos.toString();
    }


    /**
     * 堆栈转为完整字符串
     *
     * @param throwable           异常对象
     * @param limit               限制最大长度
     * @param replaceCharToStrMap 替换字符为指定字符串
     * @return 堆栈转为的字符串
     */
    private static String stacktraceToString(Throwable throwable, int limit, Map<Character, String> replaceCharToStrMap) {
        final OutputStream baos = new ByteArrayOutputStream();
        throwable.printStackTrace(new PrintStream(baos));
        String exceptionStr = baos.toString();
        int length = exceptionStr.length();
        if (limit > 0 && limit < length) {
            length = limit;
        }

        if (!replaceCharToStrMap.isEmpty()) {
            final StringBuilder sb = new StringBuilder();
            char c;
            String value;
            for (int i = 0; i < length; i++) {
                c = exceptionStr.charAt(i);
                value = replaceCharToStrMap.get(c);
                if (null != value) {
                    sb.append(value);
                } else {
                    sb.append(c);
                }
            }
            return sb.toString();
        } else {
            return sub(exceptionStr, 0, limit);
        }
    }

    /**
     * 改进JDK subString<br>
     * index从0开始计算，最后一个字符为-1<br>
     * 如果from和to位置一样，返回 "" <br>
     * 如果from或to为负数，则按照length从后向前数位置，如果绝对值大于字符串长度，则from归到0，to归到length<br>
     * 如果经过修正的index中from大于to，则互换from和to example: <br>
     * abcdefgh 2 3 =》 c <br>
     * abcdefgh 2 -3 =》 cde <br>
     *
     * @param str       String
     * @param fromIndex 开始的index（包括）
     * @param toIndex   结束的index（不包括）
     * @return 字串
     */
    private static String sub(CharSequence str, int fromIndex, int toIndex) {
        if (isEmpty(str)) {
            return str(str);
        }
        int len = str.length();

        if (fromIndex < 0) {
            fromIndex = len + fromIndex;
            if (fromIndex < 0) {
                fromIndex = 0;
            }
        } else if (fromIndex > len) {
            fromIndex = len;
        }

        if (toIndex < 0) {
            toIndex = len + toIndex;
            if (toIndex < 0) {
                toIndex = len;
            }
        } else if (toIndex > len) {
            toIndex = len;
        }

        if (toIndex < fromIndex) {
            int tmp = fromIndex;
            fromIndex = toIndex;
            toIndex = tmp;
        }

        if (fromIndex == toIndex) {
            return EMPTY;
        }

        return str.toString().substring(fromIndex, toIndex);
    }

    /**
     * {@link CharSequence} 转为字符串，null安全
     *
     * @param cs {@link CharSequence}
     * @return 字符串
     */
    private static String str(CharSequence cs) {
        return null == cs ? null : cs.toString();
    }

    /**
     * 字符串是否为空，空的定义如下:<br>
     * 1、为null <br>
     * 2、为""<br>
     *
     * @param str 被检测的字符串
     * @return 是否为空
     */
    private static boolean isEmpty(CharSequence str) {
        return str == null || str.length() == 0;
    }
}
