package org.apache.flink.metrics.kafka.util;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonUtil {

    private final static ObjectMapper mapper = new ObjectMapper();

    /**
     * 将对象转换成普通的 JSON 数据
     *
     * @param value
     * @return
     * @throws JsonProcessingException
     */
    public static String toJson(Object value) throws JsonProcessingException {
        return mapper.writeValueAsString(value);
    }


    /**
     * 将对象转换成结构化的 JSON 数据
     *
     * @param value
     * @return
     * @throws JsonProcessingException
     */
    public static String toFormatJson(Object value) throws JsonProcessingException {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    }


}
