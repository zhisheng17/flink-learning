package com.zhisheng.sql.blink.stream.tableFactory;

import org.apache.flink.table.factories.TableFactory;

import java.util.List;
import java.util.Map;

/**
 * Desc: custom table factory
 * https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/table/sourceSinks.html#define-a-tablefactory
 *
 * Created by zhisheng on 2020-01-26 21:13
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CustomMyTableFactory implements TableFactory {


    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }
}
