package com.zhisheng.connectors.cassandra.batch;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Table(name = CustomCassandraAnnotatedPojo.TABLE_NAME, keyspace = "flink")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class CustomCassandraAnnotatedPojo {
    public static final String TABLE_NAME = "zhisheng";

    @Column(name = "id")
    private String id;
    @Column(name = "counter")
    private Integer counter;
    @Column(name = "batch_id")
    private Integer batchId;
}
