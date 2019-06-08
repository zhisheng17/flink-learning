package com.zhisheng.connectors.influxdb;

import com.zhisheng.common.model.MetricEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Desc: InfluxDB sink
 * Created by zhisheng on 2019-05-01
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class InfluxDBSink extends RichSinkFunction<MetricEvent> {

    private transient InfluxDB influxDBClient;

    private final InfluxDBConfig influxDBConfig;

    public InfluxDBSink(InfluxDBConfig influxDBConfig) {
        this.influxDBConfig = Preconditions.checkNotNull(influxDBConfig, "InfluxDB client config should not be null");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        influxDBClient = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword());

        if (!influxDBClient.databaseExists(influxDBConfig.getDatabase())) {
            if(influxDBConfig.isCreateDatabase()) {
                influxDBClient.createDatabase(influxDBConfig.getDatabase());
            }
            else {
                throw new RuntimeException("This " + influxDBConfig.getDatabase() + " database does not exist!");
            }
        }

        influxDBClient.setDatabase(influxDBConfig.getDatabase());

        if (influxDBConfig.getBatchActions() > 0) {
            influxDBClient.enableBatch(influxDBConfig.getBatchActions(), influxDBConfig.getFlushDuration(), influxDBConfig.getFlushDurationTimeUnit());
        }

        if (influxDBConfig.isEnableGzip()) {
            influxDBClient.enableGzip();
        }
    }

    @Override
    public void invoke(MetricEvent metricEvent, Context context) throws Exception {
        if (StringUtils.isNullOrWhitespaceOnly(metricEvent.getName())) {
            throw new RuntimeException("No measurement defined");
        }

        Point.Builder builder = Point.measurement(metricEvent.getName())
                .time(metricEvent.getTimestamp(), TimeUnit.MILLISECONDS);

        if (!CollectionUtil.isNullOrEmpty(metricEvent.getFields())) {
            builder.fields(metricEvent.getFields());
        }

        if (!CollectionUtil.isNullOrEmpty(metricEvent.getTags())) {
            builder.tag(metricEvent.getTags());
        }

        Point point = builder.build();
        influxDBClient.write(point);
    }

    @Override
    public void close() {
        if (influxDBClient.isBatchEnabled()) {
            influxDBClient.disableBatch();
        }
        influxDBClient.close();
    }
}
