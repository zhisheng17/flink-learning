package com.zhisheng.connectors.kudu;

import com.zhisheng.connectors.kudu.connector.KuduConnector;
import com.zhisheng.connectors.kudu.connector.KuduRow;
import com.zhisheng.connectors.kudu.connector.KuduTableInfo;
import com.zhisheng.connectors.kudu.serde.KuduSerialization;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.SessionConfiguration.FlushMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KuduOutputFormat<OUT> extends RichOutputFormat<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMasters;
    private KuduTableInfo tableInfo;
    private KuduConnector.Consistency consistency;
    private KuduConnector.WriteMode writeMode;

    private KuduSerialization<OUT> serializer;

    private transient KuduConnector connector;


    public KuduOutputFormat(String kuduMasters, KuduTableInfo tableInfo, KuduSerialization<OUT> serializer) {
        Preconditions.checkNotNull(kuduMasters,"kuduMasters could not be null");
        this.kuduMasters = kuduMasters;

        Preconditions.checkNotNull(tableInfo,"tableInfo could not be null");
        this.tableInfo = tableInfo;
        this.consistency = KuduConnector.Consistency.STRONG;
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        this.serializer = serializer.withSchema(tableInfo.getSchema());
    }


    public KuduOutputFormat<OUT> withEventualConsistency() {
        this.consistency = KuduConnector.Consistency.EVENTUAL;
        return this;
    }

    public KuduOutputFormat<OUT> withStrongConsistency() {
        this.consistency = KuduConnector.Consistency.STRONG;
        return this;
    }

    public KuduOutputFormat<OUT> withUpsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPSERT;
        return this;
    }

    public KuduOutputFormat<OUT> withInsertWriteMode() {
        this.writeMode = KuduConnector.WriteMode.INSERT;
        return this;
    }

    public KuduOutputFormat<OUT> withUpdateWriteMode() {
        this.writeMode = KuduConnector.WriteMode.UPDATE;
        return this;
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        if (connector != null) return;
        connector = new KuduConnector(kuduMasters, tableInfo, consistency, writeMode,FlushMode.AUTO_FLUSH_SYNC);
        serializer = serializer.withSchema(tableInfo.getSchema());
    }

    @Override
    public void writeRecord(OUT row) throws IOException {
        boolean response;
        try {
            KuduRow kuduRow = serializer.serialize(row);
            response = connector.writeRow(kuduRow);
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }

        if(!response) {
            throw new IOException("error with some transaction");
        }
    }

    @Override
    public void close() throws IOException {
        if (this.connector == null) return;
        try {
            this.connector.close();
        } catch (Exception e) {
            throw new IOException(e.getLocalizedMessage(), e);
        }
    }
}

