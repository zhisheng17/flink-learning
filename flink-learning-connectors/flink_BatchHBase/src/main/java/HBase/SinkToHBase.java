package HBase;

import model.Student;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import java.util.List;

/**
 * by 光城
 */
public class SinkToHBase extends RichSinkFunction<List<Student>> {

    private Connection conn = null;
    private static TableName tableName = TableName.valueOf("student");
    private static final String info = "info";
    private BufferedMutator mutator;


    /**
     * open() 方法中建立连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        try {
            conn = ConnectionFactory.createConnection(config);

            System.out.println("getConnection连接成功！！！");
        } catch (IOException e) {
            System.out.println("HBase 建立连接失败 ");
        }


        BufferedMutatorParams params = new BufferedMutatorParams(tableName);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        mutator = conn.getBufferedMutator(params);

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null){
            conn.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Student> value, Context context) throws Exception {

        //遍历数据集合
        for (Student student : value) {
            Put put = new Put(Bytes.toBytes(String.valueOf(student.getId())));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("name"), Bytes.toBytes(student.getName()));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("password"), Bytes.toBytes(student.getPassword()));
            put.addColumn(Bytes.toBytes(info), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(student.getAge())));
            mutator.mutate(put);
            System.out.println("id:" + student.getId());
        }
        mutator.flush();
        System.out.println("成功了插入了" + value.size() + "行数据");
    }
}