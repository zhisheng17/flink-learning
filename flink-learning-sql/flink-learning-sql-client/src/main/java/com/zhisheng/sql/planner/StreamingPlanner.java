package com.zhisheng.sql.planner;


import com.zhisheng.sql.cli.CliOptions;
import com.zhisheng.sql.cli.SqlCommandParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class StreamingPlanner extends Planner {

    public static StreamingPlanner build(CliOptions options) {
        return new StreamingPlanner(options);
    }

    private StreamingPlanner(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
        this.isTest = options.getIsTest();
        this.k8sClusterId = options.getK8sClusterId();
    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        this.tEnv = StreamTableEnvironment.create(bsEnv, settings);
        this.statementSet = tEnv.createStatementSet();
        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
        if (!isExplain) {
            TableResult execute = statementSet.execute();
            if (k8sClusterId != null) {
                getK8sStatus(execute);
            }
        }
        LOG.info("Job submitted SUCCESS");
    }


}
