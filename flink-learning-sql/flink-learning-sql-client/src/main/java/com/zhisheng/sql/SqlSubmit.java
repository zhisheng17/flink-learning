package com.zhisheng.sql;


import com.zhisheng.sql.cli.CliOptions;
import com.zhisheng.sql.cli.CliOptionsParser;
import com.zhisheng.sql.planner.BatchPlanner;
import com.zhisheng.sql.planner.Planner;
import com.zhisheng.sql.planner.StreamingPlanner;

public class SqlSubmit {

    public static void main(String[] args) throws Exception {

        final CliOptions options = CliOptionsParser.parseClient(args);
        Planner planner;
        if (!Boolean.parseBoolean(options.getIsBatch())) {
            planner = StreamingPlanner.build(options);
        } else {
            planner = BatchPlanner.build(options);
        }

        planner.run();
    }
}