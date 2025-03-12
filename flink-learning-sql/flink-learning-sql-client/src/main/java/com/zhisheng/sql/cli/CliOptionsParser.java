package com.zhisheng.sql.cli;


import com.zhisheng.sql.exception.SqlParserException;
import org.apache.commons.cli.*;

public class CliOptionsParser {

    public static final Option OPTION_WORKING_SPACE = Option
            .builder("w")
            .required(true)
            .longOpt("working_space")
            .numberOfArgs(1)
            .argName("working space dir")
            .desc("The working space dir.")
            .build();

    public static final Option OPTION_SQL_FILE = Option
            .builder("f")
            .required(true)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("SQL file path")
            .desc("The SQL file path.")
            .build();

    public static final Option OPTION_ISTEST = Option
            .builder("t")
            .required(false)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("IS TEST RUN")
            .desc("IS TEST RUN")
            .build();

    public static final Option OPTION_ISBATCH = Option
            .builder("b")
            .required(false)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("IS BACTH MODE")
            .desc("IS BACTH MODE")
            .build();

    public static final Option OPTION_K8S_ID = Option
            .builder("k8d")
            .required(false)
            .longOpt("file")
            .numberOfArgs(1)
            .argName("k8s id")
            .desc("k8s id")
            .build();

    public static final Options CLIENT_OPTIONS = getClientOptions(new Options());

    public static Options getClientOptions(Options options) {
        options.addOption(OPTION_SQL_FILE);
        options.addOption(OPTION_WORKING_SPACE);
        options.addOption(OPTION_ISTEST);
        options.addOption(OPTION_ISBATCH);
        options.addOption(OPTION_K8S_ID);
        return options;
    }

    // --------------------------------------------------------------------------------------------
    //  Line Parsing
    // --------------------------------------------------------------------------------------------

    public static CliOptions parseClient(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("./sql-submit -w <work_space_dir> -f <sql-file> -t <isTest:true|false> -b true -k8d flink");
        }
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(CLIENT_OPTIONS, args, true);
            return new CliOptions(
                    line.getOptionValue(CliOptionsParser.OPTION_SQL_FILE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_WORKING_SPACE.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_ISTEST.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_ISBATCH.getOpt()),
                    line.getOptionValue(CliOptionsParser.OPTION_K8S_ID.getOpt())
            );
        } catch (ParseException e) {
            throw new SqlParserException(e.getMessage());
        }
    }
}
