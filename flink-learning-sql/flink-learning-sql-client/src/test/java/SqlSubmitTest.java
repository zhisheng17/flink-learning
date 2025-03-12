import com.zhisheng.sql.SqlSubmit;
import com.zhisheng.sql.cli.CliOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlSubmitTest {


    @Test
    public void testMain() throws Exception {
        String[] args = new String[]{"-w", "src/test/resources/sql", "-f", "test.sql", "-t", "false", "-b", "false", "-k8d", "flink-373362-1741687273399"};
        SqlSubmit.main(args);
    }

    @Test
    public void testCliOptions() {
        CliOptions cliOptions = new CliOptions("sqlFilePath", "workingSpace", "isTest", "isBatch", "k8sClusterId");
        assertEquals("sqlFilePath", cliOptions.getSqlFilePath());
        assertEquals("workingSpace", cliOptions.getWorkingSpace());
        assertEquals("isTest", cliOptions.getIsTest());
        assertEquals("isBatch", cliOptions.getIsBatch());
        assertEquals("k8sClusterId", cliOptions.getK8sClusterId());
    }

}
