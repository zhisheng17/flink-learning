package com.zhisheng.connectors.kudu.connector;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KuduRowIterator {
    private KuduScanner scanner;
    private RowResultIterator rowIterator;

    public KuduRowIterator(KuduScanner scanner) throws KuduException {
        this.scanner = scanner;
        nextRows();
    }

    public void close() throws KuduException {
        scanner.close();
    }

    public boolean hasNext() throws KuduException {
        if (rowIterator.hasNext()) {
            return true;
        } else if (scanner.hasMoreRows()) {
            nextRows();
            return true;
        } else {
            return false;
        }
    }

    public KuduRow next() {
        RowResult row = this.rowIterator.next();
        return KuduMapper.toKuduRow(row);
    }

    private void nextRows() throws KuduException {
        this.rowIterator = scanner.nextRows();
    }
}
