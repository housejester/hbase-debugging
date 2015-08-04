package io.d8a.hbase;


import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class RsCrashTest {
    private static final byte[] BIG_BYTES = new byte[1024 * 1024 * 9];

    public static void main(String[] args) throws ServiceException, IOException {
        final Configuration hbaseConf = HBaseConfiguration.create();
        HBaseAdmin.checkHBaseAvailable(hbaseConf);

        HConnection hConnection = HConnectionManager.createConnection(hbaseConf);

        HTableInterface table = null;
        ResultScanner scanner = null;
        try {
            table = createTable(hbaseConf, hConnection);

            int numCells = (Integer.MAX_VALUE / BIG_BYTES.length) + 100;
//            int numCells = 3;
            System.out.println("numCells=" + numCells);

            for (int i = 0; i < numCells; i++) {
                Put put = new Put(Bytes.toBytes("abc"));
                put.add(Bytes.toBytes("a"), Bytes.toBytes(i), BIG_BYTES);
                table.put(put);
            }

            Scan scan = new Scan();
            scan.setBatch(10);
//            scan.setMaxResultSize(1024 * 1024 * 1); // 1mb max

            scan.addFamily(Bytes.toBytes("a"));

            System.out.println("Scanning...");
            scanner = table.getScanner(scan);
            Iterator<Result> results = scanner.iterator();
            int numRows = 0;

            while (results.hasNext()) {
                ++numRows;
                Result result = results.next();
                CellScanner cells = result.cellScanner();
                while (cells.advance()) {
                    Cell cell = cells.current();
                    System.out.println("r=" + numRows + ", i=" + Bytes.toInt(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                }
            }
            System.out.println("DONE!");
        } finally {
            if (scanner != null) scanner.close();
            if (table != null) table.close();
            hConnection.close();
        }
    }

    private static HTableInterface createTable(Configuration hbaseConf, HConnection hConnection) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        String tableName = "big_row_" + System.currentTimeMillis();
        System.out.println("table=" + tableName);
        HTableDescriptor td = new HTableDescriptor(tableName);
        td.addFamily(new HColumnDescriptor("a"));

        admin.createTable(td);
        waitForTableAvailable(admin, tableName);

        return hConnection.getTable(tableName);
    }

    private static void waitForTableAvailable(HBaseAdmin admin, String tableName) throws IOException {
        while (!admin.isTableAvailable(tableName)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }
}
