package io.d8a.hbase;

import java.io.IOException;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class FullGcTest {
    private static final byte[] BIG_BYTES = new byte[1024 * 1024 * 9];

    /**
     * Adds a single, large row to a new table, then keeps trying to scan that table until it can scan all
     * the cells. This usually fails the first couple attempts (finds NO results & cells), and then succeeds.
     */
    public static void main(String[] args) throws ServiceException, IOException, InterruptedException {
        final Configuration hbaseConf = HBaseConfiguration.create();
        HBaseAdmin.checkHBaseAvailable(hbaseConf);

        int expectedCellCount = 99;

        HTableInterface table = null;
        try (HConnection hConnection = HConnectionManager.createConnection(hbaseConf)) {
            table = createTable(hbaseConf, hConnection);

            insertBigRow(table, expectedCellCount);

            for (int attempt = 0; attempt < 10; attempt++) {
                int actualCellCount = doScan(table);

                if (actualCellCount == expectedCellCount) {
                    System.err.println("Scan successful after " + (attempt + 1) + " attempts.");
                    return;
                } else {
                    System.err.println("Unexpected cell count in scan: " + actualCellCount + "/" + expectedCellCount + ".");
                    System.err.println("Did NOT find the row after " + (attempt + 1) + " attempts. Sleeping and will re-scan.");
                    Thread.sleep(5000);
                }
            }
        } finally {
            if (table != null) table.close();
        }
    }

    private static HTableInterface createTable(Configuration hbaseConf, HConnection hConnection) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);

        String tableName = "big_row_"+System.currentTimeMillis();
        System.out.println("table="+tableName);
        HTableDescriptor td = new HTableDescriptor(TableName.valueOf(tableName));
        td.addFamily(new HColumnDescriptor("a"));

        // Uncomment to test whether early splits have any impact. Though, they seem not to, especially since
        // there is only 1 row here and the region is unable to split.
        //td.setConfiguration("hbase.regionserver.region.split.policy", "org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy");
        //td.setConfiguration("hbase.hregion.max.filesize", "10737418240");

        // Switch creates to pre-split to test whether pre-split has an impact. Again, seems not to.
        //admin.createTable(td, createSplits(1,2));
        admin.createTable(td);

        waitForTableAvailable(admin, tableName);
        return hConnection.getTable(tableName);
    }

    private static int doScan(HTableInterface table) throws IOException {
        Scan scan = new Scan();

        // Setting a batch size definitely fixes this test, but may hide the underlying issue. eg if a GC may be
        // causing the missing row, perhaps a GC anywhere could do so?
        //scan.setBatch(10);

        scan.setMaxResultSize(1024 * 1024 * 2);
        scan.addFamily(Bytes.toBytes("a"));

        int c = 0;
        try(ResultScanner scanner = table.getScanner(scan)){
            for (Object result : scanner) {
                CellScanner cscan = ((Result)result).cellScanner();
                while (cscan.advance()) {
                    c++;
                }
            }
        }

        return c;
    }

    private static void insertBigRow(HTableInterface table, int numCells) throws IOException {
        for (int i=0; i<numCells; i++) {
            Put put = new Put(Bytes.toBytes("abc"));
            put.add(Bytes.toBytes("a"), Bytes.toBytes(i), BIG_BYTES);
            table.put(put);
        }
        table.flushCommits();
    }


    private static void waitForTableAvailable(HBaseAdmin admin, String tableName) throws IOException {
        while (!admin.isTableAvailable(tableName)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
    }

    public static byte[][] createSplits(int partitions, int splitsPerPartition) {
        byte[][] splits = new byte[(partitions * splitsPerPartition) - 1][];

        int step = 256 / splitsPerPartition;

        for (int i = 0; i < splits.length; i++) {
            splits[i] = new byte[2];
        }

        for (int p = 0; p < partitions; p++) {
            for (int s = 0; s < splitsPerPartition; s++) {
                int offset = (p * splitsPerPartition) + s;
                if (offset == 0) {
                    continue;
                }
                offset -= 1;
                splits[offset][0] = (byte) p;
                splits[offset][1] = (byte) ((step * s) & 0xff);
            }
        }

        return splits;
    }
}
