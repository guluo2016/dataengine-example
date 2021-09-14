package com.h3c.hbase.example.normal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 功能描述
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 *
 * @since 2013
 */
public class HBaseSample {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSample.class.getName());

    private TableName tableName = null;

    private Connection conn = null;

    public HBaseSample(Configuration conf) throws IOException {
        this.tableName = TableName.valueOf("hbase_sample_table");
        this.conn = ConnectionFactory.createConnection(conf);
    }

    /**
     * HBaseSample test
     */
    public void test() {
        try {
            testCreateTable();
            testPut();
            testModifyTable();
            testGet();
            testScanData();
            testSingleColumnValueFilter();
            testFilterList();
            testDelete();
            dropTable();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e1) {
                    LOG.error("Failed to close the connection ", e1);
                }
            }
        }
    }

    /**
     * Create user info table
     */
    public void testCreateTable() {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        TableDescriptorBuilder htd = TableDescriptorBuilder.newBuilder(tableName);

        // Set the column family name to info.
        ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));

        // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.setColumnFamily(hcd.build());

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table...");
                admin.createTable(htd.build());
                LOG.info(admin.getClusterMetrics().toString());
                LOG.info(admin.listNamespaceDescriptors().toString());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }


    private static Put putData(byte[] familyName, byte[][] qualifiers, List<String> data) {
        Put put = new Put(Bytes.toBytes(data.get(0)));
        put.addColumn(familyName, qualifiers[0], Bytes.toBytes(data.get(1)));
        put.addColumn(familyName, qualifiers[1], Bytes.toBytes(data.get(2)));
        put.addColumn(familyName, qualifiers[2], Bytes.toBytes(data.get(3)));
        put.addColumn(familyName, qualifiers[3], Bytes.toBytes(data.get(4)));
        return put;
    }

    /**
     * Insert data
     */
    public void testPut() {
        LOG.info("Entering testPut.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifiers = {
            Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"), Bytes.toBytes("address")
        };

        Table table = null;
        try {
            // Instantiate an HTable object.
            table = conn.getTable(tableName);
            List<Put> puts = new ArrayList<Put>();

            // Instantiate a Put object.
            Put put = putData(familyName, qualifiers,
                Arrays.asList("012005000201", "Zhang San", "Male", "19", "Shenzhen, Guangdong"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000202", "Li Wanting", "Female", "23", "Shijiazhuang, Hebei"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000203", "Wang Ming", "Male", "26", "Ningbo, Zhejiang"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000204", "Li Gang", "Male", "18", "Xiangyang, Hubei"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000205", "Zhao Enru", "Female", "21", "Shangrao, Jiangxi"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000206", "Chen Long", "Male", "32", "Zhuzhou, Hunan"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000207", "Zhou Wei", "Female", "29", "Nanyang, Henan"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000208", "Yang Yiwen", "Female", "30", "Kaixian, Chongqing"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000209", "Xu Bing", "Male", "26", "Weinan, Shaanxi"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000210", "Xiao Kai", "Male", "25", "Dalian, Liaoning"));
            puts.add(put);

            // Submit a put request.
            table.put(puts);

            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testPut.");
    }


    /**
     * Modify a Table
     */
    public void testModifyTable() {
        LOG.info("Entering testModifyTable.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("education");

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();

            // Obtain the table descriptor.
            TableDescriptor htd = admin.getDescriptor(tableName);

            // Check whether the column family is specified before modification.
            if (!htd.hasColumnFamily(familyName)) {
                // Create the column descriptor.
                TableDescriptor tableBuilder = TableDescriptorBuilder.newBuilder(htd)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(familyName).build())
                    .build();

                // Disable the table to get the table offline before modifying
                // the table.
                admin.disableTable(tableName);
                // Submit a modifyTable request.
                admin.modifyTable(tableBuilder);
                // Enable the table to get the table online after modifying the
                // table.
                admin.enableTable(tableName);
            }
            LOG.info("Modify table successfully.");
        } catch (IOException e) {
            LOG.error("Modify table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting testModifyTable.");
    }

    /**
     * Get Data
     */
    public void testGet() {
        LOG.info("Entering testGet.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifier = {Bytes.toBytes("name"), Bytes.toBytes("address")};
        // Specify RowKey.
        byte[] rowKey = Bytes.toBytes("012005000201");

        Table table = null;
        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Get get = new Get(rowKey);

            // Set the column family name and column name.
            get.addColumn(familyName, qualifier[0]);
            get.addColumn(familyName, qualifier[1]);

            // Submit a get request.
            Result result = table.get(get);

            // Print query results.
            for (Cell cell : result.rawCells()) {
                LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                    Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(CellUtil.cloneValue(cell)));
            }
            LOG.info("Get data successfully.");
        } catch (IOException e) {
            LOG.error("Get data failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testGet.");
    }

    /**
     * testScanData
     */
    public void testScanData() {
        LOG.info("Entering testScanData.");

        Table table = null;
        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;
        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the cache size.
            scan.setCaching(1000);

            // Submit a scan request.
            rScanner = table.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data successfully.");
        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testScanData.");
    }

    /**
     * testSingleColumnValueFilter
     */
    public void testSingleColumnValueFilter() {
        LOG.info("Entering testSingleColumnValueFilter.");

        Table table = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the filter criteria.
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("Xu Bing"));

            scan.setFilter(filter);

            // Submit a scan request.
            rScanner = table.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Single column value filter successfully.");
        } catch (IOException e) {
            LOG.error("Single column value filter failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testSingleColumnValueFilter.");
    }

    /**
     * testFilterList
     */
    public void testFilterList() {
        LOG.info("Entering testFilterList.");

        Table table = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Instantiate a FilterList object in which filters have "and"
            // relationship with each other.
            FilterList list = new FilterList(Operator.MUST_PASS_ALL);
            // Obtain data with age of greater than or equal to 20.
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(Long.valueOf(20))));
            // Obtain data with age of less than or equal to 29.
            list.addFilter(
                new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"), CompareOperator.LESS_OR_EQUAL,
                    Bytes.toBytes(Long.valueOf(29))));

            scan.setFilter(list);

            // Submit a scan request.
            rScanner = table.getScanner(scan);
            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Filter list successfully.");
        } catch (IOException e) {
            LOG.error("Filter list failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testFilterList.");
    }

    /**
     * deleting data
     */
    public void testDelete() {
        LOG.info("Entering testDelete.");

        byte[] rowKey = Bytes.toBytes("012005000201");

        Table table = null;
        try {
            // Instantiate an HTable object.
            table = conn.getTable(tableName);

            // Instantiate an Delete object.
            Delete delete = new Delete(rowKey);

            // Submit a delete request.
            table.delete(delete);

            LOG.info("Delete table successfully.");
        } catch (IOException e) {
            LOG.error("Delete table failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testDelete.");
    }


    /**
     * Delete user table
     */
    public void dropTable() {
        LOG.info("Entering dropTable.");

        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName)) {
                // Disable the table before deleting it.
                admin.disableTable(tableName);

                // Delete table.
                admin.deleteTable(tableName);
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropTable.");
    }

    /**
     * grantACL
     */
    public void grantACL() {
        LOG.info("Entering grantACL.");

        String user = "huawei";
        String permissions = "RW";

        String familyName = "info";
        String qualifierName = "name";

        Table mt = null;
        Admin hAdmin = null;
        try {
            // Create ACL Instance
            mt = conn.getTable(AccessControlLists.ACL_TABLE_NAME);

            Permission perm = new Permission(Bytes.toBytes(permissions));

            hAdmin = conn.getAdmin();
            TableDescriptor ht = hAdmin.getDescriptor(tableName);

            // Judge whether the table exists
            if (hAdmin.tableExists(mt.getName())) {
                // Judge whether ColumnFamily exists
                if (ht.hasColumnFamily(Bytes.toBytes(familyName))) {
                    // grant permission
                    AccessControlClient.grant(conn, tableName, user, Bytes.toBytes(familyName),
                        (qualifierName == null ? null : Bytes.toBytes(qualifierName)), perm.getActions());
                } else {
                    // grant permission
                    AccessControlClient.grant(conn, tableName, user, null, null, perm.getActions());
                }
            }
            LOG.info("Grant ACL successfully.");
        } catch (Throwable e) {
            LOG.error("Grant ACL failed ", e);
        } finally {
            if (mt != null) {
                try {
                    // Close
                    mt.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }

            if (hAdmin != null) {
                try {
                    // Close Admin Object
                    hAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting grantACL.");
    }
}
