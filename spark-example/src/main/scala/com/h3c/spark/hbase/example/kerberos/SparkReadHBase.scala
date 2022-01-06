package com.h3c.spark.hbase.example.kerberos

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.protobuf.ProtobufUtil

import java.util.Base64

object SparkReadHBase {
  def main(args: Array[String]): Unit = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    /**
     * 假定HBase集群中有一个名为simple_data的表,simple_data中有一个列族info
     * Spark从该表中读取数据
     */
    val tableName = "simple_data"
    val hbaseUtil = new HBaseUtil
    val hbaseConf = hbaseUtil.authenticate()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan
    scan.addFamily(Bytes.toBytes("info"))
    hbaseConf.set(TableInputFormat.SCAN, Base64.getEncoder.encodeToString(ProtobufUtil.toScan(scan).toByteArray))

    val sparkConf = new SparkConf().setAppName("Spark Read From HBase")
    val sc = new SparkContext(sparkConf)

    val dataRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    dataRdd.collect.foreach( x => {
      val it = x._2.listCells.iterator
      while (it.hasNext) {
        val cell = it.next
        println("Family : " + Bytes.toString(CellUtil.cloneFamily(cell)) +
          " Qualifier : " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
          " Value : " + Bytes.toString(CellUtil.cloneValue(cell)))
      }
    })
    sc.stop()
    hbaseUtil.close
  }
}
