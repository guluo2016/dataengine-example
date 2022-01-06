package com.h3c.spark.hbase.example.kerberos

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHBase {
  def main(args: Array[String]): Unit = {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    /**
     * 假定HBase集群中有一个名为simple_data的表，simple_data中有一个列族info,列为name
     * Spark向该表中写入数据
     */
    val tableName = "simple_data"
    val family = "info"
    val qualifier = "name"

    val sparkConf = new SparkConf().setAppName("Spark Write HBase")
    val sc = new SparkContext(sparkConf)
    //构造要往HBase中写的数据
    val dataRdd = sc.makeRDD(Array("100040:hanmeimei", "10050:lilei"))

    //htable等信息需要配置到闭包内，否则会出现Task not serializable错误
    dataRdd.foreachPartition(data => {
      val hbaseUtil = new HBaseUtil
      val hbaseConf = hbaseUtil.authenticate()
      val connection = hbaseUtil.createConnection(hbaseConf)
      val htable = connection.getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]

      data.foreach(ele => {
        val rowkey = ele.split(":")(0)
        val value = ele.split(":")(1)
        val put = new Put(Bytes.toBytes(rowkey))
          .addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
        htable.put(put)
      })
      hbaseUtil.close
    })
    sc.stop()
  }
}
