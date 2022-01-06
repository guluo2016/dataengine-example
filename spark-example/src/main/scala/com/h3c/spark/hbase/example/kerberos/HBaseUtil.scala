package com.h3c.spark.hbase.example.kerberos

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

import java.io.File

class HBaseUtil {

  var krb5Temp: String = _
  var keytabTemp: String = _
  var connection: Connection = _

  /**
   * 认证hbase
   * @return hbase的配置信息
   */
  def authenticate(): Configuration = {
    //连接HBase集群所需的认证信息，可以从集群管理平台中下载
    val krb5 = "krb5.conf"
    val keytab = "hbasetest.keytab"
    val principal = "hbasetest@WH5104.COM"

    krb5Temp = TempFileUtil.createTempFile(krb5);
    keytabTemp = TempFileUtil.createTempFile(keytab)

    if (krb5Temp == null || keytabTemp == null){
      println("构建krb5.conf/keytab文件失败，程序退出")
      System.exit(-1)
    }

    val hbaseConf = HBaseConfiguration.create
    System.setProperty("java.security.krb5.conf", krb5Temp)
    hbaseConf.addResource(this.getClass.getClassLoader.getResourceAsStream("conf" + File.separator + "hbase-site.xml"))
    hbaseConf.addResource(this.getClass.getClassLoader.getResourceAsStream("conf" + File.separator + "hdfs-site.xml"))
    hbaseConf.addResource(this.getClass.getClassLoader.getResourceAsStream("conf" + File.separator + "core-site.xml"))
    hbaseConf.set("keytab.file", keytabTemp)

    UserGroupInformation.setConfiguration(hbaseConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytabTemp)

    hbaseConf
  }

  /**
   * 连接Hbase集群
   * @param conf  连接HBase集群所需要的的配置
   * @return  HBase的连接对象
   */
  def createConnection(conf: Configuration): Connection = {
    connection = ConnectionFactory.createConnection(conf)
    connection
  }

  /**
   * 关闭连接，删除产生的临时文件
   */
  def close(): Unit = {
    if (connection != null) {
      connection.close
    }
    new File(krb5Temp).deleteOnExit
    new File(keytabTemp).deleteOnExit
  }
}
