package com.h3c.spark.hbase.example.kerberos

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation

import java.io.File

class HBaseUtil {
  /**
   * 认证hbase
   * @return hbase的配置信息
   */
  def authenticate(): Configuration = {
    //连接HBase集群所需的认证信息，可以从集群管理平台中下载
    val krb5 = "krb5.conf"
    val keytab = "hbasetest.keytab"
    val principal = "hbasetest@WH5104.COM"

    val confPath = this.getClass.getClassLoader.getResource("conf").getPath
    print(confPath)

    val hbaseConf = HBaseConfiguration.create
    System.setProperty("java.security.krb5.conf", confPath + File.separator + krb5)
    hbaseConf.addResource(new Path(confPath + File.separator + "hbase-site.xml"))
    hbaseConf.addResource(new Path(confPath + File.separator + "hdfs-site.xml"))
    hbaseConf.addResource(new Path(confPath + File.separator + "core-site.xml"))
    hbaseConf.set("keytab.file", confPath + File.separator + keytab)

    UserGroupInformation.setConfiguration(hbaseConf)
    UserGroupInformation.loginUserFromKeytab(principal, confPath + File.separator + keytab)

    hbaseConf
  }

  /**
   * 连接Hbase集群
   * @param conf  连接HBase集群所需要的的配置
   * @return  HBase的连接对象
   */
  def createConnection(conf: Configuration): Connection = {
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }
}
