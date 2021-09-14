package com.h3c.hbase.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * 功能描述
 * hbase-example test main class
 *
 * @since 2013
 */

public class TestMain {
    private static final Logger LOG = LoggerFactory.getLogger(TestMain.class.getName());
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static Configuration conf = null;
    // 认证信息，具体使用时修改为对应集群的用户名和keytab即可
    private static String principal = "hadoop";
    private static String keytabName = "hadoop.keytab";
    private static String krb5Name = "krb5.conf";
    public static void main(String[] args) {
        try {
            init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        // test hbase
        HBaseSample oneSample;
        try {
            oneSample = new HBaseSample(conf);
            oneSample.test();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase -------------------");
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userName = principal;
            //In Windows environment
            String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

            String userKeytabFile = userdir + keytabName;
            String krb5File = userdir + krb5Name;

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        //In Windows environment
        String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
    }
}