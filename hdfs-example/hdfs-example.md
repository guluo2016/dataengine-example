# H3C DataEngine HDFS example使用步骤

### 非kerberos集群使用
1. 登录集群管理界面，下载HDFS配置文件： **core-site.xml** 和 **hdfs-site.xml** ，并将下载好的配置文件，放置在resources目录。注意需要提前按照hosts文件配置本地集群主机IP映射；
2. 进入normal目录，执行HdfsExample即可！
3. 如果集群开启权限，可能会由于权限问题无法操作，需要修改 **HADOOP_USER_NAME** 为有权限用户，在代码中修改 **HADOOP_USER_NAME** 变量即可，用户名可以从集群的用户管理中获取。

### 开启kerberos集群使用
1. 登录集群管理界面，下载HDFS配置文件： **core-site.xml** 和 **hdfs-site.xml** ，并将下载好的配置文件，放置在resources目录。注意需要提前按照hosts文件配置本地集群主机IP映射；
2. 集群管理用户管理页面，下载用户认证文件，放置在resources目录；
3. 进入kerberos目录，修改 **PRNCIPAL_NAME** 和 **PATH_TO_KEYTAB** 为具体使用用户名和keytab文件名
4. 执行HDFS操作样例程序