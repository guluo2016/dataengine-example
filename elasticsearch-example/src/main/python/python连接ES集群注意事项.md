### 1 python连接未开启Kerberos、Xpack的ES集群

可以直接使用官方提供的elasticsearch python模块，使用该模块前，需要进行安装：

```shell
pip install elasticsearch
```

### 2 Python连接开启Kerberos的ES集群

官方提供的elasticsearch python模块，并不支持Kerberos。因此想要通过python连接ES集群，需要提前进行一些配置。

