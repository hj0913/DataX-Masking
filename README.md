# DataX-Masking

DataX-Masking 是在 [DataX 3.0](https://github.com/alibaba/DataX/) 基础上二次开发得到的**大数据脱敏平台**，可以快速地在数据传输过程中对指定的单个或多个字段用可选的脱敏方法进行处理。

一般主要运行在linux系统。

# Features

DataX本身作为数据同步框架，将不同数据源的同步抽象为从源头数据源读取数据的Reader插件，以及向目标端写入数据的Writer插件，理论上DataX框架可以支持任意数据源类型的数据同步工作。同时DataX插件体系作为一套生态系统, 每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

DataX-Masking 通过扩展DataX的transformer中间件，集成了多种脱敏算法。

#### DataX使用手册：[DataX-Introduction](/docs/datax-masking-user-guide.md)

## 支持的脱敏方法

平台中的脱敏方法可以分为两类，一种是常用的脱敏方法，这种方法计算开销比较小；另一种是加密方法，这种方法计算开销较大，一般而言用时较久。

|脱敏方法名称|描述|示例|
|---|---|---|
|Hiding|将数据置为常量，一般用于处理不需要的敏感字段。|500 ->0<br>false->true|
|Floor|对整数或浮点数或者日期向下取整。|-12.68->-12<br>12580->12000<br>2018-05-10 10:17->2018-05-01 6:00|
|Enumerate|将数字映射为新值，同时保持数据的大小顺序。|500->1500 600->1860 700->2000|
|Prefix Preserve|保持前n位不变，混淆其余部分。可针对字母和数字字符在同为字母或数字范围内进行混淆，特殊符号将保留。|10.199.90.105->10.199.38.154<br>18965432100->18985214789|
|MD5|不可逆的hash摘要方法。将不定长的数据映射成定长的数据(长度为32的字符串)。|你好世界！->4f025928d787aa7b73beb58c1a85b11d|
|EDP|Epsilon Differential Privacy | 17.5 -> 17.962 |
|AES|AES-128-CBC 对称加密|你好世界！-> 12da3fedd5f0992447b1c7b4af0d7133|
| FPE | format Preserving Encryption | abcdefg -> iskejtl |


## Support Data Channels 

DataX目前已经有了比较全面的插件体系，主流的RDBMS数据库、NOSQL、大数据计算系统都已经接入，目前支持数据如下图，详情请点击：[DataX数据源参考指南](https://github.com/alibaba/DataX/wiki/DataX-all-data-channels)

| 类型           | 数据源        | Reader(读) | Writer(写) |文档|
| ------------ | ---------- | :-------: | :-------: |:-------: |
| RDBMS 关系型数据库 | MySQL      |     √     |     √     |[读](/mysqlreader/doc/mysqlreader.md) 、[写](/mysqlwriter/doc/mysqlwriter.md)|
|              | Oracle     |     √     |     √     |[读](/oraclereader/doc/oraclereader.md) 、[写](/oraclewriter/doc/oraclewriter.md)|
|              | SQLServer  |     √     |     √     |[读](/sqlserverreader/doc/sqlserverreader.md) 、[写](/sqlserverwriter/doc/sqlserverwriter.md)|
|              | PostgreSQL |     √     |     √     |[读](/postgresqlreader/doc/postgresqlreader.md) 、[写](/postgresqlwriter/doc/postgresqlwriter.md)|
|              | DRDS |     √     |     √     |[读](/drdsreader/doc/drdsreader.md) 、[写](/drdswriter/doc/drdswriter.md)|
|              | 达梦         |     √     |     √     |[读]() 、[写]()|
|              | 通用RDBMS(支持所有关系型数据库)         |     √     |     √     |[读]() 、[写]()|
| 阿里云数仓数据存储    | ODPS       |     √     |     √     |[读](/odpsreader/doc/odpsreader.md) 、[写](/odpswriter/doc/odpswriter.md)|
|              | ADS        |           |     √     |[写](/adswriter/doc/adswriter.md)|
|              | OSS        |     √     |     √     |[读](/ossreader/doc/ossreader.md) 、[写](/osswriter/doc/osswriter.md)|
|              | OCS        |     √     |     √     |[读](/ocsreader/doc/ocsreader.md) 、[写](/ocswriter/doc/ocswriter.md)|
| NoSQL数据存储    | OTS        |     √     |     √     |[读](/otsreader/doc/otsreader.md) 、[写](/otswriter/doc/otswriter.md)|
|              | Hbase0.94  |     √     |     √     |[读](/hbase094xreader/doc/hbase094xreader.md) 、[写](/hbase094xwriter/doc/hbase094xwriter.md)|
|              | Hbase1.1   |     √     |     √     |[读](/hbase11xreader/doc/hbase11xreader.md) 、[写](/hbase11xwriter/doc/hbase11xwriter.md)|
|              | MongoDB    |     √     |     √     |[读](/mongoreader/doc/mongoreader.md) 、[写](/mongowriter/doc/mongowriter.md)|
|              | Hive       |     √     |     √     |[读](/hdfsreader/doc/hdfsreader.md) 、[写](/hdfswriter/doc/hdfswriter.md)|
| 无结构化数据存储     | TxtFile    |     √     |     √     |[读](/txtfilereader/doc/txtfilereader.md) 、[写](/txtfilewriter/doc/txtfilewriter.md)|
|              | FTP        |     √     |     √     |[读](/ftpreader/doc/ftpreader.md) 、[写](/ftpwriter/doc/ftpwriter.md)|
|              | HDFS       |     √     |     √     |[读](/hdfsreader/doc/hdfsreader.md) 、[写](/hdfswriter/doc/hdfswriter.md)|
|              | Elasticsearch       |         |     √     |[写](/elasticsearchwriter/doc/elasticsearchwriter.md)|
| 流数据        | [Apache Kafka](/docs/userGuideKafka.md)       |   √     |    √      |[读](/streamkafkareader/doc/streamkafkareader.md)、[写](/streamkafkawriter/doc/streamkafkawriter.md)|


## DataX-Masking Contributor
* Liu Kun
* Liu Wenyan
* Wang Hao
* Liu Jiaye

## 我要开发新的插件
请点击：[DataX transformer插件开发](https://blog.csdn.net/landstream/article/details/79933800)

## 当前版本 0.3 Beta

### Notice!
* 当前版本仅在开发环境下进行过测试
* 当前版本每次仅可使用一种密码学方法(RSA,AES,FPE)

### Future
* 提供可对中文字符进行加密的保型加密
* 提供动态加载自定义transformer的功能

## License

This software is free to use under the Apache License [Apache license](/license.txt).
