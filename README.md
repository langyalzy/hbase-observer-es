# hbase-observer-es

此工具是HBase数据自动同步到Elasticsearch索引库的数据同步插件，是基于HBase协处理器BaseRegionObserver自定义实现

1、程序打包
打包命令：
mvn DskipTests clean compile assembly:assembly

2、将jar包上传到hdfs指定位置
hdfs fs -put hbase-observer-es-20190926.jar

hdfs:///user/hbase/lib/

3、安装协处理器
如果是没有创建表，则需要提前创建表
例如：create 'region','data'

A、禁用表
disable 'region'

B、安装插件

alter 'region',METHOD=> 'table_att','coprocessor'=>'hdfs:///user/hbase/lib/hbase-observer-es-20190926.jar|com.langya.hbase.observer.util.HbaseDataSyncEsObserver|1001|indexName=indextest|indexType=typeTest'

参数说明：
1、jar包位置，一般是hdfs全路径
2、协处理器名称
3、1001固定参数（优先级）
4、协处理器参数，参数之间用逗号“，”分隔，参数与参数值用等号“=”连接，获取参数的方式在代码中有些，可以根据key获取到值
注意：
参数分为三个（可以通过修改源码拓展）：
1、indexName索引库名称，例如：
indexName=student
2、indexType索引类型，例如：
indexType=type
3、clientInfo es连接地址集合，注意连接地址之间用中杠“-”跟个，，ip和端口之间用冒号“：”分隔，例如：
esClientInfo=192.168.1.1:9300-192.168.1.2:9300

C、启用表
enable 'region'

到此已经完成插件安装，可以通过以下命令查看是否安装成功：
describe 'region'

后续可以往表中添加数据，例如：
put 'region','rowkey1234','date:title','aosidufwoeijfosdfj'
