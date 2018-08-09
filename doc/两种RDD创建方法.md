# [Spark core基础 -- 基本架构和RDD](https://www.cnblogs.com/kinghey-java-ljx/p/8516668.html)

----摘自网络

https://www.cnblogs.com/kinghey-java-ljx/p/8516668.html

------

Spark运行架构：

 Spark运行架构包括集群资源管理器（Cluster Manager）、运行作业任务的工作节点（Worker Node）、每个应用的任务控制节点（Driver）和每个工作节点上负责具体任务的执行进程（Executor）

与Hadoop MapReduce计算框架相比，Spark所采用的Executor有两个优点：

一是利用多线程来执行具体的任务（Hadoop MapReduce采用的是进程模型），减少任务的启动开销；

二是Executor中有一个BlockManager存储模块，会将内存和磁盘共同作为存储设备，当需要多轮迭代计算时，可以将中间结果存储到这个存储模块里，下次需要时，就可以直接读该存储模块里的数据，而不需要读写到HDFS等文件系统里，因而有效减少了IO开销；或者在交互式查询场景下，预先将表缓存到该存储系统上，从而可以提高读写IO性能。

 

在Spark中，一个应用（Application）由一个任务控制节点（Driver）和若干个作业（Job）构成，一个作业由多个阶段（Stage）构成，一个阶段由多个任务（Task）组成。当执行一个应用时，任务控制节点会向集群管理器（Cluster Manager）申请资源，启动Executor，并向Executor发送应用程序代码和文件，然后在Executor上执行任务，运行结束后，执行结果会返回给任务控制节点，或者写到HDFS或者其他数据库中。

Spark的基本运行流程如下：

（1）当一个Spark应用被提交时，首先需要为这个应用构建起基本的运行环境，即由任务控制节点（Driver）创建一个SparkContext，由SparkContext负责和资源管理器（Cluster Manager）的通信以及进行资源的申请、任务的分配和监控等。SparkContext会向资源管理器注册并申请运行Executor的资源；

（2）资源管理器为Executor分配资源，并启动Executor进程，Executor运行情况将随着“心跳”发送到资源管理器上；

（3）SparkContext根据RDD的依赖关系构建DAG图，DAG图提交给DAG调度器（DAGScheduler）进行解析，将DAG图分解成多个“阶段”（每个阶段都是一个任务集），并且计算出各个阶段之间的依赖关系，然后把一个个“任务集”提交给底层的任务调度器（TaskScheduler）进行处理；Executor向SparkContext申请任务，任务调度器将任务分发给Executor运行，同时，SparkContext将应用程序代码发放给Executor；

（4）任务在Executor上运行，把执行结果反馈给任务调度器，然后反馈给DAG调度器，运行完毕后写入数据并释放所有资源。



Spark运行架构具有以下特点：

（1）每个应用都有自己专属的Executor进程，并且该进程在应用运行期间一直驻留。Executor进程以多线程的方式运行任务，减少了多进程任务频繁的启动开销，使得任务执行变得非常高效和可靠；

（2）Spark运行过程与资源管理器无关，只要能够获取Executor进程并保持通信即可；

（3）Executor上有一个BlockManager存储模块，类似于键值存储系统（把内存和磁盘共同作为存储设备），在处理迭代计算任务时，不需要把中间结果写入到HDFS等文件系统，而是直接放在这个存储系统上，后续有需要时就可以直接读取；在交互式查询场景下，也可以把表提前缓存到这个存储系统上，提高读写IO性能；

（4）任务采用了数据本地性和推测执行等优化机制。数据本地性是尽量将计算移到数据所在的节点上进行，即“计算向数据靠拢”，因为移动计算比移动数据所占的网络资源要少得多。而且，Spark采用了延时调度机制，可以在更大的程度上实现执行过程优化。比如，拥有数据的节点当前正被其他的任务占用，那么，在这种情况下是否需要将数据移动到其他的空闲节点呢？

答案是不一定。因为，如果经过预测发现当前节点结束当前任务的时间要比移动数据的时间还要少，那么，调度就会等待，直到当前节点可用。

 

 

Spark部署模式：

三种模式：

standalone，Spark on Mesos，Spark on YARN

S：不需要依赖其他系统

M：资源调度管理框架，Spark充分支持，官方推荐

Y：与Hadoop统一部署，资源管理和调度依赖YARN，分布式存储依赖HDFS

Spark核心 -- RDD

 

RDD设计背景：

提供一个抽象数据架构，避免中间结果存储

 

RDD概念：

分布式对象集合，本质上是只读的分区记录集合

RDD操作：行动（返回非RDD），转换（返回RDD）

 

典型执行过程：

1.RDD读入外部数据

2.进行一系列“转换”操作，产生不同的RDD

3.最后一个RDD经过“行动”，输出到外部数据源

 

“行动”才会真正发生计算，而“转换”是记录相互之间的依赖关系

当“行动”要进行输出时，Spark根据RDD的依赖关系生成DAG，从起点开始计算

 

建立的“转换”然后生成DAG图称为一个“血缘关系”

血缘关系连接起来的RDD操作实现管道化，这就保证了中途不需要保存数据，而是直接管道式流入下一步

 

具体例子

------

val sc= new SparkContext(“spark://localhost:7077”,”Hello World”, “YOUR_SPARK_HOME”,”YOUR_APP_JAR”)

// 创建SC对象

val fileRDD = sc.textFile(“hdfs://192.168.0.103:9000/examplefile”)

// 从HDFS读取数据创建一个RDD

val filterRDD = fileRDD.filter(_.contains(“Hello World”))

// “转换”得到一个新的RDD

filterRDD.cache()

// 采用cache接口把RDD保存在内存中，进行持久化

filterRDD.count()

// “行动”，计算包含元素个数

 

以上执行流程：

1.创建sc对象

2.从外部数据源（HDFS）读取并创建fileRDD对象

3.构建fileRDD和filterRDD的依赖关系，形成DAG图（转换轨迹）

4.触发计算，把结果持久化到内存

 

------

RDD特性：

1.高效的容错性，只需要记录粗粒度的转换，不需细粒度的日志

2.中间结果持久化到内存

3.可以存放Java对象

------

 

RDD依赖关系：

分为窄依赖（一父对应一子分区，多子可对应一父）与宽依赖（一父对应多子）

窄依赖对应协同划分（key落在同一子分区），宽依赖对应非协同划分

 

这种依赖设计具有容错性，加快了执行速度

窄依赖的恢复更高效，Spark还有数据检查点和记录日志，在恢复时不需从头开始

------

 

阶段划分：

在DAG反向解析，遇到窄依赖才把当前RDD加入当前阶段

DAG划分为多个阶段，每个阶段时任务集合，任务调度器把任务集合分配到executor

两种RDD创建方法：

1.通过外部数据集。本地、HDFS文件系统、HBase等外部数据源

2.调用SparkContext的parallelize方法，在Driver的存在集合上创建

 

准备：

打开hadoop的hdfs和spark

./sbin/start-dfs.sh

./bin/spark-shell

 

1.textFile()方法：

把文件的URL作为参数，也就是地址

Val lines = sc.textFile(…)

生成的是一个String类型RDD，也就是RDD[String]

输入参数可以是文件名、目录和压缩文件

可以输入第二个参数来指定分区数，默认为每个block创建一个分区

 

2.通过并行集合创建RDD

parallelize方法

读入数组／整数array，得到RDD[Int]

 

RDD操作具体解释：

转换、行动

 

转换：惰性求值，只记录转换轨迹

\* filter(func)：筛选出满足函数func的元素，并返回一个新的数据集

\* map(func)：将每个元素传递到函数func中，并将结果返回为一个新的数据集

\* flatMap(func)：与map()相似，但每个输入元素都可以映射到0或多个输出结果

\* groupByKey()：应用于(K,V)键值对的数据集时，返回一个新的(K, Iterable)形式的数据集

\* reduceByKey(func)：应用于(K,V)键值对的数据集时，返回一个新的(K, V)形式的数据集，其中的每个值是将每个key传递到函数func中进行聚合

 

行动：真正的计算

\* count() 返回数据集中的元素个数

\* collect() 以数组的形式返回数据集中的所有元素

\* first() 返回数据集中的第一个元素

\* take(n) 以数组的形式返回数据集中的前n个元素

\* reduce(func) 通过函数func（输入两个参数并返回一个值）聚合数据集中的元素

\* foreach(func) 将数据集中的每个元素传递到函数func中运行

 

惰性机制：通过map切分，接着才reduce处理

 

Filter 操作

RDD.filter()会遍历RDD中每行文本，并执行括号内的匿名函数

RDD.filter().count()

括号内可填入line => line.contains(“”) 这种Lamda表达式

执行表达式前，把当前行赋值给line，再执行后面的逻辑，然后放入结果集

等所有行执行完，得到结果集，最后才执行count行动操作

 

map与reduce操作

RDD.map()把每行都传递到括号内函数

Lines.map(line => line.split(“ “).size) 对每一行line先进行切分，得到集合RDD[Array[String]]，再求出集合中的个数，所以此步转换成Int类型的RDD[Int]

 

Reduce.((a, b) => if (a > b) a else b)

计算出RDD[Int]中的最大值，每次比较中保留较大的数，并用到下一次的比较中，也就是后续每次比较只取一个新的数

 

持久化

由于spark的惰性，每次行动都会从头开始算，所以避免计算重复，引入持久化（缓存）

persist() 方法可用来标记RDD持久化，到第一次行动时，就会使标记的RDD真正持久化，持久化后可重复使用

用cache()方法会调用persist(MEMORY_ONLY)，只使用内存。而persist(MEMORY_AND_DISK)还会在内存不足时放在硬盘上

 

分区

RDD分区原则：分区的个数尽量等于集群中的CPU核心（core）数目

用spark.default.parallelism 这个参数来配置默认分区数目

从HDFS读取文件，分区数为文件分片数

 

打印

rdd.foreach(println)或者rdd.map(println)

在集群中操作时，用collect()方法，把所有worker节点的RDD都抓到Driver Program中才能把所有元素打印出，但可能会导致内存溢出

可用take()方法打印部分元素

 

 

 

常用的键值对RDD转换操作：

包括reduceByKey()、groupByKey()、sortByKey()、join()、cogroup()等

 

reduceByKey(func)

功能：使用func函数合并具有相同键的值

reduceByKey((a, b) => a + b) 就是把相同键的值都加起来，所以a，b都代表值

 

groupByKey()

功能：对具有相同键的值进行分组，若有(hey, 1)，(hey, 2) 则会生成(hey, (1, 2))

 

Keys/values

功能：返回键值对的键/值

 

sortedByKey()

功能：返回根据键排序的RDD

 

mapValues(func)

功能：只对value进行函数处理

 

join

就是连接，包括内链接(join)、左外连接(leftOuterJoin)、右外连接(rightOuterJoin)等

把两个pairRDD的相同键对应的值，连接在一起

比如第一个RDD有(“spark”, 1)、(“spark”, 2)，第二个RDD有(“spark”, “fast”)，那么join的结果是(spark, (1, fast))、(spark, (2, fast))

 

实例：

rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1 / x._2)).collect()

求相同键对应值的平均数

1.第一步建立（x, 1），记录键值对个数

2.把值和个数分别相加

3.把总值和总个数相除

 

 

 

Spark中的共享变量

 

Spark中的另一个抽象：共享变量

满足多个任务之间、人物与控制节点之间共享变量

两种类型：广播变量（broadcast）、累加器（accumulators）

 

广播变量：

在每台机器上缓存一个只读变量

val broadcastV = SparkContext.broadcast(v)

从一个普通变量v来创建一个广播变量，在集群中可免去重复分发v，一旦创建后，v的值不能修改

 

累加器：

用来实现计数器（counter）和求和（sum）

用SparkContext.longAccumulator(名称)或SparkContext.doubleAccumulator()创建

用add方法把数值累加到累加器中，然后要通过控制节点使用value方法读取

val accu = sc.longAccumulator

sc.parallelize(Array).foreach(x => accum.add(x))

accum.value

 

 

Spark SQL简介 -- 核心为DataFrame

 

Spark SQL优化了Shark（Hive on Spark）

仅依赖了HiveQL解析和Hive元数据

由Catalyst（函数式关系查询优化框架）负责执行计划

增加了SchemaRDD，后来变成了DataFrame

Spark SQL支持的数据格式和编程语言（上面为语言 下面为数据格式）：



Spark能实现从MySQL到DataFrame的转化，支持SQL查询

 

DataFrame与RDD的区别：

RDD是分布式的JAVA对象的集合，对具体对象的结构不了解

DataFrame以RDD为基础，是分布式的Row对象集合，提供了详细的schema（结构信息），清楚每个列的信息

DataFrame也和RDD一样有“惰性”机制，生成DAG图到最后才计算

 

 

得到DataFrame的两种方法：

直接创建：

在json文件中读取数据并创建DataFrame（记得先开了Hadoop再开Spark-shell！！）

1.导入org.apache.spark.sql.SparkSession

2.创建对象，builder()、getOrCreate()

3.使支持RDD转换为DataFrames及后续sql操作 import spark.implicits._

4.读取文件 read，得到DataFrames

5.各种常用操作 show、printSchema（打印模式）、select（选择某些列）、filter（过滤）、groupBy（分组）、sort（排序 desc倒序）、select as（重命名）后面几个操作都要接show()

 

RDD转换：

两种方法：1.利用反射推断RDD的schema 2.使用编程接口构造一个schema

 

一、利用反射推断RDD，定义case class，隐式转成DataFrame：

1.导包：org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

​              org.apache.spark.sql.sql.Encoder

​             spark.implicits._ 支持RDD隐式转成DataFrame

2.创建case class

3.sc对象，textFile()导入文件，map()转换文件并定义DF，toDF()创建了DataFrame

4.注册临时表，DF.createOrReplaceTempView(“”)

5.用spark.sql()引入sql语句可从临时表生成另一DataFrame，最终通过Show()显示

 

二、无法提前定义case class时，采用编程方式定义RDD模式：

1.导包：org.apache.spark.sql.types._

​              org.apache.spark.sql.Row

2.导入文件生成RDD（sc的textFile）

3.定义模式Schema字符串

4.根据字符串生成模式：split()、map()方法生成StructField，然后StructType()转化成schema

5.生成rowRDD，然后通过rowRDD和schema用createDataFrame()方法建立关系，并生成DataFrame