package com.genfile.HBase.HugeGraph;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

// 这段 Java 代码是一个 Spark 应用程序，用于生成 HFiles 并将它们加载到 HBase 中。
public class HugeGraphSparkLoader implements Serializable {
    static final Logger logger = LogManager.getLogger(HugeGraphSparkLoader.class);

    public static final String COL_FAMILY = "pd";
    public static final String COLUMN = "bf";
    private static final String APP_NAME = "FisherCoder-HFile-Generator";

    public static void main(String[] args) throws Exception {
        HugeGraphSparkLoader hFileGenerator = new HugeGraphSparkLoader(); //生成HFile
        hFileGenerator.importIntoHBase(); //loade 到HBase
    }

    protected void importIntoHBase() throws Exception {
        JavaRDD<Row> rdd = readJsonTable();
        if (true) {
            hFilesToHBase(rdd);
        } else {
            putToHBase(rdd);
        }
    }

    /**
     * 核心步骤：
     * 1.读取 source 文件 序列化
     * 2.进行预分区
     * 3.生成HFile
     * 4.load 到HBase
     *
     * @param rdd
     * @throws Exception
     */
    protected void hFilesToHBase(JavaRDD<Row> rdd) throws Exception {
        // 原始的JavaRDD<Row>通过mapToPair方法转换为JavaPairRDD<ImmutableBytesWritable, KeyValue>。
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {
                    // 通过call方法获取Row中的键和值，并将它们封装成ImmutableBytesWritable和KeyValue对象。
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        String key = (String) row.get(0);
                        String value = (String) row.get(1);
                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
                        KeyValue keyValue = null;

                        return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, keyValue);
                    }
                });

        Configuration baseConf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        // 创建与HBase的连接
        Connection conn = ConnectionFactory.createConnection(conf);
        // 获取HBase表对象，这里表名为test_salt1
        Table table = conn.getTable(TableName.valueOf("test_salt1"));

        // 创建一个Job对象，用于配置和执行Hadoop任务
        Job job = new Job(baseConf, APP_NAME);

        // 定义一个分区器，这里预分区数为5
        Partitioner partitioner = new HugeGraphSparkLoader.IntPartitioner(5);//预分区处理

        // 将RDD重新分区并确保每个分区内的数据有序
        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd =
                javaPairRdd.repartitionAndSortWithinPartitions(partitioner);//精髓 partition内部做到有序


        // 配置增量加载的作业
        HFileOutputFormat2.configureIncrementalLoadMap(job, table.getDescriptor());

        System.out.println("Done configuring incremental load....");

        // 获取作业的配置，并获取文件系统对象。
        Configuration config = job.getConfiguration();
        FileSystem fs = FileSystem.get(config);
        // 定义HFile的存储路径，路径中包含时间戳以确保唯一性
        String path = fs.getWorkingDirectory().toString() + "/hfile-gen" + "/" + System.currentTimeMillis();//HFile 存储路径

        // 将RDD数据保存为HFile。这里指定了输出路径、键和值的类类型、输出格式类。
        repartitionedRdd.saveAsNewAPIHadoopFile(
                path,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                config
        );
        System.out.println("Saved to HFiles to: " + path);

        // 使用FsShell更改生成的HFile的权限，确保HBase可以读取这些文件。
        if (true) {
            FsShell shell = new FsShell(conf);
            try {
                shell.run(new String[]{"-chmod", "-R", "777", path});
            } catch (Exception e) {
                System.out.println("Couldnt change the file permissions " + e
                        + " Please run command:"
                        + "hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles "
                        + path + " '"
                        + "test" + "'\n"
                        + " to load generated HFiles into HBase table");
            }
            System.out.println("Permissions changed.......");
            // 将生成的HFile加载到HBase表中
            loadHfiles(path);
            System.out.println("HFiles are loaded into HBase tables....");
        }
    }

    protected void loadHfiles(String path) throws Exception {
        Configuration baseConf = HBaseConfiguration.create();
        baseConf.set("hbase.zookeeper.quorum", "localhost");
        baseConf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(baseConf);
        Table table = conn.getTable(TableName.valueOf("test_salt1"));
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(baseConf);

        //TODO: load HFile to HBase
        loader.run(new String[]{path, String.valueOf(table.getName())});

    }

    // 定义了一个自定义分区器 IntPartitioner，它扩展了 Spark 的 Partitioner 类，用于控制数据在不同分区的分布。
    //精髓 repartitionAndSort
    public class IntPartitioner extends Partitioner {
        private final int numPartitions;
        public Map<List<String>, Integer> rangeMap = new HashMap<>();

        public IntPartitioner(int numPartitions) throws IOException {
            this.numPartitions = numPartitions;
            this.rangeMap = getRangeMap();
        }

        private Map<List<String>, Integer> getRangeMap() throws IOException {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            HRegionLocator locator = (HRegionLocator) admin.getConnection().getRegionLocator(TableName.valueOf("test_salt1"));

            Pair<byte[][], byte[][]> startEndKeys = locator.getStartEndKeys();

            Map<List<String>, Integer> rangeMap = new HashMap<>();
            for (int i = 0; i < startEndKeys.getFirst().length; i++) {
                String startKey = Bytes.toString(startEndKeys.getFirst()[i]);
                String endKey = Bytes.toString(startEndKeys.getSecond()[i]);
                System.out.println("startKey = " + startKey
                        + "\tendKey = " + endKey + "\ti = " + i);
                rangeMap.put(new ArrayList<>(Arrays.asList(startKey, endKey)), i);
            }
            return rangeMap;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            if (key instanceof ImmutableBytesWritable) {
                try {
                    ImmutableBytesWritable immutableBytesWritableKey = (ImmutableBytesWritable) key;
                    if (rangeMap == null || rangeMap.isEmpty()) {
                        rangeMap = getRangeMap();
                    }

                    String keyString = Bytes.toString(immutableBytesWritableKey.get());
                    for (List<String> range : rangeMap.keySet()) {
                        if (keyString.compareToIgnoreCase(range.get(0)) >= 0
                                && ((keyString.compareToIgnoreCase(range.get(1)) < 0)
                                || range.get(1).equals(""))) {
                            return rangeMap.get(range);
                        }
                    }
                    logger.error("Didn't find proper key in rangeMap, so returning 0 ...");
                    return 0;
                } catch (Exception e) {
                    logger.error("When trying to get partitionID, "
                            + "encountered exception: " + e + "\t key = " + key);
                    return 0;
                }
            } else {
                logger.error("key is NOT ImmutableBytesWritable type ...");
                return 0;
            }
        }
    }

    // readJsonTable 方法用于从 CSV 文件读取数据，并返回一个 JavaRDD<Row>
    // readJsonTable 函数在提供的代码中应该是用于从 JSON 文件中读取数据，并返回一个 JavaRDD<Row> 对象。
    // 然而，代码中的函数实现似乎与函数名描述的功能不符，它实际上是从 CSV 文件读取数据，而不是 JSON
    public JavaRDD<Row> readJsonTable() throws ClassNotFoundException {
        // 创建一个 SparkConf 对象，用于配置 Spark 应用程序的各种设置。
        // 设置使用 Kryo 序列化器，这是一种高效的序列化框架。
        // 指定应用程序名称为 spark-gen-hfile。
        // 设置 Spark 主节点为本地模式，local[*] 表示使用所有可用的本地核心。
        SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "true")
                .setAppName("spark-gen-hfile")
                .setMaster("local[*]");


        // 使用 registerKryoClasses 方法注册需要 Kryo 序列化器序列化的自定义类。这包括 Hadoop 和 Spark 的内部类。
        conf.registerKryoClasses(
                new Class[]{org.apache.hadoop.hbase.io.ImmutableBytesWritable.class,
                        org.apache.hadoop.hbase.KeyValue.class,
                        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
                        Class.forName("scala.reflect.ClassTag$$anon$1")});

        // 使用 SparkSession.builder 创建一个 SparkSession 对象，这是 Spark 应用程序的入口点
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        // 使用 spark.read().csv(path) 读取 CSV 文件，并将结果存储在 rows 变量中。
        Dataset<Row> rows = null;
        String path = "file:///home/lyh/Graph-Spark/data/test.csv";
        rows = spark.read().csv(path);
        return rows.toJavaRDD();
    }

    // putToHBase 方法提供了另一种将数据直接写入 HBase 的方式，而不是生成 HFiles。
    protected void putToHBase(JavaRDD<Row> rdd) throws IOException {
        // 定义了一个 Function2 类型的匿名内部类 writeToHBaseFunc，
        // 它接收一个分片索引 shardIndex 和一个 Row 对象的迭代器 iterator。
        Function2 writeToHBaseFunc = new Function2<Integer, Iterator<Row>, Iterator<Row>>() {
            // call 方法是 Function2 接口的核心方法，它将每个分区的数据（由 iterator 提供）写入 HBase，并返回一个空的迭代器。
            @Override
            public Iterator<Row> call(Integer shardIndex, Iterator<Row> iterator)
                    throws Exception {
                // 通过调用put(shardIndex, iterator)将迭代器中的每个Row对象写入HBase。
                put(shardIndex, iterator);
                return Collections.emptyIterator();
            }
        };

        // 使用 Files.createTempDirectory 创建一个临时目录，用于存储中间数据。
        // toAbsolutePath 方法获取临时目录的绝对路径。
        Path tempOutputPath = Files.createTempDirectory("fishercoder")
                .toAbsolutePath();
        // 确保如果临时目录已经存在，它会被删除
        Files.deleteIfExists(tempOutputPath);

        try {
            // repartition(1) 方法将 RDD 重新分区为一个分区。
            // 这通常用于确保所有数据都在单个分区中处理，简化并行写入操作。
            rdd.repartition(1)//repartion  作用
                    // mapPartitionsWithIndex 方法应用 writeToHBaseFunc 函数到每个分区的数据上。
                    // 这个方法允许开发者对每个分区的数据执行自定义的并行操作。
                    .mapPartitionsWithIndex(writeToHBaseFunc, true)
                    // saveAsTextFile 方法将处理后的数据保存为文本文件
                    .saveAsTextFile(tempOutputPath.toString());
        } finally {
            FileUtils.deleteDirectory(new File(tempOutputPath.toString()));
        }
    }

    // 通过调用put(shardIndex, iterator)将迭代器中的每个Row对象写入HBase。
    protected void put(Integer shardIndex, Iterator<Row> iterator) throws IOException {
    }
}
