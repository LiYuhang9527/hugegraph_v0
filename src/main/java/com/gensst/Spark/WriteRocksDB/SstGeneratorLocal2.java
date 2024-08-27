package com.gensst.Spark.WriteRocksDB;

import com.genfile.HBase.HugeGraph.HugeGraphSparkLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SstGeneratorLocal2 {
    static final Logger logger = LogManager.getLogger(SstGeneratorLocal2.class);
    private static final String cfdbPath = "./rocksdb-data-bulkload/";
    private static final String sstPath = "./tmp/db/sst_upload_04";

    // 定义 SST 文件路径列表
    static List<String> sstPaths = new ArrayList<>();
    // 定义一个新的列族名称
    private static final String columnFamilyName = "new_cf";

    public static void main(String[]  args) throws Exception {
        SstGeneratorLocal2 sstGenerator = new SstGeneratorLocal2(); //生成HFile
        sstGenerator.importIntoRocksDB(); //loade 到HBase
    }

    protected void importIntoRocksDB() throws Exception {
        JavaRDD<Row> rdd = readJsonTable();
        if (true) {
            sstToRocksDB(rdd);
        } else {
            putToRocksDB(rdd);
        }
    }

    private void putToRocksDB(JavaRDD<Row> rdd) {

    }

    private void sstToRocksDB(JavaRDD<Row> rdd) throws RocksDBException {

        JavaPairRDD<byte[], byte[]> javaPairRdd = rdd.mapToPair(
                (PairFunction<Row, byte[], byte[]>) row -> {
                    String key = (String) row.get(0);
                    String value = (String) row.get(1);

                    return new Tuple2<>(key.getBytes(), value.getBytes());
                });


        final EnvOptions envOptions = new EnvOptions();
        // 自定义的合并操作符，用于 RocksDB 列族
        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        Options options1 = new Options();
        RocksDB.loadLibrary();

        /****************************Gen SST**************************************/

        File sstFile = newSstFile( javaPairRdd);

        /****************************Ingesting SST files**************************************/
        final StringAppendOperator stringAppendOperator1 = new StringAppendOperator();
        IngestingSSTfiles(envOptions,options1,stringAppendOperator1);

        /****************************Read Data From the RocksDB********************************/
        scanRockDBTable("test","new-cf");
    }


    private static File newSstFile(JavaPairRDD<byte[], byte[]> javaPairRdd){
        //TODO: 重点是 K-V 有序

        javaPairRdd.foreachPartition(iterator -> {
            // 创建一个 ComparatorOptions 实例，并使用 BytewiseComparator 作为键的比较器。
            ComparatorOptions comparatorOptions = null;
            BytewiseComparator comparator = null;
            comparatorOptions = new ComparatorOptions().setUseDirectBuffer(false);
            comparator = new BytewiseComparator(comparatorOptions);

            final EnvOptions envOptions = new EnvOptions();
            // 自定义的合并操作符，用于 RocksDB 列族
            final StringAppendOperator stringAppendOperator = new StringAppendOperator();
            Options options = new Options();

            options = options
                    .setCreateIfMissing(true)
                    .setEnv(Env.getDefault())
                    .setComparator(comparator);

            int partitionIndex = TaskContext.getPartitionId();

            // 使用 SstFileWriter 生成 SST 文件
            SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);
            String sstPath = "./tmp/sst/rocksdb_sst_partition" + partitionIndex + "_" + System.currentTimeMillis() + ".sst";
            sstFileWriter.open(sstPath);
            try {
                while (iterator.hasNext()) {
                    Tuple2<byte[], byte[]> pair = iterator.next();
                    sstFileWriter.put(pair._1, pair._2);
                }
                sstFileWriter.finish();
                sstPaths.add(sstPath);
            } finally {
                stringAppendOperator.close();
                envOptions.close();
                options.close();
                if (sstFileWriter!= null) {
                    sstFileWriter.close();
                }
            }
        });


        File sstFile = new File(sstPath);
        return sstFile;
    }

    private  static void IngestingSSTfiles(EnvOptions envOptions,Options options,StringAppendOperator stringAppendOperator) throws RocksDBException {
        Options options2 = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMergeOperator(stringAppendOperator);


        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        // 创建一个 IngestExternalFileOptions 实例，用于配置 SST 文件的批量加载选项。
        IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
        // 创建一个新的 ColumnFamilyOptions 实例，并设置合并操作符。
        ColumnFamilyOptions cf_opts = new ColumnFamilyOptions().setMergeOperator(stringAppendOperator);


        // 创建一个列族描述符列表，包含默认列族和新定义的列族。
        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                new ColumnFamilyDescriptor(columnFamilyName.getBytes(), cf_opts)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        // 创建一个 DBOptions 实例，并设置数据库创建时缺失则创建，自动创建缺失的列族
        try (final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
            // 使用 RocksDB.open 方法打开数据库，传入数据库路径 cfdbPath、列族描述符列表和列族句柄列表。
            try (final RocksDB db = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles)) {
                ColumnFamilyHandle cf_handle = null;
                // 遍历列族句柄列表，找到与 columnFamilyName 匹配的列族句柄。
                for (ColumnFamilyHandle handle : cfHandles) {
                    if (new String(handle.getName()).equals(columnFamilyName)) {
                        cf_handle = handle;
                        break;
                    }
                }
                if (cf_handle != null) {
                    for (String path : sstPaths) {
                        db.ingestExternalFile(cf_handle, Collections.singletonList(path), ingestExternalFileOptions);
                    }
                }
            }

            System.out.println("Generate SST File");
        }
    }

    private static void scanRockDBTable(String tableName,String CfName){
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        RocksDB db = null;
        String cfName = columnFamilyName;
        // list of column family descriptors, first entry must always be default column family
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor(cfName.getBytes(), cfOpts)
        );

        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        try {
            db = RocksDB.open(dbOptions, cfdbPath, cfDescriptors, cfHandles);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }


        ColumnFamilyHandle cfHandle = cfHandles.stream().filter(x -> {
            try {
                return (new String(x.getName())).equals(cfName);
            } catch (RocksDBException e) {
                return false;
            }
        }).collect(Collectors.toList()).get(0);

        //Scan 动作
        RocksIterator iter = db.newIterator(cfHandle);
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            System.out.println("iterator key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        }
    }


    public JavaRDD<Row> readJsonTable() throws ClassNotFoundException {
        SparkConf conf = new SparkConf()
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                .set("spark.kryo.registrationRequired", "true")
                .setAppName("spark-gen-hfile")
                .setMaster("local[8]");

//        conf.registerKryoClasses(
//                new Class[]{
//                        org.rocksdb.Slice.class,
//                        org.rocksdb.EnvOptions.class,
//                        org.rocksdb.SstFileWriter.class,
//                        org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema.class,
//                        org.apache.spark.sql.types.StructType.class,
//                        org.apache.spark.sql.types.StructField[].class,
//                        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
//                        Class.forName("scala.reflect.ClassTag$$anon$1")});

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> rows = null;
        String path = "file:///home/lyh/Graph-Spark/data/test.csv";
        rows = spark.read().csv(path);
        JavaRDD<Row> javaRdd = rows.toJavaRDD();

        javaRdd = javaRdd.repartition(4);
        javaRdd.saveAsTextFile("./tmp/origin/");

        // 将 JavaRDD<Row> 转换为 JavaPairRDD<Integer, Row>
        JavaPairRDD<String, Row> pairedRdd = javaRdd.mapToPair(row -> {
            // 假设第一列是整数类型，可以作为排序的键
            String key = (String) row.get(0);
            return new Tuple2<>(key, row);
        });

        // 按键进行全局排序
        JavaPairRDD<String, Row> sortedRdd = pairedRdd.sortByKey();

        // 将排序后的 JavaPairRDD 转换回 JavaRDD<Row>
        JavaRDD<Row> sortedJavaRdd = sortedRdd.map(pair -> pair._2);

        // 保存或处理排序后的 RDD
        sortedJavaRdd.foreach(row -> System.out.println(row));

        sortedJavaRdd.saveAsTextFile("./tmp/sorted/");

        return sortedJavaRdd;
    }

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
}
