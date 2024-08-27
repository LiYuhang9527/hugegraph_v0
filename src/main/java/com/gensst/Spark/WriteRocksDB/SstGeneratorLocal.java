package com.gensst.Spark.WriteRocksDB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HRegionLocator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
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
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SstGeneratorLocal {
    private static final String cfdbPath = "./rocksdb-data-bulkload/";
    private static final String sstPath = "./tmp/db/sst_upload_04";
    // 定义一个新的列族名称
    private static final String columnFamilyName = "new_cf";

    public static void main(String[]  args) throws Exception {
        SstGeneratorLocal sstGenerator = new SstGeneratorLocal(); //生成HFile
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

    private void sstToRocksDB(JavaRDD<Row> rdd) throws IOException, RocksDBException {

        JavaPairRDD<Slice, Slice> javaPairRdd = rdd.mapToPair(
                (PairFunction<Row, Slice, Slice>) row -> {
                    String key = (String) row.get(0);
                    String value = (String) row.get(1);

                    Slice keySlice = new Slice(key);
                    Slice valueSlice = new Slice(value);

                    return new Tuple2<>(keySlice, valueSlice);
                });

        final EnvOptions envOptions = new EnvOptions();
        // 自定义的合并操作符，用于 RocksDB 列族
        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        Options options1 = new Options();
        RocksDB.loadLibrary();

        /****************************Gen SST**************************************/

        File sstFile = newSstFile(envOptions,options1,stringAppendOperator, javaPairRdd);

        /****************************Ingesting SST files**************************************/
        final StringAppendOperator stringAppendOperator1 = new StringAppendOperator();
        IngestingSSTfiles(envOptions,options1,stringAppendOperator1);

        /****************************Read Data From the RocksDB********************************/
        scanRockDBTable("test","new-cf");
    }

    private static File newSstFile(EnvOptions envOptions, Options options, StringAppendOperator stringAppendOperator, JavaPairRDD<Slice, Slice> javaPairRdd){
        //TODO: 重点是 K-V 有序
        SstFileWriter fw = null;
        try {
            // 创建一个 ComparatorOptions 实例，并使用 BytewiseComparator 作为键的比较器。
            ComparatorOptions comparatorOptions = null;
            BytewiseComparator comparator = null;
            comparatorOptions = new ComparatorOptions().setUseDirectBuffer(false);
            comparator = new BytewiseComparator(comparatorOptions);

            // 更新 options 对象，设置数据库创建时缺失则创建，使用默认环境，并设置比较器
            options = options
                    .setCreateIfMissing(true)
                    .setEnv(Env.getDefault())
                    .setComparator(comparator);

            // 创建 SstFileWriter 实例，并打开一个文件用于写入 SST 数据
            fw = new SstFileWriter(envOptions, options);
            fw.open(sstPath);

            // 收集结果
            List<Tuple2<Slice, Slice>> collected = javaPairRdd.collect();

            // 在驱动程序中处理结果
            for (Tuple2<Slice, Slice> pair : collected) {
                System.out.println("Key: " + pair._1.toString() + ", Value: " + pair._2.toString());
                // 这里可以处理你的 finalFw
                fw.put(pair._1, pair._2);
            }

            fw.finish();
        } catch (RocksDBException ex) {
            ex.printStackTrace();
        } finally {
            stringAppendOperator.close();
            envOptions.close();
            options.close();
            if (fw != null) {
                fw.close();
            }
        }

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
                    db.ingestExternalFile(cf_handle, Collections.singletonList(sstPath), ingestExternalFileOptions);
                }
            }
        }

        System.out.println("Generate SST File");
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
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrationRequired", "true")
                .setAppName("spark-gen-hfile")
                .setMaster("local[*]");

        conf.registerKryoClasses(
                new Class[]{
                        org.rocksdb.Slice.class,
                        Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
                        Class.forName("scala.reflect.ClassTag$$anon$1")});

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        Dataset<Row> rows = null;
        String path = "file:///home/lyh/Graph-Spark/data/test.csv";
        rows = spark.read().csv(path);
        return rows.toJavaRDD();
    }
}
