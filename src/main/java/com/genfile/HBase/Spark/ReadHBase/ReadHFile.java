package com.genfile.HBase.Spark.ReadHBase;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import com.genfile.HBase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

// 这段代码是一个使用Apache Spark读取HBase数据的Java程序
public class ReadHFile {
    // 将HBase的Scan对象转换为字符串，用于配置
    private static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public static void main(String[] args) throws IOException {
        int max_versions = 3;
        // 创建SparkConf并设置应用名称和运行模式（本地模式）
        SparkConf sparkConf = new SparkConf().setAppName("sparkReadHFile")
                                             .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // 创建HBase配置hconf并设置HBase的根目录、ZooKeeper的地址和端口。
        Configuration hconf = HBaseConfiguration.create();
        hconf.set("hbase.rootdir", "file:///Users/yangjiaqi/Documents/learnStudent/software/hbase-2.0.7-SNAPSHOT/data1");
        hconf.set("hbase.zookeeper.quorum", "localhost");
        hconf.set("hbase.zookeeper.property.clientPort", "2181");

        // 创建Scan对象并设置要扫描的列族和最大版本数。
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("f"));
        scan.setMaxVersions(max_versions);

        // 将Scan对象转换为字符串并设置到HBase的配置中。
        hconf.set(TableInputFormat.SCAN, convertScanToString(scan));
        // 使用HBase配置创建Hadoop的Job对象。
        Job job = Job.getInstance(hconf);
        Path path = new Path("file:///Users/yangjiaqi/Documents/learnStudent/software/hbase-2.0.7-SNAPSHOT/snapshot");
        // 设置快照名称和快照路径，使用TableSnapshotInputFormat作为输入格式
        String snapName ="snap_g_v";
        TableSnapshotInputFormat.setInput(job, snapName, path);

        // 使用JavaSparkContext和TableSnapshotInputFormat创建一个JavaPairRDD，包含HBase的行键和结果。
        JavaPairRDD<ImmutableBytesWritable, Result> newAPIHadoopRDD = sc.newAPIHadoopRDD(job.getConfiguration(), TableSnapshotInputFormat.class, ImmutableBytesWritable.class,Result.class);


        // 使用map操作将RDD中的每个元素转换为字符串，这里只返回行键。
        // 收集转换后的数据并打印。
        List<String> collect = newAPIHadoopRDD.map(
                new Function<Tuple2<ImmutableBytesWritable, Result>, String>(){
            public String call(Tuple2<ImmutableBytesWritable, Result> v1)
                    throws Exception {
                // TODO Auto-generated method stub
                Result result = v1._2();
                if (result.isEmpty()) {
                    return null;
                }
                String rowKey = Bytes.toString(result.getRow());
                //System.out.println("行健为："+rowKey);
                NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("f"));
                Set<Entry<byte[], byte[]>> entrySet = familyMap.entrySet();
                Iterator<Entry<byte[], byte[]>> it = entrySet.iterator();
                String columName = null;
                String columValue = null;
                System.out.println("rowKey: " + rowKey);
                while(it.hasNext()){
                    Entry<byte[], byte[]> entry = it.next();
                    columName = new String(entry.getKey());//列
                    columValue = new String(entry.getValue());
                    System.out.println("colunName: " + columName + " colunValue: "+ columValue);
                }

                return  rowKey;
            }
        }).collect();
    }
}
