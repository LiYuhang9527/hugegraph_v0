package com.genfile.HBase.MR;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// 用于生成HBase的HFile文件，然后可以批量加载到HBase表中
public class HFileGenerator {
    public static class HFileMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        // HBase表中的列族和列的字节表示
        public static final byte[] FAMILY = Bytes.toBytes("pd");
        public static final byte[] COL = Bytes.toBytes("bf");
        // 用于存储行键
        public static final ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        // map 方法接收输入键值对 key 和 value，并将它们转换为HBase的 Put 对象
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            System.out.println("in map............");
            String[] line = value.toString().split("\t"); // <1>
            byte[] rowKeyBytes = Bytes.toBytes(line[0]);
            rowKey.set(rowKeyBytes);
            System.out.println("line[0] = " + line[0] + "\tline[1] = " + line[1]);
            KeyValue kv = new KeyValue(rowKeyBytes, FAMILY, COL, Bytes.toBytes(line[1])); // <6>
            Put put = new Put(rowKeyBytes);
            put.addColumn(FAMILY, COL, Bytes.toBytes(line[1]));
            System.out.println("Got here...");
            // 使用 context.write 将 rowKey 和 Put 对象写入上下文，供后续处理。
            context.write(rowKey, put);
            System.out.println("Wrote into context...");
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("In main..");
        Configuration conf = new Configuration();
        String[] dfsArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = new Job(conf, "HFile bulk load test");
        job.setJarByClass(HFileGenerator.class);

        // Mapper生成HBase的 Put 对象，Reducer（KeyValueSortReducer）负责排序这些对象
        job.setMapperClass(HFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

        FileInputFormat.addInputPath(job, new Path(dfsArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(dfsArgs[1]));
        String OUTPUT_PATH = "/tmp/" + new Random().nextInt(1000);
//        HFileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        HFileOutputFormat2.setOutputPath(job, new Path(OUTPUT_PATH));
        System.out.println("OUTPUT_PATH = " + OUTPUT_PATH);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "10.211.126.134,10.211.126.103,10.211.126.130");
        hbaseConf.set("hbase.table.name", "steve1");
//        HTable table = new HTable(hbaseConf, "steve1");
//        System.out.println("table.getName() = " + table.getName());
//        HFileOutputFormat.configureIncrementalLoad(job, table);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
