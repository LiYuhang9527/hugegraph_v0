package com.gensst.RocksDB;


import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;


/**
 * 功能中关键技术
 * 1. Java 相关实现 https://stackoverflow.com/questions/58784381/creating-rocksdb-sst-file-in-java-for-bulk-loading
 * 2. C++ 相关实现  https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files
 *
 * 实现的功能
 * 1. generate SST
 * 2. bulkload into rocksDB
 * 3. 读取数据
 * 4. 验证 Copilot 协助编程工作流 沉淀下来 提效
 *
 *
 */

public class GenerateSST {
    private static final String cfdbPath = "./rocksdb-data-bulkload/";


    public static void main(String[] args) throws RocksDBException {
        final EnvOptions envOptions = new EnvOptions();
        // 自定义的合并操作符，用于 RocksDB 列族
        final StringAppendOperator stringAppendOperator = new StringAppendOperator();
        Options options1 = new Options();
        RocksDB.loadLibrary();

        /****************************Gen SST**************************************/

        File sstFile = newSstFile(envOptions,options1,stringAppendOperator);

        /****************************Ingesting SST files**************************************/
        final StringAppendOperator stringAppendOperator1 = new StringAppendOperator();
        IngestingSSTfiles(envOptions,options1,stringAppendOperator1);

        /****************************Read Data From the RocksDB********************************/
        scanRockDBTable("test","new-cf");



    }

    /**
     * 此方法生成SST
     *
     * @param envOptions
     * @param options
     * @param stringAppendOperator
     * @return
     */
    // 这个方法创建并返回一个 SST 文件。它设置了 RocksDB 的配置选项，
    // 创建了一个 SstFileWriter 对象，写入了一些键值对，并将 SST 文件保存到磁盘。
    // envOptions：用于配置 RocksDB 环境的选项。
    // options：用于配置 RocksDB 数据库的选项。
    // stringAppendOperator：用于指定 RocksDB 中字符串的合并操作符。
    private static File newSstFile(EnvOptions envOptions,Options options,StringAppendOperator stringAppendOperator){
        //TODO: 重点是 K-V 有序
        final Random random = new Random();

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
            fw.open("./tmp/db/sst_upload_01");

            // 创建一个 HashMap 存储键值对
            Map<String, String> data = new HashMap<>();
            for (int index = 0; index < 1000; index++) {
                data.put("Key-" + random.nextLong(), "Value-" + random.nextDouble());
            }
            List<String> keys = new ArrayList<String>(data.keySet());
            // 将键排序，确保它们是有序的，因为 SST 文件要求键必须是严格递增的。
            Collections.sort(keys);
            // 遍历排序后的键，将每个键值对写入 SST 文件
            for (String key : keys) {
                Slice keySlice = new Slice(key);
                Slice valueSlice = new Slice(data.get(key));
                fw.put(keySlice, valueSlice);
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

        File sstFile = new File("./tmp/db/sst_upload_01");
        return sstFile;
    }

    /**
     * 此方法主要是将SST 文件装载到RocksDB
     *
     */
    // 这个方法打开 RocksDB 数据库，并将之前创建的 SST 文件加载到数据库中。
    // envOptions：用于配置 RocksDB 环境的选项。
    // options：用于配置 RocksDB 数据库的选项。
    // stringAppendOperator：用于指定 RocksDB 中字符串的合并操作符。
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

        // 定义一个新的列族名称 new_cf
        String columnFamilyName = "new_cf";
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
                    db.ingestExternalFile(cf_handle, Collections.singletonList("./tmp/db/sst_upload_01"), ingestExternalFileOptions);
                }
            }
        }

        System.out.println("Generate SST File");

    }


    /**
     * 指定表名+CFName
     * 遍历此表
     * @param tableName
     * @param CfName
     */
    // 这个方法打开 RocksDB 数据库，并遍历指定的表和列族，打印出所有的键值对
    private static void scanRockDBTable(String tableName,String CfName){
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        RocksDB db = null;
        String cfName = "new_cf";
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

}
