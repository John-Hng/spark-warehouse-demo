package com.kirk.warehouse.ods;

import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class Ods_Loader_sample {
    public static final StructType CSV_SCHEMA = new StructType()
            .add("user_id", DataTypes.LongType)
            .add("item_id", DataTypes.LongType)
            .add("category_id", DataTypes.IntegerType)
            .add("behavior_type", DataTypes.StringType)
            .add("ts", DataTypes.LongType);

    public static void runFull(SparkSession spark) {
        // 1. 读取HDFS上的全量原始CSV
        Dataset<Row> rawDF = spark.read()
                .option("header","false")
                .schema(CSV_SCHEMA)
                .csv("hdfs://hadoop102:8020/user/kirk/data/")
                .toDF("user_id","item_id","category_id","behavior_type","ts");

        rawDF.cache();

        // 2. 统计总条数，计算抽样比例
        long total = rawDF.count();
        double target = 10000000.0;
        double fraction = target / total;
        System.out.println("总数据量：" + total + "，抽样比例：" + fraction);

        // 3. 不放回随机抽样
        // 第一个参数false=不放回；第二个参数=抽样比例；第三个参数=随机种子，固定种子可复现结果
        Dataset<Row> sample = rawDF.sample(false, fraction, 1234);

        // 4. 写入Hive ODS分区表
        sample.createOrReplaceTempView("tmp_ods_sample");

        // 开启动态分区
        spark.sql("set hive.exec.dynamic.partition=true");
        // 关闭strict严格模式，允许全部分区都是动态
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");

        spark.sql("INSERT OVERWRITE TABLE ods.ods_user_behavior_log " +
                "SELECT " +
                "  user_id, item_id, category_id, behavior_type, ts, " +
                "  from_unixtime(ts, 'yyyy-MM-dd') as dt " +
                "FROM tmp_ods_sample"
        );

        rawDF.unpersist();

        System.out.println("最终数据量：" + sample.count());

    }

    public static void main (String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");
        SparkSession spark = SparkSessionUtil.getSession("spark");

        try {
            runFull(spark);
        } catch (Throwable t) {
            System.err.println("ODS 层导入失败");
            t.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }
}
