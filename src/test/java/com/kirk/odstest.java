package com.kirk;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;




public class odstest {
    private static final StructType CSV_SCHEMA = new StructType()
            .add("user_id", DataTypes.LongType)
            .add("item_id", DataTypes.LongType)
            .add("category_id", DataTypes.LongType)
            .add("behavior_type", DataTypes.StringType)
            .add("ts", DataTypes.LongType);

    public static void runFull(SparkSession spark) {
        // 1. 读取HDFS上的全量原始CSV
        Dataset<Row> rawDF = spark.read()
                .option("header","false")
                .schema(CSV_SCHEMA)
                .csv("C:\\Users\\34961\\Desktop\\UserBehavior.csv")
                .toDF("user_id","item_id","category_id","behavior_type","ts");

        rawDF.cache();

        // 2. 统计总条数，计算抽样比例
        long total = rawDF.count();
        double target = 10000.0;
        double fraction = target / total;
        System.out.println("总数据量：" + total + "，抽样比例：" + fraction);

        // 3. 不放回随机抽样
        // 第一个参数false=不放回；第二个参数=抽样比例；第三个参数=随机种子，固定种子可复现结果
        Dataset<Row> sample = rawDF.sample(false, fraction, 1234);

        // 4. 写入Hive ODS分区表
        sample.createOrReplaceTempView("tmp_ods_sample");

        spark.sql("INSERT OVERWRITE TABLE test.test1 " +
                "SELECT " +
                "  user_id, item_id, category_id, behavior_type, ts, " +
                "  from_unixtime(ts, 'yyyy-MM-dd') as dt " +
                "FROM tmp_ods_sample"
        );

        rawDF.unpersist();

        System.out.println("最终数据量：" + sample.count());

    }

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark")
                .enableHiveSupport()
                .config("spark.hadoop.hive.exec.dynamic.partition", "true")
                .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
                .config("spark.hadoop.hive.exec.max.dynamic.partitions", "100")
                .getOrCreate();

        try {
            runFull(spark);
        } catch (Exception e) {
            System.err.println("ODS 层导入失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
