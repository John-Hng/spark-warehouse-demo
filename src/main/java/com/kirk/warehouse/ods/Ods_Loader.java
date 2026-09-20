package com.kirk.warehouse.ods;

import com.kirk.warehouse.util.DataQualityUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;


public class Ods_Loader {
    public static final StructType CSV_SCHEMA = new StructType()
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
                .csv("hdfs://hadoop102:8020/user/kirk/data/")
                .toDF("user_id","item_id","category_id","behavior_type","ts");

        // 2. 统计总条数，计算抽样比例
        long total = rawDF.count();
        System.out.println("总数据量：" + total);

        // 4. 写入Hive ODS分区表
        rawDF.createOrReplaceTempView("tmp_ods");

        spark.sql("INSERT OVERWRITE TABLE ods.ods_user_behavior_log " +
                "SELECT " +
                "  user_id, item_id, category_id, behavior_type, ts, " +
                "  from_unixtime(ts, 'yyyy-MM-dd') as dt " +
                "FROM tmp_ods"
        );

        spark.catalog().dropTempView("tmp_ods");

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
