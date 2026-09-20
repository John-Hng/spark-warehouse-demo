package com.kirk;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.kirk.wholetest.dates;


public class dwdtest {
    public static void runFull (SparkSession spark,List<String> dates) {
        spark.sql("select user_id,item_id,category_id,behavior_type,ts,dt " +
                "from (select *," +
                "row_number() over(partition by user_id,behavior_type,ts order by ts desc) as rn " +
                "from test.test1 " +
                "where user_id is not null and item_id is not null " +
                "and category_id is not null and behavior_type is not null " +
                "and ts is not null " +
                "and dt >= \"2017-11-25\" and dt <= \"2017-12-03\") as t " +
                "where rn = 1 "
        ).createOrReplaceTempView("t1");

        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);
            spark.sql("insert overwrite table test.dwdtest partition(dt = '" + dt + "') " +
                    "select user_id,item_id,category_id,behavior_type,ts " +
                    "from t1 " +
                    "where dt = '" + dt + "'" );
            System.out.println(dt + "分区处理完成");
        }

        spark.catalog().dropTempView("t1");

    }
    public static void main (String[] args) {
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
            runFull(spark,dates);
        } catch (Exception e) {
            System.err.println("DWD 层清洗失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
