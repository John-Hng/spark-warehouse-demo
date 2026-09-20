package com.kirk.warehouse;

import org.apache.spark.sql.SparkSession;

import static com.kirk.warehouse.scheduler.WholeProcessScheduler.dates;

public class Drop {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark")
                .enableHiveSupport()
                .getOrCreate();

        for (String dt : dates){
            spark.sql("ALTER TABLE ods.ods_user_behavior_log DROP IF EXISTS PARTITION(dt = '" + dt + "');");
            spark.sql("ALTER TABLE dwd.dwd_user_behavior_detail DROP IF EXISTS PARTITION(dt = '" + dt + "');");
            spark.sql("ALTER TABLE dws.dws_user_behavior_day DROP IF EXISTS PARTITION(dt = '" + dt + "');");
            spark.sql("ALTER TABLE dws.dws_goods_sale_day DROP IF EXISTS PARTITION(dt = '" + dt + "');");
            spark.sql("ALTER TABLE ads.ads_core_metrics_day DROP IF EXISTS PARTITION(dt = '" + dt + "');");
            spark.sql("ALTER TABLE ads.ads_user_rfm_level DROP IF EXISTS PARTITION(dt = '" + dt + "');");
        }
    }
}
