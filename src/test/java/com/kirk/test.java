package com.kirk;

import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

public class test {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");
        SparkSession spark = SparkSession
                .builder()
                .appName("spark")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("select count(1) from ods.ods_user_behavior_log").show();
        spark.sql("select count(1) from dwd.dwd_user_behavior_detail").show();
        spark.sql("select count(1) from dws.dws_user_behavior_day").show();
        spark.sql("select count(1) from dws.dws_goods_sale_day").show();
        spark.sql("select count(1) from ads.ads_core_metrics_day").show();
        spark.sql("select count(1) from ads.ads_user_rfm_level").show();
    }
}
