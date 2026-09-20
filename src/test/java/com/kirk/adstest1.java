package com.kirk;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.kirk.wholetest.dates;


public class adstest1 {
    public static void runFull (SparkSession spark,List<String> dates) {
        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);

            spark.sql("insert overwrite test.adstest1 partition (dt = '" + dt + "')\n" +
                    "select\n" +
                    "\tsum(pv_cnt) as pv,\n" +
                    "\tcount(distinct user_id) as uv,\n" +
                    "\tsum(cart_cnt) as cart_cnt,\n" +
                    "\tsum(fav_cnt) as fav_cnt,\n" +
                    "\tsum(buy_cnt) as order_cnt,\n" +
                    "\tcount(distinct case when buy_cnt > 0 then user_id end) as order_user_cnt,\n" +
                    "\tround(count(distinct case when buy_cnt > 0 then user_id end) / count(distinct user_id),4) as pay_rate,\n" +
                    "\tround(sum(buy_cnt) / sum(cart_cnt),4) as  cart_convert_rate\n" +
                    "from test.dwstest1\n" +
                    "where dt = '" + dt + "'");

            System.out.println(dt + "分区处理完成");
        }
    }
    public static void main (String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("spark")
                .enableHiveSupport()
                .getOrCreate();

        try {
            runFull(spark,dates);
        } catch (Exception e) {
            System.err.println("ADS 层核心指标失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
