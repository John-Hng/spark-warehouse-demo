package com.kirk;

import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.kirk.wholetest.dates;


public class dwstest1 {
    public static void runFull (SparkSession spark,List<String> dates) {
        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);

            spark.sql("insert overwrite table test.dwstest1 partition(dt = '" + dt + "')\n" +
                    "select \n" +
                    "\tuser_id,\n" +
                    "\tsum(case when behavior_type = 'pv' then 1 else 0 end) as pv_cnt,\n" +
                    "\tsum(case when behavior_type = 'cart' then 1 else 0 end) as cart_cnt,\n" +
                    "\tsum(case when behavior_type = 'fav' then 1 else 0 end) as fav_cnt,\n" +
                    "\tsum(case when behavior_type = 'buy' then 1 else 0 end) as buy_cnt\n" +
                    "from test.dwdtest\n" +
                    "where dt = '" + dt + "'\n" +
                    "group by user_id");

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
            System.err.println("DWS 层用户宽表失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }

}
