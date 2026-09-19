package com.kirk.warehouse.dws;

import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.kirk.warehouse.scheduler.WholeProcessScheduler.dates;
import static com.kirk.warehouse.scheduler.WholeProcessScheduler.spark;

public class Dws_User_Behavior_Day {
    public static void runFull (SparkSession spark,List<String> dates) {
        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);

            spark.sql("insert overwrite table dws.dws_user_behavior_day partition(dt = '" + dt + "')\n" +
                    "select \n" +
                    "\tuser_id,\n" +
                    "\tsum(case when behavior_type = 'pv' then 1 else 0 end) as pv_cnt,\n" +
                    "\tsum(case when behavior_type = 'cart' then 1 else 0 end) as cart_cnt,\n" +
                    "\tsum(case when behavior_type = 'fav' then 1 else 0 end) as fav_cnt,\n" +
                    "\tsum(case when behavior_type = 'buy' then 1 else 0 end) as buy_cnt\n" +
                    "from dwd.dwd_user_behavior_detail\n" +
                    "where dt = '" + dt + "'\n" +
                    "group by user_id");

            System.out.println(dt + "分区处理完成");
        }

    }
    public static void main (String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

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
