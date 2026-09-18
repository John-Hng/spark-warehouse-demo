package com.kirk.warehouse.dwd;

import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Dwd_Full_Scheduler {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession spark = SparkSessionUtil.getSession("dwd");

        spark.sql("select count(1) from ods.ods_user_behavior_log").show();

        List<String> dates = GenerateDateListUtil.generateDateList("2017-11-25", "2017-12-03");

        spark.sql("select user_id,item_id,category_id,behavior_type,ts,dt " +
                "from (select *," +
                "row_number() over(partition by user_id,behavior_type,ts order by ts desc) as rn " +
                "from ods.ods_user_behavior_log " +
                "where user_id is not null and item_id is not null " +
                "and category_id is not null and behavior_type is not null " +
                "and ts is not null " +
                "and dt >= \"2017-11-25\" and dt <= \"2017-12-03\") as t " +
                "where rn = 1 "
        ).createOrReplaceTempView("t1");

        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);
            spark.sql("insert overwrite table dwd.dwd_user_behavior_detail partition(dt = '" + dt + "') " +
                    "select user_id,item_id,category_id,behavior_type,ts " +
                    "from t1 " +
                    "where dt = '" + dt + "'" );
            System.out.println(dt + "分区处理完成");
        }

        spark.sql("select count(1) from dwd.dwd_user_behavior_detail").show();

        spark.close();
    }
}
