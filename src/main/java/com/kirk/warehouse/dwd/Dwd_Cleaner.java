package com.kirk.warehouse.dwd;

import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Dwd_Cleaner {
    public static void runDay(SparkSession spark,String dt) {
        if (dt == null || !dt.matches("\\d{4}-\\d{2}-\\d{2}")){
            throw new IllegalArgumentException("日期参数格式错误，必须为 yyyy-MM-dd，当前值：" + dt);
        }

        spark.sql("select user_id,item_id,category_id,behavior_type,ts,dt " +
                "from (select *," +
                "row_number() over(partition by user_id,behavior_type,ts order by ts desc) as rn " +
                "from ods.ods_user_behavior_log " +
                "where user_id is not null and user_id != '' and item_id is not null " +
                "and item_id != '' and category_id is not null and behavior_type is not null " +
                "and category_id != '' and behavior_type != ''" +
                "and dt = '" + dt + "') as t " +
                "where rn = 1 "
        ).createOrReplaceTempView("t1");

        System.out.println("开始清理 DWD 层日期：" + dt);
        spark.sql("insert overwrite table dwd.dwd_user_behavior_detail partition(dt = '" + dt + "') " +
                "select user_id,item_id,category_id,behavior_type,ts " +
                "from t1 " +
                "where dt = '" + dt + "'" );
        System.out.println(dt + "分区处理完成");
    }
    public static void main(String[] args){
        SparkSession spark = SparkSessionUtil.getSession("dwd_clean");

        try {
            // 支持两种方式：args传参 或 默认日期
            String dt = args.length > 0 ? args[0] : "2017-11-25";
            runDay(spark, dt);
        } finally {
            spark.stop();
        }
    }
}
