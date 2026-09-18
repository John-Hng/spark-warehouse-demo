package com.kirk.warehouse.dws;

import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Dws_Goods_Sale_Day {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession spark = SparkSessionUtil.getSession("dws");

        List<String> dates = GenerateDateListUtil.generateDateList("2017-11-25", "2017-12-03");

        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);

            spark.sql("insert overwrite dws.dws_goods_sale_day partition(dt = '" + dt + "')\n" +
                    "select \n" +
                    "\tcategory_id,\n" +
                    "\tsum(case when behavior_type = 'pv' then 1 else 0 end) as pv_cnt,\n" +
                    "\tsum(case when behavior_type = 'cart' then 1 else 0 end) as cart_cnt,\n" +
                    "\tsum(case when behavior_type = 'fav' then 1 else 0 end) as fav_cnt,\n" +
                    "\tsum(case when behavior_type = 'buy' then 1 else 0 end) as buy_cnt,\n" +
                    "\tcount(distinct case when behavior_type = 'buy' then user_id end) as buy_user_cnt\n" +
                    "from dwd.dwd_user_behavior_detail\n" +
                    "where dt = '" + dt + "'\n" +
                    "group by category_id");

            System.out.println(dt + "分区处理完成");
        }

        spark.sql("select count(1) from dws.dws_goods_sale_day").show();

        spark.close();


    }

}
