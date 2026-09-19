package com.kirk.warehouse.ads;

import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.kirk.warehouse.scheduler.WholeProcessScheduler.dates;
import static com.kirk.warehouse.scheduler.WholeProcessScheduler.spark;

public class Ads_User_RFM_Level {
    public static void runFull (SparkSession spark,List<String> dates) {
        for (String dt : dates){
            System.out.println("开始处理分区：" + dt);

            spark.sql("insert overwrite table ads.ads_user_rfm_level partition(dt = '" + dt + "')\n" +
                    "select\n" +
                    "\tuser_id,\n" +
                    "\tr_value,\n" +
                    "\tf_value,\n" +
                    "\tm_value,\n" +
                    "\tcast((r_score * 0.3 + f_score * 0.4 + m_score * 0.3) * 20 as int) as rfm_score,\n" +
                    "\tcase\n" +
                    "\t\twhen (r_score * 0.3 + f_score * 0.4 + m_score * 0.3) * 20 >= 90 then '高价值'\n" +
                    "\t\twhen (r_score * 0.3 + f_score * 0.4 + m_score * 0.3) * 20 >= 80 then '中高价值'\n" +
                    "\t\twhen (r_score * 0.3 + f_score * 0.4 + m_score * 0.3) * 20 >= 70 then '中价值'\n" +
                    "\t\twhen (r_score * 0.3 + f_score * 0.4 + m_score * 0.3) * 20 >= 60 then '中低价值'\n" +
                    "\t\telse '低价值'\n" +
                    "\tend as user_level\n" +
                    "from (\n" +
                    "\tselect\n" +
                    "\t\tuser_id,\n" +
                    "\t\tr_value,\n" +
                    "\t\tf_value,\n" +
                    "\t\tm_value,\n" +
                    "\t\tcase \n" +
                    "\t\t\twhen percent_rank() over(order by r_value) <= 0.2 then 5\n" +
                    "\t\t\twhen percent_rank() over(order by r_value) <= 0.4 then 4\n" +
                    "\t\t\twhen percent_rank() over(order by r_value) <= 0.6 then 3\n" +
                    "\t\t\twhen percent_rank() over(order by r_value) <= 0.8 then 2\n" +
                    "\t\t\telse 1\n" +
                    "\t\tend as r_score,\n" +
                    "\t\tcase\n" +
                    "\t\t\twhen percent_rank() over(order by f_value desc) <= 0.2 then 5\n" +
                    "\t\t\twhen percent_rank() over(order by f_value desc) <= 0.4 then 4\n" +
                    "\t\t\twhen percent_rank() over(order by f_value desc) <= 0.6 then 3\n" +
                    "\t\t\twhen percent_rank() over(order by f_value desc) <= 0.8 then 2\n" +
                    "\t\t\telse 1\n" +
                    "\t\tend as f_score,\n" +
                    "\t\tcase\n" +
                    "\t\t\twhen percent_rank() over(order by m_value desc) <= 0.2 then 5\n" +
                    "\t\t\twhen percent_rank() over(order by m_value desc) <= 0.4 then 4\n" +
                    "\t\t\twhen percent_rank() over(order by m_value desc) <= 0.6 then 3\n" +
                    "\t\t\twhen percent_rank() over(order by m_value desc) <= 0.8 then 2\n" +
                    "\t\t\telse 1\n" +
                    "\t\tend as m_score\n" +
                    "\tfrom (\n" +
                    "\t\tselect\n" +
                    "\t\t\tuser_id,\n" +
                    "\t\t\tdatediff('" + dt + "',max(case when buy_cnt > 0 then dt end)) as r_value,\n" +
                    "\t\t\tsum(buy_cnt) as f_value,\n" +
                    "\t\t\tsum(buy_cnt) as m_value\n" +
                    "\t\tfrom dws.dws_user_behavior_day\n" +
                    "\t\twhere dt <= '" + dt + "'\n" +
                    "\t\tgroup by user_id\n" +
                    "\t\thaving sum(buy_cnt) > 0\n" +
                    "\t) as \n" +
                    ") as t_paid\t\n" +
                    "union all\n" +
                    "select\n" +
                    "\tuser_id,\n" +
                    "\t0 as r_value,\n" +
                    "\t0 as f_value,\n" +
                    "\t0 as m_value,\n" +
                    "\t0 as rfm_score,\n" +
                    "\t'低价值' AS user_level\n" +
                    "from (\n" +
                    "\tselect user_id\n" +
                    "\tfrom dws.dws_user_behavior_day\n" +
                    "\twhere dt <= '" + dt + "'\n" +
                    "\tgroup by user_id\n" +
                    "\thaving sum(buy_cnt) = 0\n" +
                    ") as t_unpaid");

            System.out.println(dt + "分区处理完成");
        }
    }
    public static void main (String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        try {
            runFull(spark,dates);
        } catch (Exception e) {
            System.err.println("ADS 层 RFM 分层失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
