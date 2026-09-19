package com.kirk.warehouse.scheduler;

import com.kirk.warehouse.ads.Ads_Core_Metrics_Day;
import com.kirk.warehouse.ads.Ads_User_RFM_Level;
import com.kirk.warehouse.dwd.Dwd_Full_Scheduler;
import com.kirk.warehouse.dws.Dws_Goods_Sale_Day;
import com.kirk.warehouse.dws.Dws_User_Behavior_Day;
import com.kirk.warehouse.ods.Ods_Loader;
import com.kirk.warehouse.util.GenerateDateListUtil;
import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class WholeProcessScheduler {
    public static final SparkSession spark = SparkSessionUtil.getSession("spark");

    public static final String start_date = "2017-11-25";
    public static final String end_date = "2017-12-03";

    // 生成调度日期列表
    public static final List<String> dates = GenerateDateListUtil.generateDateList(start_date, end_date);

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        try {
            System.out.println("-------------开始执行全链路数仓调度-------------");

            System.out.println("[1/6] 开始 ODS 层全量数据导入...");
            Ods_Loader.runFull(spark);
            System.out.println("[1/6] ODS 层导入完成");

            System.out.println("[2/6] 开始 DWD 层全量数据清洗...");
            Dwd_Full_Scheduler.runFull(spark, dates);
            System.out.println("[2/6] DWD 层清洗完成");

            System.out.println("[3/6] DWS 层用户日行为宽表聚合...");
            Dws_User_Behavior_Day.runFull(spark,dates);
            System.out.println("[3/6] DWS 层用户宽表完成");

            System.out.println("[4/6] 开始 DWS 层商品类目日销售宽表聚合...");
            Dws_Goods_Sale_Day.runFull(spark,dates);
            System.out.println("[4/6] DWS 层商品宽表完成");

            System.out.println("[5/6] 开始 ADS 层每日核心指标计算...");
            Ads_Core_Metrics_Day.runFull(spark,dates);
            System.out.println("[5/6] ADS 层核心指标完成");

            System.out.println("[6/6] 开始 ADS 层 RFM 用户价值分层计算...");
            Ads_User_RFM_Level.runFull(spark,dates);
            System.out.println("[6/6] ADS 层 RFM 分层完成");

            System.out.println("-------------全链路数仓调度执行完成-------------");


        } catch (Exception e) {
            System.err.println("全链路调度执行失败");
            e.printStackTrace();
        } finally {
            spark.stop();
        }

    }
}
