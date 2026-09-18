package com.kirk.warehouse;

import com.kirk.warehouse.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

public class CreateDatabaseAndTable {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","kirk");

        SparkSession create = SparkSessionUtil.getSession("create");

        // TODO 各层数据库
        create.sql("-- 创建ODS层库：贴源原始数据层\n" +
                "CREATE DATABASE IF NOT EXISTS ods \n" +
                "COMMENT '操作数据层，原样接入业务源系统原始数据，保留原始粒度';\n" +
                "\n" +
                "-- 创建DWD层库：明细清洗层\n" +
                "CREATE DATABASE IF NOT EXISTS dwd \n" +
                "COMMENT '数据明细层，清洗标准化后的原子级明细数据';\n" +
                "\n" +
                "-- 创建DWS层库：主题汇总层\n" +
                "CREATE DATABASE IF NOT EXISTS dws \n" +
                "COMMENT '数据汇总层，按业务主题维度聚合的主题宽表';\n" +
                "\n" +
                "-- 创建ADS层库：应用输出层\n" +
                "CREATE DATABASE IF NOT EXISTS ads \n" +
                "COMMENT '应用数据层，面向具体业务场景的高度聚合结果';\n");

        // TODO ODS层表结构
        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_user_behavior_log (\n" +
                "    user_id     BIGINT      COMMENT '序列化后的用户ID',\n" +
                "    item_id     BIGINT      COMMENT '序列化后的商品ID',\n" +
                "    category_id INT         COMMENT '序列化后的商品所属类目ID',\n" +
                "    behavior_type STRING    COMMENT '行为类型：pv/buy/cart/fav',\n" +
                "    ts          BIGINT      COMMENT '行为发生的时间戳（秒级）'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')\n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ',' \n" +
                "LINES TERMINATED BY '\\n'\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '/warehouse/ods/ods_user_behavior_log'\n" +
                "TBLPROPERTIES ('skip.header.line.count'='0');\n");

        // TODO DWD层表结构
        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_user_behavior_detail (\n" +
                "    user_id     BIGINT      COMMENT '用户ID',\n" +
                "    item_id     BIGINT      COMMENT '商品ID',\n" +
                "    category_id INT         COMMENT '商品类目ID',\n" +
                "    behavior_type STRING    COMMENT '行为类型：pv浏览/buy购买/cart加购/fav收藏',\n" +
                "    ts          BIGINT      COMMENT '行为时间戳'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')\n" +
                "STORED AS ORC\n" +
                "LOCATION '/warehouse/dwd/dwd_user_behavior_detail'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress'='SNAPPY',\n" +
                "    'orc.create.index'='true'\n" +
                ");\n");

        // TODO DWS层表结构
        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_user_behavior_day (\n" +
                "    user_id     BIGINT      COMMENT '用户ID',\n" +
                "    pv_cnt      BIGINT      COMMENT '日浏览量',\n" +
                "    cart_cnt    BIGINT      COMMENT '日加购次数',\n" +
                "    fav_cnt     BIGINT      COMMENT '日收藏次数',\n" +
                "    buy_cnt     BIGINT      COMMENT '日购买次数'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '日期分区')\n" +
                "STORED AS ORC\n" +
                "LOCATION '/warehouse/dws/dws_user_behavior_day'\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY');\n");

        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_goods_sale_day (\n" +
                "    category_id     INT         COMMENT '商品类目ID',\n" +
                "    pv_cnt          BIGINT      COMMENT '类目日浏览量',\n" +
                "    cart_cnt        BIGINT      COMMENT '类目日加购次数',\n" +
                "    fav_cnt         BIGINT      COMMENT '类目日收藏次数',\n" +
                "    buy_cnt         BIGINT      COMMENT '类目日下单次数',\n" +
                "    buy_user_cnt    BIGINT      COMMENT '类目日下单用户数'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')\n" +
                "STORED AS ORC\n" +
                "LOCATION '/warehouse/dws/dws_goods_sale_day'\n" +
                "TBLPROPERTIES (\n" +
                "    'orc.compress'='SNAPPY',\n" +
                "    'orc.create.index'='true'\n" +
                ");\n");

        // TODO ADS层表结构
        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_core_metrics_day (\n" +
                "    pv                  BIGINT          COMMENT '全站页面浏览量',\n" +
                "    uv                  BIGINT          COMMENT '全站独立访客数',\n" +
                "    cart_cnt            BIGINT          COMMENT '全站加购次数',\n" +
                "    fav_cnt             BIGINT          COMMENT '全站收藏次数',\n" +
                "    order_cnt           BIGINT          COMMENT '全站下单次数',\n" +
                "    order_user_cnt      BIGINT          COMMENT '全站下单用户数',\n" +
                "    pay_rate            DECIMAL(5,4)    COMMENT '全站付费转化率=下单用户数/UV',\n" +
                "    cart_convert_rate   DECIMAL(5,4)    COMMENT '加购转化率=下单次数/加购次数'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '统计日期，格式yyyy-MM-dd')\n" +
                "STORED AS ORC\n" +
                "LOCATION '/warehouse/ads/ads_core_metrics_day'\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY');\n");

        create.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_user_rfm_level (\n" +
                "    user_id         BIGINT          COMMENT '用户ID',\n" +
                "    r_value         INT             COMMENT 'R值：最近一次消费距统计日的天数',\n" +
                "    f_value         INT             COMMENT 'F值：统计周期内消费频次',\n" +
                "    m_value         DECIMAL(10,2)   COMMENT 'M值：统计周期内消费总金额',\n" +
                "    rfm_score       INT             COMMENT 'RFM综合得分，百分制',\n" +
                "    user_level      STRING          COMMENT '用户价值层级：高价值/中高价值/中价值/中低价值/低价值'\n" +
                ")\n" +
                "PARTITIONED BY (dt STRING COMMENT '分层计算截止日期，格式yyyy-MM-dd')\n" +
                "STORED AS ORC\n" +
                "LOCATION '/warehouse/ads/ads_user_rfm_level'\n" +
                "TBLPROPERTIES ('orc.compress'='SNAPPY');\n");


    }
}
