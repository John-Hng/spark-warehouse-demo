-- 创建ADS层库：应用输出层
CREATE DATABASE IF NOT EXISTS ads
COMMENT '应用数据层，面向具体业务场景的高度聚合结果';

-- 每日全站核心指标表
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_core_metrics_day (
    pv                  BIGINT          COMMENT '全站页面浏览量',
    uv                  BIGINT          COMMENT '全站独立访客数',
    cart_cnt            BIGINT          COMMENT '全站加购次数',
    fav_cnt             BIGINT          COMMENT '全站收藏次数',
    order_cnt           BIGINT          COMMENT '全站下单次数',
    order_user_cnt      BIGINT          COMMENT '全站下单用户数',
    pay_rate            DECIMAL(5,4)    COMMENT '全站付费转化率=下单用户数/UV',
    cart_convert_rate   DECIMAL(5,4)    COMMENT '加购转化率=下单次数/加购次数'
)
PARTITIONED BY (dt STRING COMMENT '统计日期，格式yyyy-MM-dd')
STORED AS ORC
LOCATION '/warehouse/ads/ads_core_metrics_day'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 用户 RFM 价值分层表
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_user_rfm_level (
    user_id         BIGINT          COMMENT '用户ID',
    r_value         INT             COMMENT 'R值：最近一次消费距统计日的天数',
    f_value         INT             COMMENT 'F值：统计周期内消费频次',
    m_value         DECIMAL(10,2)   COMMENT 'M值：统计周期内消费总金额',
    rfm_score       INT             COMMENT 'RFM综合得分，百分制',
    user_level      STRING          COMMENT '用户价值层级：高价值/中高价值/中价值/中低价值/低价值'
)
PARTITIONED BY (dt STRING COMMENT '分层计算截止日期，格式yyyy-MM-dd')
STORED AS ORC
LOCATION '/warehouse/ads/ads_user_rfm_level'
TBLPROPERTIES ('orc.compress'='SNAPPY');
