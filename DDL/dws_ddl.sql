-- 创建DWS层库：主题汇总层
CREATE DATABASE IF NOT EXISTS dws
COMMENT '数据汇总层，按业务主题维度聚合的主题宽表';

-- 用户日行为统计表
CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_user_behavior_day (
    user_id     BIGINT      COMMENT '用户ID',
    pv_cnt      BIGINT      COMMENT '日浏览量',
    cart_cnt    BIGINT      COMMENT '日加购次数',
    fav_cnt     BIGINT      COMMENT '日收藏次数',
    buy_cnt     BIGINT      COMMENT '日购买次数'
)
PARTITIONED BY (dt STRING COMMENT '日期分区')
STORED AS ORC
LOCATION '/warehouse/dws/dws_user_behavior_day'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 商品销售情况表
CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_goods_sale_day (
    category_id     INT         COMMENT '商品类目ID',
    pv_cnt          BIGINT      COMMENT '类目日浏览量',
    cart_cnt        BIGINT      COMMENT '类目日加购次数',
    fav_cnt         BIGINT      COMMENT '类目日收藏次数',
    buy_cnt         BIGINT      COMMENT '类目日下单次数',
    buy_user_cnt    BIGINT      COMMENT '类目日下单用户数'
)
PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')
STORED AS ORC
LOCATION '/warehouse/dws/dws_goods_sale_day'
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.create.index'='true'
);
