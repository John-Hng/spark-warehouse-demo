-- 创建ODS层库：贴源原始数据层
CREATE DATABASE IF NOT EXISTS ods
COMMENT '操作数据层，原样接入业务源系统原始数据，保留原始粒度';

CREATE EXTERNAL TABLE IF NOT EXISTS ods.ods_user_behavior_log (
    user_id     BIGINT      COMMENT '序列化后的用户ID',
    item_id     BIGINT      COMMENT '序列化后的商品ID',
    category_id INT         COMMENT '序列化后的商品所属类目ID',
    behavior_type STRING    COMMENT '行为类型：pv/buy/cart/fav',
    ts          BIGINT      COMMENT '行为发生的时间戳（秒级）'
)
PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/warehouse/ods/ods_user_behavior_log'
TBLPROPERTIES ('skip.header.line.count'='0');
