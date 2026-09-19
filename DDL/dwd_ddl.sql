-- 创建DWD层库：明细清洗层
CREATE DATABASE IF NOT EXISTS dwd
COMMENT '数据明细层，清洗标准化后的原子级明细数据';

CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_user_behavior_detail (
    user_id     BIGINT      COMMENT '用户ID',
    item_id     BIGINT      COMMENT '商品ID',
    category_id INT         COMMENT '商品类目ID',
    behavior_type STRING    COMMENT '行为类型：pv浏览/buy购买/cart加购/fav收藏',
    ts          BIGINT      COMMENT '行为时间戳'
)
PARTITIONED BY (dt STRING COMMENT '日期分区，格式yyyy-MM-dd')
STORED AS ORC
LOCATION '/warehouse/dwd/dwd_user_behavior_detail'
TBLPROPERTIES (
    'orc.compress'='SNAPPY',
    'orc.create.index'='true'
);
