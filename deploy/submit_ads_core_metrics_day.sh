/opt/module/spark-yarn/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.kirk.warehouse.ads.Ads_Core_Metrics_Day \
  --num-executors 2 \
  --executor-memory 1G \
  /opt/warehouse/jars/ecommerce-warehouse.jar