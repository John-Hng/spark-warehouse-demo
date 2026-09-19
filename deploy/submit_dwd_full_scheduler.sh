/opt/module/spark-yarn/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.kirk.warehouse.dwd.Dwd_Full_Scheduler \
  --num-executors 2 \
  --executor-memory 1G \
  /opt/warehouse/jars/ecommerce-warehouse.jar