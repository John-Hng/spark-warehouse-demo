/opt/module/spark-yarn/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.kirk.warehouse.scheduler.WholeProcessScheduler \
  --num-executors 2 \
  --executor-memory 1G \
  /opt/warehouse/jars/ecommerce-warehouse.jar