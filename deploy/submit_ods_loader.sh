/opt/module/spark-yarn/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class com.kirk.warehouse.ods.OdsLoader \
  --num-executors 2 \
  --executor-memory 1G \
  /opt/warehouse/jars/ecommerce-warehouse.jar
