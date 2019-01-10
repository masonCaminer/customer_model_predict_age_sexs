/opt/app/spark/spark-2.2.0/bin/spark-submit \
--class server.platform.xy.User_sex_age_predict  \
--num-executors 30 --executor-memory 3G --executor-cores 2 --driver-memory 2G \
--conf spark.scheduler.listenerbus.eventqueue.size=100000 \
--jars /opt/developers/wangch/ToolsJar/zkclient-0.3.jar,/opt/developers/hesheng/JarTools/spark-streaming-kafka-0-10_2.11-2.2.0.jar,\
/opt/developers/hesheng/JarTools/kafka_2.11-0.10.0.1.jar,/opt/developers/hesheng/JarTools/kafka-clients-0.10.0.1.jar,\
/opt/developers/wangch/ToolsJar/hbase-client-1.2.0-cdh5.9.2.jar,/opt/developers/wangch/ToolsJar/hbase-server-1.2.0-cdh5.9.2.jar,\
/opt/developers/wangch/ToolsJar/hbase-common-1.2.0-cdh5.9.2.jar,/opt/developers/hesheng/JarTools/kafka_2.11-0.10.0.1.jar \
--driver-class-path /opt/developers/wangch/ToolsJar/mysql-connector-java-5.1.38.jar \
customer_model_predict_age_sex-1.0-SNAPSHOT.jar $1 $2>> run3.log 2>&1