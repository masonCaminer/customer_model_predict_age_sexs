nohup /opt/app/spark/spark-2.2.0/bin/spark-submit \
        --class server.game.ald.User_sex_age_predict  \
        --num-executors 30 \
        --executor-memory 3G \
        --executor-cores 2  \
        --driver-memory 2G  \
        --conf spark.scheduler.listenerbus.eventqueue.size=100000 \
        --jars /opt/developers/maoyl/tools/kafka_2.11-0.10.2.0.jar,/opt/developers/maoyl/tools/spark-streaming-kafka-0-10_2.11-2.2.0.jar,/opt/developers/hesheng/topic_lda/hanlp-portable-1.5.3.jar,/opt/developers/maoyl/tools/kafka-clients-0.10.2.0.jar,/opt/developers/wangch/ToolsJar/bigdata-tools-1.0-SNAPSHOT.jar,/opt/developers/maoyl/tools/fastjson-1.2.47.jar,/opt/developers/maoyl/tools/spark-mllib_2.11-2.2.0.jar \
                customer_model_predict_age_sex-1.0-SNAPSHOT1.jar $1 $2>> run3.log 2>&1
