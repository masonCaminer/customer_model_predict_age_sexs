nohup /opt/app/spark/spark-2.2.0/bin/spark-submit \
        --class server.platform.xy.train_predictModel_xy_sex  \
        --num-executors 30 \
        --executor-memory 5G \
        --executor-cores 2  \
        --driver-memory 2G  \
        --conf spark.scheduler.listenerbus.eventqueue.size=100000 \
        --jars /opt/developers/maoyl/tools/kafka_2.11-0.10.2.0.jar,/opt/developers/maoyl/tools/spark-streaming-kafka-0-10_2.11-2.2.0.jar,/opt/developers/hesheng/topic_lda/hanlp-portable-1.5.3.jar,/opt/developers/maoyl/tools/kafka-clients-0.10.2.0.jar,/opt/developers/wangch/ToolsJar/bigdata-tools-1.0-SNAPSHOT.jar,/opt/developers/maoyl/tools/fastjson-1.2.47.jar,/opt/developers/maoyl/tools/spark-mllib_2.11-2.2.0.jar \
                customer_model_predict_age_sex-1.0-SNAPSHOT1.jar $1 2> log/1_sex.data 1>log/2_sex.data
