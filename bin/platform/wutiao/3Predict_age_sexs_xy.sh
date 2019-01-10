nohup /opt/app/spark/spark-2.2.1/bin/spark-submit \
        --class server.platform.wutiao.User_sex_age_predict  \
        --num-executors 30 \
        --executor-memory 3G \
        --executor-cores 2  \
        --driver-memory 2G  \
        --conf spark.scheduler.listenerbus.eventqueue.size=100000 \
                customer_model_predict_age_sex-1.0-SNAPSHOT1.jar $1 $2>> run3.log 2>&1