#!/usr/bin/sh
/opt/app/spark/spark-2.2.1/bin/spark-submit \
        --class server.platform.wutiao.ExtractInterestTag  \
        --num-executors 30 \
        --executor-memory 3G \
        --executor-cores 2  \
        --driver-memory 2G  \
        --jars /data/developers/chenghd/tools/hbase-client-1.2.0-cdh5.14.2.jar,/data/developers/chenghd/tools/hbase-common-1.2.0-cdh5.14.2.jar,/data/developers/chenghd/tools/hbase-protocol-1.2.0-cdh5.14.2.jar,/data/developers/chenghd/tools/hbase-server-1.2.0-cdh5.14.2.jar,/data/developers/chenghd/tools/htrace-core-3.2.0-incubating.jar,/data/developers/chenghd/tools/metrics-core-2.2.0.jar,/data/mining/Tools/metrics.properties,/opt/app/hive/hive-2.3.3/conf/hive-site.xml,/data/mining/Tools/watermelon_2.2.1-1.0-SNAPSHOT.jar \
        --conf spark.scheduler.listenerbus.eventqueue.size=100000 \
         /data/developers/maoyl/age_sex_predict/wutiao/customer_model_predict_age_sex-1.0-SNAPSHOT.jar $1 $2> /data/developers/maoyl/age_sex_predict/wutiao/run4.log 2>&1