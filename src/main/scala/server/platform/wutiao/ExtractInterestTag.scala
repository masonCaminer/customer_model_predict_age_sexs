package server.platform.wutiao

import java.text.SimpleDateFormat

import com.kingnetdc.watermelon.output.HBaseConnection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import server.platform.wutiao.PropertiesFactory.title
import utils.SparkGlobalSession.buildSparkSession
import com.kingnetdc.watermelon.utils._
import com.kingnetdc.watermelon.output.HBaseConnection._
import utils.TimeUtil.{currentDs, lastDs, setCalendarTime}
import PropertiesFactory._
import org.apache.spark.sql.{SparkSession,functions}
import ModelInputOutput._

object ExtractInterestTag {
  val appName: String = "sex_age_UserTag_Extract" + s"$title"
  val num_excutor: Int = 40
  val num_core: Int = 2
  /**
    * 五条hbase地址
    */
   val hbase_zookeeper_quorum=
    "hwwg-bigdata-hadoopdn-prod-1,hwwg-bigdata-hadoopdn-prod-2,hwwg-bigdata-hadoopdn-prod-3,hwwg-bigdata-hadoopdn-prod-4,hwwg-bigdata-hadoopdn-prod-5"
  /**
    * 五条用户新闻兴趣表
    */
   val hbase_table_news = "wutiao_rcmd:wt_v1.0_user_label_news"

  /**
    * 五条用户视频兴趣表
    */
   val hbase_table_video = "wutiao_rcmd:wt_v1.0_user_label_video"
  /**
    * Hbase表family名字
    */
   val base_info = Bytes.toBytes("words")

  /**
    * Hbase表qualify名字
    */
   val value1 = Bytes.toBytes("c")
   val value2 = Bytes.toBytes("k")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val t1 = System.currentTimeMillis()
    val spark = buildSparkSession(num_excutor, num_core, appName)
    import spark.implicits._
    val sc=spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val cur_ds=if(args.length==0)currentDs() else args(0)
    assert(cur_ds.matches("[\\d]{4}-[\\d]{2}-[\\d]{2}"),s"date must be XXXX-XX-XX but get cur_ds is ${cur_ds}")
    val format=new SimpleDateFormat("yyyy-MM-dd")
    val yester_ds=lastDs(1,timeTemp = setCalendarTime(cur_ds,format))
    println(s"current day is ${cur_ds} and get yesterday User Data insert into ${ResultProTable}")


    def getItemTag(ItemTable:String,sc:SparkContext)={
      val HbaseConf=getHbaseConfig(ItemTable,hbase_zookeeper_quorum)
      val HbaseScan=new Scan()
      HbaseScan.addColumn(base_info,value1)
      HbaseScan.addColumn(base_info,value2)
      HbaseConf.set(TableInputFormat.SCAN,convertScanToString(HbaseScan))
      val dataBase=sc.newAPIHadoopRDD(HbaseConf,
        classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result]).map{kv=>
//        uns=>rowkey:String,uns=>Array(Tuple(String,Double))
        val rowkey=kv._2.getRow().map(_.toString).mkString("")
        val deser_c=kv._2.getValue(Bytes.toBytes("words"),Bytes.toBytes("c"))
        val deser_k=kv._2.getValue(Bytes.toBytes("words"),Bytes.toBytes("k"))
        val label_c=if(deser_c==null || deser_c.isEmpty)Array.empty[(String,Double)] else StringUtils.deserializeObject[Array[(String,Double)]](deser_c)
        val label_k=if(deser_k==null ||deser_k.isEmpty)Array.empty[(String,Double)] else StringUtils.deserializeObject[Array[(String,Double)]](deser_k)
        val tag=(if(label_k.length>=10)label_k.take(10) else {
          (label_c++ label_k).sortWith((a,b)=>a._2>b._2).take(10)
        })
        (rowkey,tag)
      }
      dataBase
    }
    val news=getItemTag(hbase_table_news,spark.sparkContext).repartition(num_excutor*num_core*6)
    val videos=getItemTag(hbase_table_video,spark.sparkContext).repartition(num_excutor*num_core*6)
    val tagdata=news.join(videos).mapValues(vrdd=>{
      (vrdd._1 ++ vrdd._2).sortWith((a,b)=>a._2>b._2).take(10).map(_._1)
    }).toDF("tag_md5uid","array_tag")
//    val broadcastHbaseConnect=sc.broadcast(HBaseConnection.create(Map(("hbase.zookeeper.quorum", "hwwg-bigdata-zk-prod-1,hwwg-bigdata-zk-prod-2,hwwg-bigdata-zk-prod-3,hwwg-bigdata-zk-prod-4,hwwg-bigdata-zk-prod-5"),
//      ("zookeeper.znode.parent", "/hbase"),("hbase.rpc.timeout", "500000"),("hbase.client.retries.number", "3")
//    ,("hbase.client.pause", "50"),("mapreduce.output.fileoutputformat.outputdir", "/tmp"))))
    val hashMd5=functions.udf( (uid:String)=>{
        val md5=StringUtils.md5AsByteArray(uid)
        md5.map(_.toString).mkString("")
})
    val source=spark.sql(
      s"""
        |select *
        |from widetab.bdl_profile_general_gameladel_wutiao
        |where ds='$yester_ds'
      """.stripMargin).select($"uid",$"real_sex",$"real_age",$"label_age",$"model_sex",$"model_age",$"game_label",hashMd5($"uid") as "md5_uid")
    val result=source.join(tagdata,$"md5_uid"===$"tag_md5uid","left")
      .select($"uid",$"real_sex",$"real_age",$"label_age",$"model_sex",$"model_age",functions.expr("if(array_tag is null,game_label,array_tag)")  as "game_label")
    OutputModelPredictIntoHiveTable(result,yester_ds,ResultProTable)
    val t2 = (System.currentTimeMillis() - t1) / 1000
    println(s"The time cost $t2 second !")
    spark.stop()
  }

    def convertScanToString(scan: Scan): String =
    {
      val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
      return Base64.encodeBytes(proto.toByteArray)
    }
  def getHbaseConfig(tableName:String,zookeeper_quorum:String)={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zookeeper_quorum)
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set("hbase.rpc.timeout", "500000")
    conf.set("hbase.client.retries.number", "3")
    conf.set("hbase.client.pause", "50")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")
    conf.set(TableInputFormat.INPUT_TABLE,tableName)
    conf
  }

}
