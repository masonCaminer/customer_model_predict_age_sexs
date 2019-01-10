package utils

import com.google.common.math.IntMath
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.Row

import scala.Array.ofDim
import scala.collection.mutable.ArrayBuffer


object EvaluationAlgorithm {
  /**
    * 只能用于二分类
    * prediction,label
    * 计算auc
    * label 为0,1 1为正，0为负
    * @param data
    * @return
    */
  def auc(data: RDD[(Double, Long)]): Double = {
    //group same score result
    val group_result = data.groupByKey().map(x => {
      var r = new Array[Long](2)
      for (item <- x._2) {
        if (item > 0) r(1) += 1
        else r(0) += 1
      }
      (x._1, r) // score, [ MinusN, PositiveN ]
    })
    //points 需要累积
    val group_rank = group_result.sortByKey(false) //big first
    //计算累积
    var step_sizes = group_rank.mapPartitions(x => {
      var r = List[(Long, Long)]()
      var mn_sum = 0L//负样本个数
      var pn_sum = 0L//正样本个数
      while (x.hasNext) {
        val cur = x.next
        mn_sum += cur._2(0)
        pn_sum += cur._2(1)
      }
      r.::(mn_sum, pn_sum).toIterator
    }, true).collect
    var step_sizes_sum = ofDim[Long](step_sizes.size, 2) //二维数组
    for (i <- 0 to (step_sizes.size - 1)) {
      if (i == 0) {
        step_sizes_sum(i)(0) = 0
        step_sizes_sum(i)(1) = 0
      } else {
        step_sizes_sum(i)(0) = step_sizes_sum(i - 1)(0) + step_sizes(i - 1)._1
        step_sizes_sum(i)(1) = step_sizes_sum(i - 1)(1) + step_sizes(i - 1)._2
      }
    }
    val sss_len = step_sizes_sum.size
    val total_fn = step_sizes_sum(sss_len - 1)(0) + step_sizes(sss_len - 1)._1
    val total_pn = step_sizes_sum(sss_len - 1)(1) + step_sizes(sss_len - 1)._2
    val bc_step_sizes_sum = data.context.broadcast(step_sizes_sum)
    val modified_group_rank = group_rank.mapPartitionsWithIndex((index, x) => {
      var sss = bc_step_sizes_sum.value
      var r = List[(Double, Array[Long])]()
      var mn = sss(index)(0) //start point
      var pn = sss(index)(1)
      while (x.hasNext) {
        var p = new Array[Long](2)
        val cur = x.next
        p(0) = mn + cur._2(0)
        p(1) = pn + cur._2(1)
        mn += cur._2(0)
        pn += cur._2(1)
        //r.::= (cur._1, p(0).toString() + "\t" + p(1).toString())
        r.::=(cur._1, p)
      }
      r.reverse.toIterator
    }, true)
    val score = modified_group_rank.sliding(2).aggregate(0.0)(
      seqOp = (auc: Double, points: Array[(Double, Array[Long])]) => auc + TrapezoidArea(points),
      combOp = _ + _
    )
    score / (total_fn * total_pn)
  }

  /**
    * RMSE算法
    * 预测分，真实分
    * @param predictAndActual
    * @return
    */
  def caleRMSE(predictAndActual: RDD[(Double, Double)]): Double = {
    val squareErrorTotal = predictAndActual.map { x =>
      val predict = x._1
      val actual = x._2
      val diff = predict - actual
      val square = Math.pow(diff, 2)
      square
    }.reduce(_ + _)
    val rmse = Math.sqrt(squareErrorTotal / predictAndActual.count())
    rmse
  }

  /**
    * MAE算法
    * 预测分，真实分
    * @param predictAndActual
    * @return
    */
  def calcMAE(predictAndActual: RDD[(Double, Double)]): Double = {
    val squareErrorTotal = predictAndActual.map { x =>
      val predict = x._1
      val actual = x._2
      val diff = predict - actual
      val square = Math.abs(diff)
      square
    }.reduce(_ + _)
    val rmse = Math.sqrt(squareErrorTotal / predictAndActual.count())
    rmse
  }

  /**
    * 准确率计算
    *
    * @param labelsTrue
    * @param labelsPred
    * @return
    */
  def precision(TP: Int, FP: Int, FN: Int, TN: Int): Double = {
    val table = contingencyTable(TP, FP, FN, TN)
    1.0 * table.TP / (table.TP + table.FP)
  }

  /**
    * 准确率：Precision
    * 通过真实标签和测试标签来计算
    * @param labelsTrue
    * @param labelsPred
    * @return
    */
  def precision(labelsTrue: Array[Int], labelsPred: Array[Int]) = {
    labelChecker(labelsTrue, labelsPred)
    val table: Table = contingencyTable(labelsTrue, labelsPred)
    1.0 * table.TP / (table.TP + table.FP)
  }

  /**
    * 召回率计算
    *
    * @param TP
    * @param FP
    * @param FN
    * @param TN
    * @return
    */
  def recall(TP: Int, FP: Int, FN: Int, TN: Int): Double = {
    val table = contingencyTable(TP, FP, FN, TN)
    1.0 * table.TP / (table.TP + table.FN)
  }

  /**
    * 召回率：Recall
    * 通过真实标签和测试标签来计算
    * @param labelsTrue
    * @param labelsPred
    * @return
    */
  def recall(labelsTrue: Array[Int], labelsPred: Array[Int]) = {
    labelChecker(labelsTrue, labelsPred)
    val table: Table = contingencyTable(labelsTrue, labelsPred)
    1.0 * table.TP / (table.TP + table.FN)
  }

  /**
    * FMeasure
    *
    * @param TP
    * @param FP
    * @param FN
    * @param TN
    * @param beta
    * @return
    */
  def FMeasure1(TP: Int, FP: Int, FN: Int, TN: Int)(implicit beta: Double = 1.0): Double = {
    val precision1: Double = precision(TP, FP, FN, TN)
    val recall1: Double = recall(TP, FP, FN, TN)
    (math.pow(beta, 2) + 1) * precision1 * recall1 / (math.pow(beta, 2) * precision1 + recall1)
  }

  /**
    * FMeasure
    * F值
    * 通过真实标签和测试标签来计算
    * @param labelsTrue
    * @param labelsPred
    * @param beta
    * @return
    */
  def FMeasure2(labelsTrue: Array[Int], labelsPred: Array[Int])(implicit beta: Double = 1.0): Double = {
    labelChecker(labelsTrue, labelsPred)
    val precision1: Double = precision(labelsTrue, labelsPred)
    val recall1: Double = recall(labelsTrue, labelsPred)
    (math.pow(beta, 2) + 1) * precision1 * recall1 / (math.pow(beta, 2) * precision1 + recall1)
  }

  /**
    * 正确率计算
    *
    * @param TP
    * @param FP
    * @param FN
    * @param TN
    * @return
    */
  def accuracy(TP: Int, FP: Int, FN: Int, TN: Int): Double = {
    val table = contingencyTable(TP, FP, FN, TN)
    1.0 * (table.TP + table.TN) / (table.TP + table.FP + table.TN + table.FN)
  }

  /**
    * 多分类度量
    * @param model
    * @param trainRawData
    * @return
    */
  def getMetrics(trainRawData: sql.DataFrame): MulticlassMetrics = {
    val predictionsAndLabels = trainRawData.rdd.map{case Row(label:Double,prediction:Double)=>(prediction,label)}
    new MulticlassMetrics(predictionsAndLabels)
  }

  /**
    * 多分类综合评估
    * @param label
    * @param predict
    * @param vaild_Prediction
    * @return acc,precision,weightedRecall,f1
    */
  def comprehensiveAssessment(label:String,predict:String,vaild_Prediction:sql.DataFrame) ={
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(label)
      .setPredictionCol(predict)
    val acc = evaluator.setMetricName("accuracy").evaluate(vaild_Prediction)
    val precision = evaluator.setMetricName("weightedPrecision").evaluate(vaild_Prediction)
    val weightedRecall = evaluator.setMetricName("weightedRecall").evaluate(vaild_Prediction)
    val f1 = evaluator.setMetricName("f1").evaluate(vaild_Prediction)
    (acc,precision,weightedRecall,f1)
  }


  /**
    * 二分类评估
    * @param label
    * @param predict
    * @param vaild_Prediction
    * @return
    */
  def  binaryEvalation(label:String,predict:String,vaild_Prediction:sql.DataFrame) ={
    val evaluater = new BinaryClassificationEvaluator().setLabelCol(label).setRawPredictionCol(predict)
    val areaUnderROC = evaluater.setMetricName("areaUnderROC").evaluate(vaild_Prediction)
    val areaUnderPR = evaluater.setMetricName("areaUnderPR").evaluate(vaild_Prediction)
    (areaUnderROC,areaUnderPR)
  }
  /**
    * 正确率计算
    *
    * @param labelsTrue
    * @param labelsPred
    * @return
    */
  def accuracy(labelsTrue: Array[Int], labelsPred: Array[Int]): Double = {
    labelChecker(labelsTrue, labelsPred)
    val table: Table = contingencyTable(labelsTrue, labelsPred)
    1.0 * (table.TP + table.TN) / (table.TP + table.FP + table.FN + table.TN)
  }

  /**
    * RS
    * 商品a的排名，推荐列表的数目
    */
  def RS(result:RDD[(Int,Int)]): Double = {
      val c = result.map{x=>
        1.0*x._1/x._2
      }.reduce(_+_)
    c/result.count()
  }

  /**
    * MRR
    * 商品列表，推荐的商品
    */
  def MRR(results: RDD[(Array[String], String)]): Double = {
    val r = results.map { x =>
      val results = x._1.zip(Stream from 1).toMap
      val corrRes = x._2
      println(results.get(corrRes).get)
      1.0 / results.get(corrRes).get
    }.reduce(_ + _)
    1.0 / results.count() * r
  }

  /**
    * MAP
    * results 为每个推荐列表中点击的商品的顺序（推荐列表默认从1开始）
    */
  def MAP(results: RDD[Array[Int]]): Double = {
    val o = results.map { x =>
      val res = x.zip(Stream from 1).toMap
      val r = res.map { x =>
        x._2 / x._1
      }.reduce(_ + _)
      1.0 * r / res.size
    }.reduce(_ + _)
    o / results.count()
  }

  /**
    * NDCG
    * 用户的实际操作行为
    */
  def NDCG(relevanceScore: List[Int]): Double = {
    if (!relevanceScore.isEmpty) {
      val dcgAndList = caluclateDiscountedCumulativeGain(relevanceScore)
      val dcg = dcgAndList._1
      val idcgScores = dcgAndList._2
      val idcg = calucalteIdealDiscountedCumulativeGain(idcgScores)
      dcg / idcg
    } else {
      0.0
    }
  }

  /**
    * IntraSimilarity
    * userId,similarity
    * 所有用户求平均
    */
  def IntraSimilarity1(result:RDD[(Long,Double)]) = {
      val c = result.groupByKey().map(x=>{
        var r = new ArrayBuffer[Double]()
        for(similarity <-x._2){
          r+=similarity
        }
        (x._1,r)
      })
      val IntraListSimilarity =c.map{x=>
        val sim = x._2.map{line=>
          line
        }.reduce(_+_)
        sim*2/x._2.size
      }.reduce(_+_)
    IntraListSimilarity/c.count()
  }
  /**
    * 单个用户的推荐列表列内的所有物品的平均相似度
    *
    * @param similarity
    * @return
    */
  def IntraSimilarity2(similarity: RDD[Double]): Double = {
    val allSimilarity = similarity.map(x => x).reduce(_ + _)
    2 * allSimilarity / similarity.count()
  }
  /**
    * Coverage
    * 每个RDD为一个用户的推荐列表
    * n为推荐数
    */
  def Coverage(result:RDD[List[String]],n:Int): Double = {
    var recommend_items = Set[String]()
    var all_items = Set[String]()
    result.map{ x=>
      var num=0
      x.map{item=>
        if(num<n){
          recommend_items+item
        }
        all_items+item
        num+=1
      }
    }
    1.0*recommend_items.size/all_items.size
  }

  private def caluclateDiscountedCumulativeGain(relevanceScore: List[Int]): (Double, List[Int]) = {
    var rank = 1
    var dcg = 0.0
    val a = relevanceScore.map { act =>
      val c = (Math.pow(2, act) - 1) / (Math.log(1 + rank))
      rank+=1
      dcg += c
      (act,c)
    }.sortBy(x => x._2).reverse.map(x=>x._1)
    (dcg, a)
  }

  private def calucalteIdealDiscountedCumulativeGain(idcgScores: List[Int]): (Double) = {
    var rank = 1
    var dcg = 0.0
    val a = idcgScores.map { act =>
      val c = (Math.pow(2, act) - 1) / (Math.log(1 + rank))
      rank+=1
      c
    }.reduce(_ + _)
    a
  }
  /**
    * 用户喜欢  推荐   不推荐
    * 喜欢    N(tp)   N(fn)
    * 不喜欢   N(fp)   N(tn)
    */
  private def contingencyTable(TP: Int, FP: Int, FN: Int, TN: Int): EvaluationAlgorithm.Table = {
    Table(TP, FP, FN, TN)
  }

  /**
    * 计算混淆矩阵
    *
    * @param labelsTrue
    * @param labelsPred
    * @return
    */
  private def contingencyTable(labelsTrue: Array[Int], labelsPred: Array[Int]): EvaluationAlgorithm.Table = {
    labelChecker(labelsTrue, labelsPred)

    def binomial(x: Int) = if (x < 2) 0 else IntMath.binomial(x, 2)

    val TPAndFP: Int = labelsPred.groupBy(x => x).values.map(x => binomial(x.length)).sum
    val tmp: Map[(Int, Int), Array[(Int, Int)]] = labelsTrue.zip(labelsPred).groupBy(x => x)
    val TP: Int = tmp.values.map(x => binomial(x.length)).sum
    val FP: Int = TPAndFP - TP

    def fun(xs: Array[Int]) = {
      val length: Int = xs.length
      val sums: Array[Int] = xs.tails.slice(1, length).toArray.map(_.sum)
      (xs.init, sums).zipped.map(_ * _).sum
    }

    val FN: Int = tmp.groupBy(_._1._1).mapValues(_.values.map(_.length).toArray).values.map(fun).sum
    val total: Int = binomial(labelsTrue.length)
    val TN: Int = total - TPAndFP - FN
    Table(TP, FP, FN, TN)
  }

  /**
    * 检查标签
    *
    * @param labelsTrue
    * @param labelsPred
    */
  private def labelChecker(labelsTrue: Array[Int], labelsPred: Array[Int]): Unit = {
    require(labelsTrue.length == labelsPred.length && labelsTrue.length >= 2, "The length must be equal!" +
      "The size of labels must be greater than 1!")
  }

  /**
    * 用户喜欢  推荐   不推荐
    * 喜欢    N(tp)   N(tn)
    * 不喜欢   N(fp)   N(fn)
    *
    * @param TP
    * @param FP
    * @param FN
    * @param TN
    */
  private case class Table(TP: Int, FP: Int, FN: Int, TN: Int)

  private def TrapezoidArea(points: Array[(Double, Array[Long])]): Double = {
    val x1 = points(0)._2(0)
    val y1 = points(0)._2(1)
    val x2 = points(1)._2(0)
    val y2 = points(1)._2(1)

    val base = x2 - x1
    val height = (y1 + y2) / 2.0
    return base * height
  }


}
