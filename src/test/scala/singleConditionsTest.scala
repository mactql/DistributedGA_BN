import Config.RedisConfig
import LocalDistributedGATest.inputPath
import Models.ScoreCondition
import Utils.BayesTools
import Utils.MutualInformationUtils.getMapsFromDataWithRedis
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.{Map, Set, mutable}

object singleConditionsTest {

	def main(args: Array[String]): Unit = {

		val tournamentSize:Int = 2
		val scoreJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
		val scoreJedisPipeline:Pipeline = scoreJedis.pipelined()
		//创建sparkContext

		val sc = new SparkSession.Builder().master("local").getOrCreate().sparkContext

		//读取输入数据RDD，最小分区数为48(师兄设置的)
		val textfile:RDD[Array[String]] = sc.textFile(inputPath,48).cache().map(_.split(","))
		val numOfAttribute = textfile.take(1)(0).length
		val valueTypeSet:RDD[(Int,String)] = BayesTools.getNodeValueMap(textfile)
		val valueTpyeSettest = valueTypeSet.collect().toSet

		val broadb = sc.broadcast(valueTpyeSettest)




		val singleConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(line => {
			val xValues:Set[(Int,String)] = broadb.value.filter(_._1==line).flatMap(m => m._2.split(",")).map((line, _)).toSet
			val xsingleConditions = xValues.map(xType => {
				val singleScoreCondition = new ScoreCondition()
				singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
				singleScoreCondition

			})
			xsingleConditions
		}).toSet

		//求每两个节点的所有可能的condition
		val pairConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(x => {
			val xValues:Set[(Int,String)] = broadb.value.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
			val xNeeds:Set[ScoreCondition] = ((x+1) until numOfAttribute).flatMap(y => {
				val yValues:Set[(Int,String)] = broadb.value.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet
				val yNeeds:Set[ScoreCondition] = xValues.flatMap(xType => {
					yValues.map(yType => {
						val singleScoreCondition = new ScoreCondition()
						singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
						singleScoreCondition.addKeyValue(yType._1.toString,yType._2)
						singleScoreCondition
					})
				})
				yNeeds
			}).toSet
			xNeeds
		}).toSet

		val allConditions = singleConditions.union(pairConditions)

		val noparraltime = System.currentTimeMillis()
		//获取求所有取值情况在数据集D中数量的映射关系
		val numOfConditionMap:Map[ScoreCondition, Long] = getMapsFromDataWithRedis(textfile, allConditions, sc, scoreJedisPipeline)

		val noparrel = System.currentTimeMillis()-noparraltime
		scoreJedis.flushAll()

		/**********************************/


		val singleConditionss:RDD[ScoreCondition] = sc.parallelize(Array.range(0,numOfAttribute),48).flatMap(line => {
			val xValues:Set[(Int,String)] = broadb.value.filter(_._1==line).flatMap(m => m._2.split(",")).map((line, _))
			val xsingleConditions:Set[ScoreCondition] = xValues.map(xType => {
				val singleScoreCondition = new ScoreCondition()
				singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
				singleScoreCondition
			})
			xsingleConditions
		})
		//求每两个节点的所有可能的condition
		val pairConditionss:RDD[ScoreCondition] = sc.parallelize(Array.range(0,numOfAttribute),48).flatMap(x => {
			val xValues:Set[(Int,String)] = broadb.value.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
			val xNeeds:Set[ScoreCondition] = ((x+1) until numOfAttribute).flatMap(y => {
				val yValues:Set[(Int,String)] = broadb.value.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet
				val yNeeds:Set[ScoreCondition] = xValues.flatMap(xType => {
					yValues.map(yType => {
						val singleScoreCondition = new ScoreCondition()
						singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
						singleScoreCondition.addKeyValue(yType._1.toString,yType._2)
						singleScoreCondition
					})
				})
				yNeeds
			}).toSet
			xNeeds
		})
		val allconditionss = singleConditionss.union(pairConditionss)

		val paralleltime = System.currentTimeMillis()
		//获取求所有取值情况在数据集D中数量的映射关系
		val numOfConditionMapp:Map[ScoreCondition, Long] = getMapsFromDataWithRedisTest(textfile, allConditions, sc, scoreJedisPipeline)

		val parral = System.currentTimeMillis()-paralleltime
		println("未并行："+ noparrel + "ms")
		println("并行："+ parral + "ms")
		scoreJedis.flushAll()
		scoreJedis.close()
	}

	def getMapsFromDataWithRedisTest(textfile: RDD[Array[String]], allConditions: Set[ScoreCondition], sc:SparkContext, scoreJedisPipeline:Pipeline): Map[ScoreCondition, Long] = {
		if(allConditions.isEmpty) {
			return Map[ScoreCondition, Long]()
		}

		//求所有取值情况的Key
		val allIndexConditions:Set[Set[String]] = allConditions.map(sc => {
			sc.getConditions.keySet
		})

		val broadAllIndexConditions = sc.broadcast(allIndexConditions)

		//计算每种情况在数据集D中的数量RDD
		val conditionCountMapRDD:RDD[(ScoreCondition, Long)] = textfile.mapPartitions(iter => {
			var tmpMap:mutable.Map[ScoreCondition,Long] = mutable.Map[ScoreCondition,Long]()
			iter.foreach(sampleLine=>{
				broadAllIndexConditions.value.foreach(indexCondition => {
					val curScoreCondition = new ScoreCondition()
					indexCondition.foreach(index => curScoreCondition.addKeyValue(index,sampleLine(index.toInt)))
					if(tmpMap.contains(curScoreCondition))
						tmpMap.put(curScoreCondition,tmpMap(curScoreCondition)+1)
					else
						tmpMap += (curScoreCondition->1)
				})
			})
			tmpMap.toIterator
		}).reduceByKey(_+_)


		//对RDD的每个分区，将数量结果，以keyvalue形式通过redis管道存入redis
		conditionCountMapRDD.mapPartitions(conditionCountIter => {
			val savingJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
			val savingJedisPipeline:Pipeline = savingJedis.pipelined()
			conditionCountIter.foreach(conditionCount=>{
				val conditionStr:String = conditionCount._1.toString
				val countStr:String = conditionCount._2.toString
				savingJedisPipeline.setnx(conditionStr, countStr)
			})
			savingJedisPipeline.sync()
			savingJedisPipeline.close()
			Set[String]().iterator
		}).collect()

		val conditionCountMap:mutable.Map[ScoreCondition,Long] = mutable.Map(conditionCountMapRDD.collect().toSeq: _*)

		//存入包括数量为0的情况
		allConditions.foreach(condition => {
			val curConditionStr = condition.toString
			if(conditionCountMap.contains(condition) != true) {
				conditionCountMap += (condition->0)
				scoreJedisPipeline.setnx(curConditionStr, "0")
			}
		})
		scoreJedisPipeline.sync()

		conditionCountMap


	}

}
