package Utils

import Config.RedisConfig
import Models.ScoreCondition
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.{Map, Set, mutable}

object MutualInformationUtils extends java.io.Serializable {

	def getMutualInfoMatrixSerial(textfile:Array[Array[String]], numOfAttribute:Int, nodeValueType:Set[(Int,String)], scoreJedisPipeline:Pipeline, sc:SparkContext, numOfSamples:Long):DenseMatrix[Double] = {
		//求每一个节点的所有可能的condition
		val singleConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(line => {
			val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==line).flatMap(m => m._2.split(",")).map((line, _)).toSet
			val xsingleConditions = xValues.map(xType => {
				val singleScoreCondition = new ScoreCondition()
				singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
				singleScoreCondition

			})
			xsingleConditions
		}).toSet

		//求每两个节点的所有可能的condition
		val pairConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(x => {
			val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
			val xNeeds:Set[ScoreCondition] = ((x+1) until numOfAttribute).flatMap(y => {
				val yValues:Set[(Int,String)] = nodeValueType.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet
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

		//单节点和双节点所有可能的condition
		val allConditions:Set[ScoreCondition] = singleConditions.union(pairConditions)
		//获取求所有取值情况在数据集D中数量的映射关系
		val numOfConditionMap:Map[ScoreCondition, Long] = getMapsFromDataSerialWithRedis(textfile, allConditions,sc,scoreJedisPipeline)

		/*numOfConditionMap.foreach(a=>{
			val s:ScoreCondition = a._1
			s.conditions.foreach(b=>{
				print(b._1 + " " + b._2 + " ")
			})
			println(a._2)
		})*/

		//求互信息矩阵
		val mutualInformationMatrix:DenseMatrix[Double] = DenseMatrix.zeros(numOfAttribute, numOfAttribute)

		/*
			通过互信息计算公式，两两节点计算出互信息矩阵
			对每对节点x,y 求p(x,y) * log[ p(x,y) / ( p(x)* p(y) ) ]
			其中p(x,y)是D中满足xi=X和yi=Y的样本数量 / 样本数量；p(x)是D中满足xi=X的样本数量 / 样本数量
		 */
		(0 until numOfAttribute).foreach(x => {
			(x+1 until numOfAttribute).foreach(y => {
				//x的所有取值情况，y的所有取值情况
				val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
				val yValues:Set[(Int,String)] = nodeValueType.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet

				val mutualInformation:Double = yValues.map(yValue => {
					val curYValue = yValue._2
					xValues.map(xValue => {
						var curAnsValue:Double = 0
						val curXValue = xValue._2
						val xCondition = new ScoreCondition()
						xCondition.addKeyValue(xValue._1.toString, curXValue)
						val yCondition = new ScoreCondition()
						yCondition.addKeyValue(yValue._1.toString, curYValue)
						if(numOfConditionMap(xCondition) == 0 || numOfConditionMap(yCondition) == 0) {
							curAnsValue = 0
						} else {
							val xyCondition = new ScoreCondition()
							xyCondition.addKeyValue(xValue._1.toString, curXValue)
							xyCondition.addKeyValue(yValue._1.toString, curYValue)
							var p_xy:Double = 0
							if(numOfConditionMap(xyCondition) == 0) {
								curAnsValue = 0
							}
							else {
								p_xy = 1.0 * numOfConditionMap(xyCondition) / numOfSamples
								val p_x = 1.0 * numOfConditionMap(xCondition) / numOfSamples
								val p_y = 1.0 * numOfConditionMap(yCondition) / numOfSamples
								curAnsValue = p_xy * Math.log(p_xy / (p_x * p_y))
							}
						}
						curAnsValue
					}).sum
				}).sum
				mutualInformationMatrix.update(x, y, mutualInformation)
				mutualInformationMatrix.update(y, x, mutualInformation)
			})
		})
		mutualInformationMatrix
	}


	def getMutualInfoMatrix(textfile:RDD[Array[String]], numOfAttribute:Int, nodeValueType:Set[(Int,String)], scoreJedisPipeline:Pipeline, sc:SparkContext, numOfSamples:Long):DenseMatrix[Double] = {
		//求每一个节点的所有可能的condition
		val singleConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(line => {
			val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==line).flatMap(m => m._2.split(",")).map((line, _)).toSet
			val xsingleConditions = xValues.map(xType => {
				val singleScoreCondition = new ScoreCondition()
				singleScoreCondition.addKeyValue(xType._1.toString,xType._2)
				singleScoreCondition

			})
			xsingleConditions
		}).toSet

		//求每两个节点的所有可能的condition
		val pairConditions:Set[ScoreCondition] = (0 until numOfAttribute).flatMap(x => {
			val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
			val xNeeds:Set[ScoreCondition] = ((x+1) until numOfAttribute).flatMap(y => {
				val yValues:Set[(Int,String)] = nodeValueType.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet
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

		//单节点和双节点所有可能的condition
		val allConditions:Set[ScoreCondition] = singleConditions.union(pairConditions)
		//获取求所有取值情况在数据集D中数量的映射关系
		val numOfConditionMap:Map[ScoreCondition, Long] = getMapsFromDataWithRedis(textfile, allConditions, sc, scoreJedisPipeline)

		/*numOfConditionMap.foreach(a=>{
			val s:ScoreCondition = a._1
			s.conditions.foreach(b=>{
				print(b._1 + " " + b._2 + " ")
			})
			println(a._2)
		})*/

		//求互信息矩阵
		val mutualInformationMatrix:DenseMatrix[Double] = DenseMatrix.zeros(numOfAttribute, numOfAttribute)

		/*
			通过互信息计算公式，两两节点计算出互信息矩阵
			对每对节点x,y 求p(x,y) * log[ p(x,y) / ( p(x)* p(y) ) ]
			其中p(x,y)是D中满足xi=X和yi=Y的样本数量 / 样本数量；p(x)是D中满足xi=X的样本数量 / 样本数量
		 */
		(0 until numOfAttribute).foreach(x => {
			(x+1 until numOfAttribute).foreach(y => {
				//x的所有取值情况，y的所有取值情况
				val xValues:Set[(Int,String)] = nodeValueType.filter(_._1==x).flatMap(m => m._2.split(",")).map((x, _)).toSet
				val yValues:Set[(Int,String)] = nodeValueType.filter(_._1==y).flatMap(m => m._2.split(",")).map((y, _)).toSet

				val mutualInformation:Double = yValues.map(yValue => {
					val curYValue = yValue._2
					xValues.map(xValue => {
						var curAnsValue:Double = 0
						val curXValue = xValue._2
						val xCondition = new ScoreCondition()
						xCondition.addKeyValue(xValue._1.toString, curXValue)
						val yCondition = new ScoreCondition()
						yCondition.addKeyValue(yValue._1.toString, curYValue)
						if(numOfConditionMap(xCondition) == 0 || numOfConditionMap(yCondition) == 0) {
							curAnsValue = 0
						} else {
							val xyCondition = new ScoreCondition()
							xyCondition.addKeyValue(xValue._1.toString, curXValue)
							xyCondition.addKeyValue(yValue._1.toString, curYValue)
							var p_xy:Double = 0
							if(numOfConditionMap(xyCondition) == 0) {
								curAnsValue = 0
							}
							else {
								p_xy = 1.0 * numOfConditionMap(xyCondition) / numOfSamples
								val p_x = 1.0 * numOfConditionMap(xCondition) / numOfSamples
								val p_y = 1.0 * numOfConditionMap(yCondition) / numOfSamples
								curAnsValue = p_xy * Math.log(p_xy / (p_x * p_y))
							}
						}
						curAnsValue
					}).sum
				}).sum
				mutualInformationMatrix.update(x, y, mutualInformation)
				mutualInformationMatrix.update(y, x, mutualInformation)
			})
		})
		mutualInformationMatrix
	}

	def getMapsFromDataSerial(textfile: Array[Array[String]], allConditions: Set[ScoreCondition]) :Map[ScoreCondition,Long]={
		val actualNeedsMap:mutable.Map[ScoreCondition, Long] = mutable.Map(allConditions.map(condition => (condition, 0.toLong)).toSeq: _*)
		textfile.foreach(sampleArrayList=> {
			allConditions.filter(condition => {
				condition.matchData(sampleArrayList)
			}).foreach(condition => {
				actualNeedsMap(condition) = actualNeedsMap(condition) + 1
			})
		})
		actualNeedsMap
	}

	def getMapsFromDataSerialWithRedis(textfile: Array[Array[String]], allConditions: Set[ScoreCondition], sc:SparkContext,scoreJedisPipeline:Pipeline) :Map[ScoreCondition,Long]={
		if(allConditions.isEmpty) {
			return Map[ScoreCondition, Long]()
		}

		//求所有取值情况的Key
		val allIndexConditions:Set[Set[String]] = allConditions.map(sc => {
			sc.getConditions.keySet
		})

		val broadAllIndexConditions = sc.broadcast(allIndexConditions)

		val conditionCountMap:mutable.Map[ScoreCondition, Long] = mutable.Map()


		//计算每种情况在数据集D中的数量RDD
		textfile.foreach(sampleLine => {
			broadAllIndexConditions.value.foreach(indexCondition => {
				val curScoreCondition = new ScoreCondition()
				indexCondition.foreach(index => curScoreCondition.addKeyValue(index,sampleLine(index.toInt)))
				if(conditionCountMap.contains(curScoreCondition))
					conditionCountMap.put(curScoreCondition,conditionCountMap(curScoreCondition)+1)
				else
					conditionCountMap += (curScoreCondition->1)
			})
		})

		//对RDD的每个分区，将数量结果，以keyvalue形式通过redis管道存入redis
		conditionCountMap.foreach(conditionCount => {
			val conditionStr:String = conditionCount._1.toString
			val countStr:String = conditionCount._2.toString
			scoreJedisPipeline.setnx(conditionStr, countStr)
		})
		scoreJedisPipeline.sync()

		//存入包括数量为0的情况(可能是condition不在数据集D中)
		allConditions.foreach(condition => {
			val curConditionStr = condition.toString
			scoreJedisPipeline.setnx(curConditionStr, "0")
		})
		scoreJedisPipeline.sync()

		/*
			将取值情况和在D中数量的映射关系从redis中取出，作为map返回
		 */
		val allConditionsArr:Array[ScoreCondition] = allConditions.toArray
		allConditionsArr.foreach(condition => {
			val conditionStr = condition.toString
			scoreJedisPipeline.get(conditionStr)
		})
		val allConditionsCount:Array[Long] = scoreJedisPipeline.syncAndReturnAll().toArray().map(_.toString.toLong)
		broadAllIndexConditions.destroy()
		allConditionsArr.zip(allConditionsCount).toMap

	}

	def getMapsFromDataWithRedis(textfile: RDD[Array[String]], allConditions: Set[ScoreCondition], sc:SparkContext, scoreJedisPipeline:Pipeline): Map[ScoreCondition, Long] = {
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

		//存入包括数量为0的情况(可能是condition不在数据集D中)
		allConditions.foreach(condition => {
			val curConditionStr = condition.toString
			scoreJedisPipeline.setnx(curConditionStr, "0")
		})
		scoreJedisPipeline.sync()

		/*
			将取值情况和在D中数量的映射关系从redis中取出，作为map返回
		 */
		val allConditionsArr:Array[ScoreCondition] = allConditions.toArray
		allConditionsArr.foreach(condition => {
			val conditionStr = condition.toString
			scoreJedisPipeline.get(conditionStr)
		})
		val allConditionsCount:Array[Long] = scoreJedisPipeline.syncAndReturnAll().toArray().map(_.toString.toLong)
		broadAllIndexConditions.destroy()
		allConditionsArr.zip(allConditionsCount).toMap

	}
	
}
