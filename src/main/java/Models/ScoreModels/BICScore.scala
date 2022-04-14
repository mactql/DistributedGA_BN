package Models.ScoreModels

import Config.RedisConfig
import Models.{BNStructure, ScoreCondition}
import Utils.BayesTools.joinStringPair
import Utils.BayesTools
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.{Map, Set, mutable}



class BICScore extends java.io.Serializable{

	var numOfSamples:Long = 0
	var numOfAttributes = 0

	def this(initNumOfAttr:Int, dataSet:Array[Array[String]]) = {
		this()
//		BICScore.eachIterCountCalNumFile.initCSV()
		numOfAttributes = initNumOfAttr
		this.numOfSamples = dataSet.length
	}

	def this(initNumOfAttr:Int, dataSet:RDD[Array[String]]) = {
		this()
		//		BICScore.eachIterCountCalNumFile.initCSV()
		numOfAttributes = initNumOfAttr
		this.numOfSamples = dataSet.count()
	}

	//构造计算需要的集合及调用BIC评分计算函数完成种群的评分
	def calculateScore(population: Array[DenseMatrix[Int]], textfile:Array[Array[String]],nodeValueSet:Set[(Int,String)]): Array[BNStructure] = {

		//获取每个节点的ij和ijk的condition，具体解释看函数内部
		val allConditions:Set[ScoreCondition] = this.getAllNeeds(population,nodeValueSet)


		//对每一种condition计算在样本中的数量
		val numOfConditionMap:Map[ScoreCondition, Long] = this.getMapsFromData(textfile, allConditions)

		/*dataMaps.foreach(a =>{
			a._1.needs.foreach(b => print(b._1 + " " + b._2))
			println(" " + a._2)
		})*/

		//用每个BN矩阵+conditionmap+每个节点的取值种类计算BIC评分并构建BN结构
		population.map(BNMatrix=>{
			new BNStructure(BNMatrix, calculate(BNMatrix, numOfConditionMap, nodeValueSet))
		})
	}

	//调用redis中的数据对种群串行评分计算
	def calculateScoreSerialWithRedis(population: Array[DenseMatrix[Int]], textfile:Array[Array[String]],nodeValueSet:Set[(Int,String)],ScoreJedisPipeline:Pipeline): Array[BNStructure] ={
		//获取每个节点的ij和ijk的condition，具体解释看函数内部
		val allConditions:Set[ScoreCondition] = this.getAllNeeds(population,nodeValueSet)

		//从pipeline中取出所有取值情况的在D中的数量，若不在redis中则返回-1
		val dataMap:mutable.Map[ScoreCondition, Long] = getAllConditionsMap(allConditions, ScoreJedisPipeline)

		//得到不在redis中仍然需要计算的取值情况
		val stillNeeds:Set[ScoreCondition] = dataMap.filter(condition => condition._2 == -1).keys.toSet

		//得到仍然需要计算的取值情况与其D中数量的映射
		val stillNeedsMap:Map[ScoreCondition, Long] = this.getMapsFromData(textfile, stillNeeds)

		val savingJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
		val savingJedisPipeline:Pipeline = savingJedis.pipelined()
		stillNeedsMap.foreach(conditionCount => {
			val conditionStr:String = conditionCount._1.toString
			val countStr:String = conditionCount._2.toString
			savingJedisPipeline.setnx(conditionStr, countStr)
		})
		savingJedisPipeline.sync()
		savingJedis.close()

		//将仍需要计算的映射和已计算的映射合并成所有情况的映射关系
		stillNeedsMap.foreach(eachMap=>{
			dataMap(eachMap._1) = eachMap._2
		})

		//用每个BN矩阵+datamap+每个节点的取值种类计算BIC评分并构建BN结构
		population.map(BNMatrix=>{
			new BNStructure(BNMatrix, calculate(BNMatrix, dataMap, nodeValueSet))
		})

	}



	//调用redis中的数据对种群并行评分计算
	def calculateScoreParallelWithRedis(population: RDD[BNStructure],textfile:RDD[Array[String]],broadValueTpye:Broadcast[Set[(Int,String)]],sc:SparkContext,ScoreJedisPipeline:Pipeline):Array[BNStructure] = {
		//获取每个节点的ij和ijk的condition，具体解释看函数内部
		val allConditions:Set[ScoreCondition] = this.getAllNeeds(population.map(_.structure).collect(),broadValueTpye.value)

		//从pipeline中取出所有取值情况的在D中的数量，若不在redis中则返回-1
		val dataMap:mutable.Map[ScoreCondition, Long] = getAllConditionsMap(allConditions, ScoreJedisPipeline)

		//得到不在redis中仍然需要计算的取值情况
		val stillNeeds:Set[ScoreCondition] = dataMap.filter(condition => condition._2 == -1).keys.toSet



		//得到仍然需要计算的取值情况与其D中数量的映射
		val stillNeedsMap:Map[ScoreCondition,Long] = getStillMapsFromDataWithRedis(textfile,stillNeeds,sc,ScoreJedisPipeline)

		//将仍需要计算的映射和已计算的映射合并成所有情况的映射关系
		stillNeedsMap.foreach(eachMap=>{
			dataMap(eachMap._1) = eachMap._2
		})




		//广播所有情况与D中数量的映射关系
		val broadDataMap:Broadcast[Map[ScoreCondition,Long]] = sc.broadcast(dataMap.toMap)

		//对每个BN结构进行评分计算
		var finalBNPopulations:Array[BNStructure] = population.mapPartitions(iter=>{
			val tempBNstructureSet:mutable.Set[BNStructure] = mutable.Set[BNStructure]()
			iter.foreach(eachBN=>{
				tempBNstructureSet.add(new BNStructure(eachBN.structure,calculate(eachBN.structure,broadDataMap.value,broadValueTpye.value)))
			})
			tempBNstructureSet.iterator
		}).collect()
		broadDataMap.destroy()
		finalBNPopulations
	}



	/*
		获取需要的condition的集合
		包含每个节点的{父节点取值condition + 父节点取值condition时自身节点取值的所有可能的联合condition}
	 */
	def getAllNeeds(population: Array[DenseMatrix[Int]], nodeValueSet: Set[(Int, String)]): Set[ScoreCondition] = {
		var allNeeds = mutable.Set[ScoreCondition]()
		val eachConditions:Array[Set[ScoreCondition]] = population.map(BNMatrix=>{
			this.getNodeNeeds(BNMatrix,nodeValueSet)
		})
		eachConditions.foreach(condition=>{
			allNeeds ++= condition
		})
		allNeeds.toSet
	}

	//获取单个节点需要的condition的集合
	def getNodeNeeds(BNMatrix: DenseMatrix[Int], nodeValueSet: Set[(Int, String)]): Set[ScoreCondition] = {
		var allMaps = mutable.Set[ScoreCondition]()

		/*var arr:Array[Int] = BNMatrix.toArray

		arr.foreach(a =>print(a + " "))*/

		for(node <- 0 until numOfAttributes){
			val parentsOfNode = BayesTools.getParentSet(BNMatrix,node,numOfAttributes)
			// i是自己节点的所有取值情况集合
			val i:mutable.Set[ScoreCondition] = nodeValueSet.filter(x=>x._1==node).aggregate(mutable.Set[ScoreCondition]())(BayesTools.joinStringPair,ScoreCondition.cartesianTwoSet)
			// ij是自己节点的父节点的所有取值情况集合
			val ij:mutable.Set[ScoreCondition] = nodeValueSet.filter(x=>parentsOfNode.contains(x._1)).aggregate(mutable.Set[ScoreCondition]())(BayesTools.joinStringPair,ScoreCondition.cartesianTwoSet)
			//ijk是自己节点的取值匹配父节点取值的所有可能的取值情况集合
			val ijk:mutable.Set[ScoreCondition] = ScoreCondition.cartesianTwoSet(i,ij)
			allMaps = allMaps ++ ij ++ ijk


			/*println("第一次")
			println("node：" + node + " 的数据")
			println("i的数据")
			i.foreach(_.needs.foreach(p =>println(p._1 + " " + p._2)))*/
			/*println("ij的数据")
			ij.foreach(a =>{ println("下一个condition")
				a.conditions.foreach(p =>println(p._1 + " " + p._2))})*/
			/*println("ijk的数据")
			ijk.foreach(_.needs.foreach(p =>println(p._1 + " " + p._2)))
			println("结束")*/
		}
		allMaps.toSet
	}

	//获取该节点的所有的父节点condition
	def getNeeds_MIJ(BNMatrix:DenseMatrix[Int], node:Int, nodeValueSet:Set[(Int,String)]):Set[ScoreCondition] ={
		var allMaps = mutable.Set[ScoreCondition]()
		val parentOfNode = BayesTools.getParentSet(BNMatrix,node,this.numOfAttributes)
		//val i:mutable.Set[ScoreCondition] = nodeValueSet.filter(x=>x._1==node).aggregate(mutable.Set[ScoreCondition]())(joinStringPair,ScoreCondition.cartesianTwoSet)
		val ij:mutable.Set[ScoreCondition] = nodeValueSet.filter(x=>parentOfNode.contains(x._1)).aggregate(mutable.Set[ScoreCondition]())(joinStringPair,ScoreCondition.cartesianTwoSet)
		//val ijk = cartesianTwoSet(i,ij)
		allMaps = allMaps ++ ij// ++ ijk
		allMaps.toSet
	}
	
	//获取一个map，其中key为condition，value为这个condition在样本中的数量
	def getMapsFromData(textfile: Array[Array[String]], allNeeds: Set[ScoreCondition]): Map[ScoreCondition, Long] = {
		if(allNeeds.isEmpty) {
			return Map[ScoreCondition, Long]()
		}
		//创建一个取值condition为key，相同condition数量为value的键值对Map，取值情况包括父节点取值condition+自身节点取值与父节点取值的所有可能即师兄论文中mijk的condition
		val actualNeedsMap:mutable.Map[ScoreCondition, Long] = mutable.Map(allNeeds.map(condition => (condition, 0.toLong)).toSeq: _*)

		//	在allNeeds中过滤出在样本中的condition数量
		textfile.foreach(dataline=> {
			allNeeds.filter(condition => {
				condition.matchData(dataline)
			}).foreach(condition => {
				actualNeedsMap(condition) = actualNeedsMap(condition) + 1
			})
		})
		actualNeedsMap
	}

	//获取仍然需要计算的数据，其中key为condition，value为这个condition在样本中的数量,并用redis存储数据复用
	def getStillMapsFromDataWithRedis(textfile: RDD[Array[String]], stillNeeds: Set[ScoreCondition], sc:SparkContext,scoreJedisPipeline:Pipeline): Map[ScoreCondition, Long] = {
		if(stillNeeds.isEmpty) {
			return Map[ScoreCondition, Long]()
		}

		//计算出仍然需要计算的取值情况的节点集合 如[1,2,3]，[1,3,4],[1,2,3,4] 并广播
		val stillNeedsKeys:Set[Set[String]] = stillNeeds.map(condition => {
			condition.conditions.keySet
		})

		val broadStillNeedsKeys = sc.broadcast(stillNeedsKeys)

		/*
			先把stillNeeds的所有节点组合统计出来，组成Set[Set[String]]  ，如[1,2,3],[1,2,3,4],[2,3,4]
			记为stillNeedsKeys并广播。
			然后直接用统计出来的节点组合在每行数据中取出，组合成取值情况，若在map中则+1
		 */

		val stillMapsRDD:RDD[(ScoreCondition,Long)] = textfile.mapPartitions(iter=>{
			var tempMap:mutable.Map[ScoreCondition,Long] = mutable.Map[ScoreCondition,Long]()
			iter.foreach(sampleLine=>{
				broadStillNeedsKeys.value.foreach(condition => {
					val curScoreCondition = new ScoreCondition()
					condition.foreach(indexStr => curScoreCondition.addKeyValue(indexStr,sampleLine(indexStr.toInt)))
					if(tempMap.contains(curScoreCondition)) tempMap.put(curScoreCondition,tempMap(curScoreCondition)+1)
					else tempMap += (curScoreCondition->1)
				})
			})
			tempMap.toIterator
		}).reduceByKey(_+_)

		/*
			将数据保存在redis中
		 */
		stillMapsRDD.mapPartitions(iter=>{
			val savingJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
			val savingJedisPipeline:Pipeline = savingJedis.pipelined()
			iter.foreach(conditionMap=>{
				val conditionStr:String = conditionMap._1.toString
				val countStr:String = conditionMap._2.toString
				savingJedisPipeline.setnx(conditionStr, countStr)
			})
			savingJedisPipeline.sync()
			savingJedis.close()
			Set[String]().iterator
		}).collect()

		stillNeeds.foreach(condition=>{
			scoreJedisPipeline.setnx(condition.toString,"0")
		})
		scoreJedisPipeline.sync()

		val stillNeedsArr:Array[ScoreCondition] = stillNeeds.toArray
		stillNeedsArr.foreach(condition=>{
			scoreJedisPipeline.get(condition.toString)
		})

		val stillNeedsCount:Array[Long] = scoreJedisPipeline.syncAndReturnAll().toArray.map(_.toString.toLong)
		broadStillNeedsKeys.destroy()
		stillNeedsArr.zip(stillNeedsCount).toMap
	}



	/*
		用公式计算评分
		dataMaps：计算的每个condition的所有元素都不在样本中的数量
		nodeValueSet：每个节点的取值种类，如0 novisit，visit
		q_i表示该节点的父节点的取值condition的数量
		r_i表示该节点的取值condition数量
		m_ijk是该节点取第k个condition且父节点取第j个condition的样本数目
		m_ij是该节点的父节点取第j个condition的样本数目
	 */
	def calculate(BNMatrix:DenseMatrix[Int],dataMaps:Map[ScoreCondition,Long], nodeValueSet:Set[(Int,String)]):Double = {
		var bicScore:Double = 0.0

		for(i <- 0 until numOfAttributes){ //遍历每个节点 从i到n
			var LL = 0.0

			//找到该节点的所有父节点index集合
			val parentSet = BayesTools.getParentSet(BNMatrix,i,numOfAttributes)
			//把nodeValueSet滤出当前节点的键值对并拆成多个键值对，例、如<0 "novisit,visit">拆成 <0,novisit>和<0,visit>
			val splitedNodeValueSet:Set[(Int,String)] = nodeValueSet.filter(_._1==i).flatMap(m=>{
				var ans:mutable.Set[(Int,String)] = mutable.Set[(Int,String)]()
				m._2.split(",").foreach(v=>{
					ans += Tuple2(i,v)
				})
				ans.toSet
			})

			if(parentSet.isEmpty){ //如果没有父节点
				val q_i = 1  //父节点condition种数
				val r_i = splitedNodeValueSet.size //自身节点condition种数

				/*	mijk = 节点i取第k个值时的样本数量
					mij = 样本数量
				 */

				val tmp = splitedNodeValueSet.map(k=>{

					val singleScoreCondition:ScoreCondition = new ScoreCondition()
					singleScoreCondition.addKeyValue(k._1.toString,k._2)

					val m_ijk = dataMaps(singleScoreCondition)
					val m_ij:Double = numOfSamples.doubleValue()
					if(m_ijk == 0) {
						0
					} else {
						m_ijk*Math.log(m_ijk/m_ij)
					}
				}).sum
				LL += tmp
				LL -= Math.log(numOfSamples)*q_i*(r_i-1)/2 //减去参数总数
			}
			else{//若有父节点

				//计算该节点的父节点condition种数，即q_i
				var q_i = 1
				parentSet.toArray.map(paNode => {
					nodeValueSet.filter(_._1 == paNode).flatMap(_._2.split(",")).size
				}).foreach(numOfType => q_i = q_i * numOfType)

				//该节点的取值condition种数
				val r_i = splitedNodeValueSet.size

				/*	获取MIJ需要的集合，即该该节点的父节点condition取值
					以0节点，1和2是其父节点为例，每一行是一个condition
					   1 c 2 e
					   1 c 2 f
					   1 d 2 e
					   1 d 2 f
				 */
				val dataMapNodeI_MIJ:Set[ScoreCondition] = this.getNeeds_MIJ(BNMatrix, i, nodeValueSet)

				//遍历每一个父节点condition，从j=1到j=q_i 计算BIC
				LL += dataMapNodeI_MIJ.flatMap(condition=>{

					//m_ij = 节点i的父节点取第j个值时的样本数量
					val m_ij:Double = dataMaps(condition)

					splitedNodeValueSet.map(index_type=>{ //遍历i节点的每一种取值 从k到r_i

						//将i节点的取值联合父节点的取值，得到i节点取第k个值时其父节点取第j个值的样本数量
						val ijk = condition.tmpAddKeyValue(index_type._1.toString,index_type._2)
						val m_ijk = dataMaps(ijk)

						if(m_ijk == 0) {
							0
						} else {
							m_ijk*Math.log(m_ijk/m_ij)
						}
					})
				}).sum
				LL -= Math.log(numOfSamples)*q_i*(r_i-1)/2
			}
			bicScore += LL
		}
		bicScore
	}

	//从pipeline中取出所有取值情况的在D中的数量
	def getAllConditionsMap(allConditions:Set[ScoreCondition], scoreJedisPipeline:Pipeline):mutable.Map[ScoreCondition, Long] = {

		val allConditionsArray:Array[ScoreCondition] = allConditions.toArray
		allConditionsArray.foreach(need => {
			scoreJedisPipeline.get(need.toString)
		})

		mutable.Map(allConditionsArray.zip(scoreJedisPipeline.syncAndReturnAll().toArray().map(fetchedValue => {
			if(fetchedValue == null) -1
			else fetchedValue.toString.toLong
		})).toMap.toSeq: _*)
	}

}
