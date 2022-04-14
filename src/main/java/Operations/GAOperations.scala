package Operations


import Experiences.DistributedGA.maxParent
import Models.BNStructure
import Utils.BayesTools.{getEdgeDirectType, setEdgeDirectType}
import Utils.CycleUtils
import breeze.linalg.DenseMatrix
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Random

object GAOperations {

	val crossoverRate = 0.5

	//初始化种群
	def initPopulationAllWithRemoveCycle(numOfPopulation:Int, numOfAttributes:Int, sc:SparkContext):Array[DenseMatrix[Int]] = {
		//n个变量的BN结构可以用n*n的邻接矩阵表示，aij=1则表示i是j的父节点
		val populationArray:Array[DenseMatrix[Int]] = (0 until numOfPopulation).toArray.map(i => {
			var tmpBN:DenseMatrix[Int] = DenseMatrix.zeros(numOfAttributes,numOfAttributes)
			(0 until numOfAttributes-1).foreach(x => {
				((x+1) until numOfAttributes).foreach(y => {
					val edgeType = Random.nextInt(3)
					tmpBN = setEdgeDirectType(tmpBN, x, y, edgeType)
				})
			})

			var validBN = CycleUtils.removeCycleWithDegree(tmpBN, numOfAttributes)
			validBN = CycleUtils.limitParentNaive(validBN, numOfAttributes, maxParent)
			validBN

		})
		populationArray
	}
	
	//用超结构初始化种群
	def initPopulationAllWithRemoveCycleAndSS(numOfPopulation:Int, numOfAttributes:Int, sc:SparkContext,SS:Broadcast[DenseMatrix[Int]]):RDD[DenseMatrix[Int]] = {
		sc.parallelize(Array.range(0, numOfPopulation)).map(i => {
			var tmpBN:DenseMatrix[Int] = DenseMatrix.zeros(numOfAttributes, numOfAttributes)
			(0 until numOfAttributes-1).foreach(x => {
				((x+1) until numOfAttributes).foreach(y => {
					if(SS.value(x, y) == 1) {
						val edgeType = Random.nextInt(3)
						tmpBN = setEdgeDirectType(tmpBN, x, y, edgeType)
					}
				})
			})
			var next = CycleUtils.removeCycleWithDegree(tmpBN, numOfAttributes)
			next = CycleUtils.limitParentNaive(next, numOfAttributes, maxParent)
			next
		})
	}

	def initPopulationAllWithRemoveCycleAndSSSerial(numOfPopulation:Int, numOfAttributes:Int, sc:SparkContext,SS:DenseMatrix[Int]) : Array[DenseMatrix[Int]] = {
		Array.range(0, numOfPopulation).map(i => {
			var tmpBN:DenseMatrix[Int] = DenseMatrix.zeros(numOfAttributes, numOfAttributes)
			(0 until numOfAttributes-1).foreach(x => {
				((x+1) until numOfAttributes).foreach(y => {
					if(SS(x, y) == 1) {
						val edgeType = Random.nextInt(3)
						tmpBN = setEdgeDirectType(tmpBN, x, y, edgeType)
					}
				})
			})
			var next = CycleUtils.removeCycleWithDegree(tmpBN, numOfAttributes)
			next = CycleUtils.limitParentNaive(next, numOfAttributes, maxParent)
			next
		})
	}
	

	//获取种群中评分最高的个体，更新历史最优个体
	def getEliteIndividual(populations:Array[BNStructure],bestBN:BNStructure):BNStructure = {
		var curBestBN = new BNStructure(bestBN.structure.copy,bestBN.score)
		populations.foreach(individual =>{
			if(individual.score > curBestBN.score){
				curBestBN.structure = individual.structure.copy
				curBestBN.score = individual.score
			}
		})
		curBestBN
	}
	
	//找出种群中评分最差的劣质个体并用精英个体替换
	def replaceLowestWithElite(populations:Array[BNStructure], elite:BNStructure):Array[BNStructure] = {
		var minIndex:Int = -1
		var minScore:Double = Double.MaxValue
		populations.zipWithIndex.foreach(m => {
			if(m._1.score < minScore) {
				minScore = m._1.score
				minIndex = m._2
			}
		})
		populations(minIndex) = elite
		populations
	}
	
	
	/*	二元锦标赛算子
		每次从种群中随机选择两个个体，根据下标找到并组成锦标赛BN数组(tournamentBNArray)
		找出锦标赛BN数组中评分最高的个体放入下一代
		循环前两步，直到下一代达到种群规模
	 */
	def tournamentSelection(doublePopulation: Array[BNStructure], tournamentSize:Int, numOfPopulation:Int, sc:SparkContext):Array[BNStructure] = {
		val nextPopulations:Array[BNStructure] = new Array[BNStructure](numOfPopulation)
		val curPopulationSize:Int = doublePopulation.length
		for (populationIndex <- 0 until numOfPopulation) {
			/* 师兄代码
			//在种群中随机取两个个体下标，并根据下标构成锦标赛BN数组
			val tournamentIndexArr:Array[Int] = (0 until tournamentSize).map(i => {Random.nextInt(curPopulationSize)}).toArray
			val tournamentBNArray:Array[BNStructure] = tournamentIndexArr.map(doublePopulation(_))
			//找到评分最高的个体
			val maxScore:Double = tournamentBNArray.map(_.score).max
			val winner:BNStructure = tournamentBNArray.find(_.score == maxScore).orNull
			val copyWinner:BNStructure = new BNStructure(winner.structure.copy, winner.score)
			nextPopulations(populationIndex) = copyWinner
			*/


			val tournamentA = doublePopulation(Random.nextInt((curPopulationSize)))
			val tournamentB = doublePopulation(Random.nextInt((curPopulationSize)))
			var winner:BNStructure = null
			if(tournamentA.score > tournamentB.score)
				winner = new BNStructure(tournamentA.structure.copy,tournamentA.score)
			else
				winner = new BNStructure(tournamentB.structure.copy,tournamentB.score)
			nextPopulations(populationIndex) = winner
		}
		nextPopulations
	}

	
	//调用均匀交叉，得到两百条染色题的population：前100条染色体为锦标赛选择出来的种群，后100条为前100条经过均匀交叉后的种群
	def uniformCrossoverAll(populations:Array[BNStructure], numOfPopulation:Int, numOfAttribute:Int,sc:SparkContext):Array[BNStructure] = {
		val nextPopulations:Array[BNStructure] = new Array[BNStructure](numOfPopulation)
		populations.zipWithIndex.foreach(pi => {
			val index = pi._2
			val p:DenseMatrix[Int] = pi._1.structure
			var tempIndex = Random.nextInt(populations.length)
			while (tempIndex == index) {
				tempIndex = Random.nextInt(populations.length)
			}
			val tempP:DenseMatrix[Int] = populations(tempIndex).structure
			nextPopulations(index) = new BNStructure(uniformCrossover(p, tempP,numOfAttribute))
		})
		val population = populations++nextPopulations
		population
	}
	//对两条染色体均匀交叉+去环+限制父节点数量
	private def uniformCrossover(base: DenseMatrix[Int], other: DenseMatrix[Int],numOfAttribute:Int):DenseMatrix[Int] = {
		var next:DenseMatrix[Int] = base.copy
		(0 until numOfAttribute).foreach(x => {
			((x+1) until numOfAttribute).foreach(y => {
				val useBase:Boolean = Random.nextDouble() <= crossoverRate
				if (!useBase) {
					next.update(x, y, base(x, y))
					next.update(y, x, base(y, x))
				} else {
					next.update(x, y, other(x, y))
					next.update(y, x, other(y, x))
				}
			})
		})
		next = CycleUtils.removeCycleWithDegree(next, numOfAttribute)
		next = CycleUtils.limitParentNaive(next, numOfAttribute, maxParent)
		next
	}

	
	//调用单点突变 得到200条突变后的染色体
	def singlePointMutationAll(populations:Array[BNStructure],numOfAttribute:Int):Array[BNStructure] = {
		val nextPopulation = populations.map(p =>{
			new BNStructure(singlePointMutation(p.structure,numOfAttribute))
		})
		nextPopulation
	}
	//对一条染色体单点突变+去环+限制父节点数量
	private def singlePointMutation(base: DenseMatrix[Int],numOfAttribute:Int):DenseMatrix[Int] = {
		var next:DenseMatrix[Int] = base.copy
		val rate:Double = 1.0 / (numOfAttribute * (numOfAttribute - 1) / 2)

		//对每一条边进行突变
		(0 until numOfAttribute).foreach(x => {
			((x+1) until numOfAttribute).foreach(y => {
				val originType:Int = getEdgeDirectType(base, x, y)
				if(rate >= Random.nextDouble()) {
					val newType:Int = (originType + Random.nextInt(2) + 1) % 3
					next = setEdgeDirectType(next, x, y, newType)
				}
			})
		})
		next = CycleUtils.removeCycleWithDegree(next, numOfAttribute)
		next = CycleUtils.limitParentNaive(next, numOfAttribute, maxParent)
		next
	}

	//对一条染色体基于SS单点突变+去环+限制父节点数量，若SS中没有这条边则不进行突变操作
	def singlePointMutationWithSS(base: DenseMatrix[Int],ss:Broadcast[DenseMatrix[Int]],numOfAttribute:Int):DenseMatrix[Int] = {
		var next:DenseMatrix[Int] = base.copy
		val rate:Double = 1.0 / (numOfAttribute * (numOfAttribute - 1) / 2)
		//对每一条边进行突变
		(0 until numOfAttribute).foreach(x => {
			((x+1) until numOfAttribute).foreach(y => {
				if(ss.value(x,y) == 1){
					val originType:Int = getEdgeDirectType(base, x, y)
					if(rate >= Random.nextDouble()) {
						val newType:Int = (originType + Random.nextInt(2) + 1) % 3
						next = setEdgeDirectType(next, x, y, newType)
					}
				}
			})
		})
		next = CycleUtils.removeCycleWithDegree(next, numOfAttribute)
		next = CycleUtils.limitParentNaive(next, numOfAttribute, maxParent)
		next
	}


	def tournamentSelectionAndUniformCrossover(broadPopulation:Broadcast[Array[BNStructure]],tournamentSize:Int,numOfPopulation:Int,sc:SparkContext,numOfAttributes:Int):RDD[BNStructure] = {
		//获取当前种群数量，numOfPopulation*2
		val curSize:Int = broadPopulation.value.length

		//构造选择信息数组，其中包含numOfPopulation个锦标赛选择信息数组TSI,每个TSI是包含两个0-numOfPopulation*2的随机数组成的数组
		val selectInfoArray:Array[Array[Int]] = (0 until numOfPopulation).toArray.map(index =>{
			val tempTSI:Array[Int] = new Array[Int](tournamentSize)
			(0 until tournamentSize).foreach(tempTSI(_) = Random.nextInt(curSize))
			tempTSI
		})

		//广播选择信息数组
		val broadSelectInfoArray = sc.broadcast(selectInfoArray)

		//将选择信息数组转化为分布式RDD,这里别增加分区数，会增加任务切换开销反而降低性能
		val selectInfoRDD = sc.parallelize(selectInfoArray)

		//对TSIRDD中每一个TSI数组，随机在广播的选择信息数组中选取两条TSI，与其合并为TSI组，即一个TSI组包含三条TSI，方便后续做演化操作，每个TSI组对应一个后代
		val TSIGroupRDD:RDD[Array[Int]] = selectInfoRDD.map(TSI_1 =>{
			val TSI_2 = broadSelectInfoArray.value(Random.nextInt(numOfPopulation))
			val TSI_3 = broadSelectInfoArray.value(Random.nextInt(numOfPopulation))
			var eachTSIGroup = Array.concat(TSI_1,TSI_2,TSI_3)
			eachTSIGroup
		})

		//对每个TSI组中的三条TSI分别求出最高评分的BN个体G0，G1，G2，并且将G1、G2作为父代进行交叉
		TSIGroupRDD.flatMap(eachTSIGroup =>{
			val G0 = new BNStructure()
			val G1 = new BNStructure()
			val G2 = new BNStructure()
			//计算出第一条TSI中评分最高的BN结构作为G0
			for(i <- 0 until tournamentSize) {
				if(broadPopulation.value(eachTSIGroup(i)).score > G0.score) {
					G0.score = broadPopulation.value(eachTSIGroup(i)).score
					G0.structure = broadPopulation.value(eachTSIGroup(i)).structure.copy
				}
			}
			//计算出第二条TSI中评分最高的BN结构作为G1
			for(i <-tournamentSize until tournamentSize*2) {
				if(broadPopulation.value(eachTSIGroup(i)).score > G1.score) {
					G1.score = broadPopulation.value(eachTSIGroup(i)).score
					G1.structure = broadPopulation.value(eachTSIGroup(i)).structure.copy
				}
			}
			//计算出第三条TSI中评分最高的BN结构作为G2
			for(i <-tournamentSize*2 until tournamentSize*3) {
				if(broadPopulation.value(eachTSIGroup(i)).score > G2.score) {
					G2.score = broadPopulation.value(eachTSIGroup(i)).score
					G2.structure = broadPopulation.value(eachTSIGroup(i)).structure.copy
				}
			}
			//对G1、G2进行交叉，并合并G0
			Array(G0,new BNStructure(uniformCrossover(G1.structure,G2.structure,numOfAttributes)))
		})
	}

}
