package Experiences

import Config.RedisConfig
import Experiences.parallelTest.{inputPath, numOfAttributes, numOfMaxInterator, numOfPopulation, sampleName}
import Models.{BNStructure, ScoreCondition}
import Models.ScoreModels.BICScore
import Models.SuperStructure.getSSWithMutualInfo
import Operations.GAOperations.{getEliteIndividual, initPopulationAllWithRemoveCycleAndSS, replaceLowestWithElite, singlePointMutationWithSS, tournamentSelectionAndUniformCrossover}
import Utils.{BayesTools, EndUtils, MutualInformationUtils}
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.Set
import scala.io.StdIn

object parallelTest {
	var sampleName = ""

	var datasetName = ""

	var inputRootPath = "/Users/caiyiming/BNDataSet/mySamples/"

	var inputPath = ""

	var maxParent = 4

	var numOfAttributes:Int = 0

	var numOfPopulation = 100

	var numOfMaxInterator = 200

	var crossoverRate:Double = 0.5

	var mutationRate:Double = 0

	var numOfSamples:Long = 0

	def run(): Unit = {
		print("模型名称：")
		sampleName = StdIn.readLine()
		print("数据集名称：")
		datasetName = StdIn.readLine()
		inputPath = inputRootPath + datasetName + ".csv"
		val ga: parallelTest = new parallelTest()
		ga.run()
	}

}

class parallelTest extends java.io.Serializable {
	//用来判断是否连续30次都没有进步
	var sameTimesScore:Double = Double.MinValue
	var countBestSameTimes:Int = 0
	//迭代次数
	var countIterNum = 0
	var finalBNStructure:BNStructure = _

	var SPARK_JARS_HOME = "/usr/hdp/3.1.0.0-78/spark2/jars/"

	def run(): Unit = {

		val tournamentSize: Int = 2
		val scoreJedis: Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
		val scoreJedisPipeline: Pipeline = scoreJedis.pipelined()

		//创建sparkContext
		val conf = new SparkConf().setAppName("parallelTest")
				.setMaster("yarn")
				.setSparkHome(SPARK_JARS_HOME)
		val sc = new SparkSession.Builder().config(conf).getOrCreate().sparkContext

		//读取输入数据RDD，最小分区数为48(师兄设置的)
		val textfile: RDD[Array[String]] = sc.textFile(inputPath).cache().map(_.split(","))

		//获取样本数据的节点数目和样本数量
		val copyNumOfAttributes = textfile.take(1)(0).length
		numOfAttributes = copyNumOfAttributes
		//numOfSamples = textfile.count()

		//设置当前最优结构和BIC评分对象
		var curBestBN:BNStructure = new BNStructure()
		val score:BICScore = new BICScore(numOfAttributes,textfile)


		//记录算法开始时间
		val startTime = System.currentTimeMillis()

		/*
			将每个节点的取值种类用","连成string作为Value，用index作为key，组成set集合
			0 NoVisit,Visit
			1 Absent,Present
			...
		 */
		val valueTypeSet:Set[(Int,String)] = BayesTools.getNodeValueMap(textfile).collect().toSet

		//记录求节点取值种类所花时间
		val getValueTpyeTime = System.currentTimeMillis()-startTime

		//广播每个节点取值种类
		val broadValueTpye = sc.broadcast(valueTypeSet)

		//记录计算互信息矩阵开始时间
		val startMIMatrixTime = System.currentTimeMillis()

		/*
			计算互信息矩阵
		 */
		val mutualInfoMatrix = MutualInformationUtils.getMutualInfoMatrix(textfile,copyNumOfAttributes,broadValueTpye.value,scoreJedisPipeline,sc,score.numOfSamples)

		//记录计算互信息矩阵所花时间以及构造超结构开始时间
		val getMIMatrixTime = (System.currentTimeMillis()-startMIMatrixTime)/1000
		val startGetSS = System.currentTimeMillis()

		//通过互信息矩阵构造超结构
		val SS = getSSWithMutualInfo(mutualInfoMatrix,copyNumOfAttributes)

		//记录互信息构造超结构所花时间
		val getSSTime = System.currentTimeMillis()-startGetSS

		//广播超结构
		val broadSS = sc.broadcast(SS)

		//记录初始化种群开始时间
		val startGetPopulationTime = System.currentTimeMillis()

		//通过超结构初始化BN结构种群,当前种群数量为numOfPopulation*2
		val BNMatrixPopulation:RDD[DenseMatrix[Int]] = initPopulationAllWithRemoveCycleAndSS(numOfPopulation * 2,copyNumOfAttributes,sc,broadSS)
		var BNStructurePopulationRDD:RDD[BNStructure] = BNMatrixPopulation.map(m=> new BNStructure(m)).cache()



		//记录使用超结构初始化种群所花时间及第一次评分开始时间
		val getPopulationWithSSTime = System.currentTimeMillis()-startGetPopulationTime
		val startFirstCallScoreTime = System.currentTimeMillis()

		//对BN结构种群进行评分计算
		var BNStructurePopulationArray = score.calculateScoreParallelWithRedis(BNStructurePopulationRDD,textfile,broadValueTpye,sc,scoreJedisPipeline)

		//记录第一次计算评分所花时间及求当前最优个体开始时间
		val firstCalScoreTime = (System.currentTimeMillis()-startFirstCallScoreTime)/1000
		val startGetElite = System.currentTimeMillis()

		//求出当前的最优个体
		curBestBN = getEliteIndividual(BNStructurePopulationArray,curBestBN)

		//记录求最优个体所花时间及迭代开始时间
		val getEliteTime = System.currentTimeMillis()-startGetElite
		val startIterateTime = System.currentTimeMillis()

		//开始迭代
		while(countIterNum < numOfMaxInterator && countBestSameTimes < 30){
			println("第"+countIterNum+"代：" + curBestBN.score)

			//广播种群，当前种群数量为numOfPopulation*2
			val broadPopulation:Broadcast[Array[BNStructure]] = sc.broadcast(BNStructurePopulationArray)
			//将种群数组转化为分布式种群RDD
			//BNStructurePopulationRDD = sc.parallelize(BNStructurePopulationArray)

			//进行分布式锦标赛选择与交叉算子
			val crossoveredPopulationRDD:RDD[BNStructure] = tournamentSelectionAndUniformCrossover(broadPopulation,tournamentSize,numOfPopulation,sc,numOfAttributes)

			//进行分布式突变算子，基于SS单点突变，若不在SS中，则不变异
			val mutationedPopulationRDD:RDD[BNStructure] = crossoveredPopulationRDD.map(eachBN =>{
				new BNStructure(singlePointMutationWithSS(eachBN.structure,broadSS,copyNumOfAttributes))
			}).cache()

			//评分计算
			val scoredBNPopulation = score.calculateScoreParallelWithRedis(mutationedPopulationRDD,textfile,broadValueTpye,sc,scoreJedisPipeline)

			//精英替换
			curBestBN = getEliteIndividual(scoredBNPopulation,curBestBN)
			BNStructurePopulationArray = replaceLowestWithElite(scoredBNPopulation,curBestBN)

			broadPopulation.unpersist()

			//判断迭代是否已经无法更优，若迭代已经连续30次相同的精英个体说明已经收敛
			if(curBestBN.score != sameTimesScore){
				sameTimesScore = curBestBN.score
				countBestSameTimes = 0
			}else
				countBestSameTimes += 1


			countIterNum += 1
		}
		//记录演化迭代阶段总耗时
		val iterateTime = (System.currentTimeMillis() - startIterateTime)/1000.0

		//记录算法执行总耗时
		val executeTime:Double = (System.currentTimeMillis()-startTime)/1000.0

		finalBNStructure = curBestBN
		//finalBNStructure.printBNStructure()

		//f1评分为评估学习的BN结构准确率
		val f1Score:Double = EndUtils.evaluateAccuracyOfTruePositive(sampleName,finalBNStructure.structure,sc)
		println("*****************************************************")
		println("求节点取值种类耗时" + getValueTpyeTime + "ms")
		println("计算互信息矩阵耗时" + getMIMatrixTime + "s")
		println("互信息构造超结构耗时" + getSSTime + "ms")
		println("使用超结构初始化种群耗时" + getPopulationWithSSTime + "ms")
		println("第一次计算评分耗时" + firstCalScoreTime + "s")
		println("求最优个体耗时" + getEliteTime + "ms")
		println("演化迭代阶段总耗时" + iterateTime + "s")
		println("*****************************************************")
		println("F1score: " + f1Score)
		println("BICScore:" + finalBNStructure.score)
		println("Execute time: " + executeTime + "s")
		println("Stop iter: " + countIterNum)
		println("*****************************************************")
		broadValueTpye.destroy()
		broadSS.destroy()

		scoreJedis.flushAll()
		scoreJedis.close()

	}
}
