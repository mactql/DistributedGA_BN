package Experiences

import Models.ScoreModels.BICScore
import Experiences.SingleGA._
import Models.BNStructure
import Utils._
import Operations.GAOperations._
import org.apache.spark.sql._

import scala.collection._
import breeze.linalg._
import org.apache.spark.SparkConf

import scala.io.StdIn

object SingleGA{

	var sampleName = ""

	var datasetName = ""

	var inputRootPath = "/Users/caiyiming/BNDataSet/mySamples/"

	var inputPath = ""

	var maxParent = 4

	var numOfPopulation = 100

	var numOfMaxInterator = 200

	var SPARK_JARS_HOME = "/usr/hdp/3.1.0.0-78/spark2/jars/"

	def run(): Unit = {

		print("模型名称：")
		sampleName = StdIn.readLine()
		print("数据集名称：")
		datasetName = StdIn.readLine()
		inputPath = inputRootPath + datasetName + ".csv"
		val ga: SingleGA = new SingleGA()
		ga.run()
	}
}

class SingleGA extends java.io.Serializable{

//	var sampleName = "survey"
//	var inputPath = "/Users/caiyiming/SingleGA/Samples/survey50000.csv"

	var finalBNStructure:BNStructure = _

	def run(): Unit = {
		val tournamentSize:Int = 2
//		val scoreJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
//		val scoreJedisPipeline:Pipeline = scoreJedis.pipelined()

		//创建sparkContext
		val conf = new SparkConf().setAppName("SingleGA")
				.setMaster("yarn")
				.setSparkHome(SPARK_JARS_HOME)
		val sc = new SparkSession.Builder().config(conf).getOrCreate().sparkContext

		//读取输入数据，最小分区数为48(师兄设置的),用collect将RDD转化为数组，即样本数据的二维数组
		val textfile:Array[Array[String]] = sc.textFile(inputPath).cache().map(_.split(",")).collect()

		//获取样本数据的节点数目
		val numOfAttributes = textfile(0).length

		//记录算法开始时间
		val startTime = System.currentTimeMillis()

		/*
			将每个节点的取值种类用,连成string作为Value，用index作为key，组成set集合
			0 NoVisit,Visit
			1 Absent,Present
			...
		 */
		val valueTypeSet:Set[(Int,String)] = BayesTools.getNodeValueMap(textfile).toSet
		//广播节点取值set
		val broadValueTpye = sc.broadcast(valueTypeSet)

		//初始化种群，n个变量的BN结构可以用n*n的邻接矩阵表示，aij=1则表示i是j的父节点，i指向j
		val BNMatrixPopulation:Array[DenseMatrix[Int]] = initPopulationAllWithRemoveCycle(numOfPopulation * 2, numOfAttributes, sc)


		//对BN结构种群进行评分计算
		val score:BICScore = new BICScore(numOfAttributes,textfile)
		var BNStructurePopulation:Array[BNStructure] = score.calculateScore(BNMatrixPopulation,textfile,broadValueTpye.value)

		//获取当前种群中的精英个体
		var curBestBN = getEliteIndividual(BNStructurePopulation,new BNStructure())


		//进行迭代，每次按顺序执行锦标赛选择算子、均匀交叉算子、单点突变算子、BIC评分、获取精英个体、替换最差个体

		//用来判断是否连续30次都没有进步
		var sameTimesScore:Double = Double.MinValue
		var countBestSameTimes:Int =   0

		var countIterNum = 0
		while(countIterNum < numOfMaxInterator && countBestSameTimes < 30){
			//println("第"+countIterNum+"代：" + curBestBN.score)
			//锦标赛选择算子得到100条染色体
			BNStructurePopulation = tournamentSelection(BNStructurePopulation,tournamentSize,numOfPopulation,sc)
			//对这100条染色体均匀交叉，得到100条均匀交叉后的染色体与锦标赛得到的100条混合成200条染色体的种群
			BNStructurePopulation = uniformCrossoverAll(BNStructurePopulation,numOfPopulation,numOfAttributes,sc)
			//对200条染色体进行单点突变
			BNStructurePopulation = singlePointMutationAll(BNStructurePopulation,numOfAttributes)
			//BIC评分
			BNStructurePopulation = score.calculateScore(BNStructurePopulation.map(_.structure),textfile,broadValueTpye.value)
			//获取精英个体
			curBestBN = getEliteIndividual(BNStructurePopulation,curBestBN)
			BNStructurePopulation = replaceLowestWithElite(BNStructurePopulation,curBestBN)

			//判断迭代是否已经无法更优，若迭代已经连续30次相同的精英个体说明已经收敛
			if(curBestBN.score != sameTimesScore){
				countBestSameTimes = 0
				sameTimesScore = curBestBN.score
			}else
				countBestSameTimes += 1
			countIterNum += 1
		}

		//记录算法执行的时间
		val executeTime:Double = (System.currentTimeMillis()-startTime)/1000.0

		finalBNStructure = curBestBN
//finalBNStructure.printBNStructure()

		//f1评分为评估学习的BN结构准确率
		val f1Score:Double = EndUtils.evaluateAccuracyOfTruePositive(sampleName,finalBNStructure.structure,sc)

		println("*****************************************************")
		println("F1score: " + f1Score)
		println("BICScore:" + finalBNStructure.score)
		println("Execute time: " + executeTime + "s")
		println("Stop iter: " + countIterNum)
		println("*****************************************************")
		broadValueTpye.destroy()


	}

}
