import Experiences.SingleGA.{numOfMaxInterator, numOfPopulation}
import Models.BNStructure
import Models.ScoreModels.BICScore
import Operations.GAOperations._
import Utils.BayesTools
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.Set

object LocalSingleGATest {
	var sampleName = "test"
	var inputPath = "/Users/caiyiming/Documents/Sparkproject/Samples/test.csv"

	var finalBNStructure: BNStructure = _

	def main(args: Array[String]): Unit = {

		var tournamentSize: Int = 2
		//		val scoreJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
		//		val scoreJedisPipeline:Pipeline = scoreJedis.pipelined()


		//创建sparkContext
		val sc = new SparkSession.Builder().master("local").getOrCreate().sparkContext

		//读取输入数据，最小分区数为48(师兄设置的),用collect将RDD转化为数组，即样本数据的二维数组
		val textfile: Array[Array[String]] = sc.textFile(inputPath, 48).cache().map(_.split(",")).collect()

		//获取样本数据的节点数目
		val numOfAttributes = textfile(0).length

		//记录算法开始时间
		var startTime = System.currentTimeMillis()

		/*
			将每个节点的取值种类用,连成string作为Value，用index作为key，组成set集合
			0 NoVisit,Visit
			1 Absent,Present
			...
		 */
		val nodeValueSet: Set[(Int, String)] = BayesTools.getNodeValueMap(textfile).toSet
		//广播节点取值set
		val broadNodeValue = sc.broadcast(nodeValueSet)

		//初始化种群，n个变量的BN结构可以用n*n的邻接矩阵表示，aij=1则表示i是j的父节点，i指向j
		val BNMatrixPopulation: Array[DenseMatrix[Int]] = initPopulationAllWithRemoveCycle(numOfPopulation * 2, numOfAttributes, sc)

		//对BN结构种群进行评分计算
		val score: BICScore = new BICScore(numOfAttributes, textfile)
		var BNStructurePopulation: Array[BNStructure] = score.calculateScore(BNMatrixPopulation, textfile, broadNodeValue.value)

		//获取当前种群中的精英个体，是否可以和上一步合并，在计算评分的时候就计算精英个体
		var curBestBN = getEliteIndividual(BNStructurePopulation,new BNStructure())


		//进行迭代，每次按顺序执行锦标赛选择算子、均匀交叉算子、单点突变算子、BIC评分、获取精英个体、替换最差个体

		//用来判断是否连续30次都没有进步
		var sameTimesScore: Double = Double.MinValue
		var countBestSameTimes: Int = 0

		var countIterNum = 0
		while (countIterNum < numOfMaxInterator && countBestSameTimes < 30) {
			println("第"+countIterNum+"代：" + curBestBN.score)

			//锦标赛选择算子得到100条染色体
			BNStructurePopulation = tournamentSelection(BNStructurePopulation, tournamentSize, numOfPopulation, sc)
			//对这100条染色体均匀交叉，得到100条均匀交叉后的染色体与锦标赛得到的100条混合成200条染色体的种群
			BNStructurePopulation = uniformCrossoverAll(BNStructurePopulation, numOfPopulation, numOfAttributes, sc)
			//对200条染色体进行单点突变
			BNStructurePopulation = singlePointMutationAll(BNStructurePopulation, numOfAttributes)
			//BIC评分
			BNStructurePopulation = score.calculateScore(BNStructurePopulation.map(_.structure), textfile, broadNodeValue.value)
			//获取精英个体
			curBestBN = getEliteIndividual(BNStructurePopulation,curBestBN)
			BNStructurePopulation = replaceLowestWithElite(BNStructurePopulation, curBestBN)

			//判断迭代是否已经无法更优，若迭代已经连续30次相同的精英个体说明已经收敛
			if (curBestBN.score != sameTimesScore) {
				countBestSameTimes = 0
				sameTimesScore = curBestBN.score
			} else
				countBestSameTimes += 1
			countIterNum += 1
		}

		//记录算法执行的时间
		val executeTime: Double = (System.currentTimeMillis() - startTime) / 1000.0

		finalBNStructure = curBestBN
		//finalBNStructure.printBNStructure()

		val f1Score: Double = evaluateAccuracyOfTruePositive(sampleName, finalBNStructure.structure, sc)
		println("*****************************************************")
		println("F1score: " + f1Score)
		println("Execute time: " + executeTime + "s")
		println("Stop iter: " + countIterNum)
		println("*****************************************************")
		broadNodeValue.destroy()
	}

	def evaluateAccuracyOfTruePositive(stdModelName: String, curModel: DenseMatrix[Int], sc: SparkContext): Double = {

		//val stdModel:Array[Array[Int]] = CSVFileUtils.readStructureFromCsv(stdModelName)
		val stdModel: Array[Array[Int]] = sc.textFile("/Users/caiyiming/Documents/Sparkproject/Models/CSV/" + stdModelName + ".csv").map(line => {
			var temp: Array[String] = line.split(",")
			temp.map(t => {
				if (t.equals("1"))
					1;
				else 0;
			})
		}).collect()

		val numOfAttribute = stdModel.length
		var totalPositiveInBench = 0
		var totalNegativeInBench = 0
		for (i <- 0 until numOfAttribute) {
			for (j <- i until numOfAttribute) {
				//只有没边的时候才会相等
				if (stdModel(i)(j) != stdModel(j)(i)) {
					totalPositiveInBench = totalPositiveInBench + 1
				} else {
					totalNegativeInBench = totalNegativeInBench + 1
				}
			}
		}
		var F1Score: Double = 0
		var sensitivity: Double = 0
		var specificity: Double = 0
		var matchPositive: Int = 0
		var matchNegative: Int = 0
		var totalPositiveInPredict: Int = 0
		var totalNegativeInPredict: Int = 0
		for (i <- 0 until numOfAttribute) {
			for (j <- i until numOfAttribute) {
				val ij = curModel(i, j)
				val ji = curModel(j, i)
				if (ij != ji) {
					totalPositiveInPredict = totalPositiveInPredict + 1
				} else {
					totalNegativeInPredict = totalNegativeInPredict + 1
				}

				if (ij == stdModel(i)(j) && ji == stdModel(j)(i)) {
					if (ij == 0 && ji == 0) {
						matchNegative = matchNegative + 1
					} else {
						matchPositive = matchPositive + 1
					}
				}
			}
		}
		if (totalPositiveInBench == 0) {
			sensitivity = 1.0
		} else {
			sensitivity = 1.0 * matchPositive / totalPositiveInBench
		}
		var precision: Double = 0
		if (totalPositiveInPredict == 0) {
			precision = 1
		} else {
			precision = 1.0 * matchPositive / totalPositiveInPredict
		}
		if (totalNegativeInBench == 0) {
			specificity = 1
		} else {
			specificity = 1.0 * matchNegative / totalNegativeInBench
		}
		if (precision + sensitivity == 0) {
			F1Score = 0
		} else {
			F1Score = 1.0 * 2 * precision * sensitivity / (precision + sensitivity)
		}
		F1Score
	}
}
