import Config.RedisConfig
import Models.{BNStructure, ScoreCondition}
import Models.ScoreModels.BICScore
import Models.SuperStructure.getSSWithMutualInfo
import Operations.GAOperations._
import Utils.{BayesTools, MutualInformationUtils}
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import redis.clients.jedis.{Jedis, Pipeline}

import java.io.{File, PrintWriter}
import scala.collection.{Map, Set, mutable}

object LocalDistributedGATest extends java.io.Serializable{
	var sampleName = "cancer"
	var inputPath = "/Users/caiyiming/Documents/Sparkproject/Samples/mySamples/Cancer50000.csv"

	var numOfSamples:Long = 0

	var maxParent = 4

	var numOfPopulation = 100

	var numOfMaxInterator = 200

	var numOfAttributes:Int = 0

	var numOfMaxIterator:Int = 200

	var crossoverRate:Double = 0.5

	var mutationRate:Double = 0

	//用来判断是否连续30次都没有进步
	var sameTimesScore:Double = Double.MinValue
	var countBestSameTimes:Int = 0
	//迭代次数
	var countIterNum = 0
	var finalBNStructure:BNStructure = _

	var curBestBN = new BNStructure()

	def main(args: Array[String]): Unit = {
		val tournamentSize:Int = 2
		val scoreJedis:Jedis = new Jedis(RedisConfig.redisHosts, RedisConfig.redisPort)
		val scoreJedisPipeline:Pipeline = scoreJedis.pipelined()

		//创建sparkContext

		val sc = new SparkSession.Builder().master("local").getOrCreate().sparkContext

		//读取输入数据RDD，最小分区数为48(师兄设置的)
		val textfile:RDD[Array[String]] = sc.textFile(inputPath,48).cache().map(_.split(","))



		//获取样本数据的节点数目和样本数量
		numOfAttributes = textfile.take(1)(0).length
		numOfSamples = textfile.count()

		//记录算法开始时间
		val startTime = System.currentTimeMillis()

		/*
			将每个节点的取值种类用","连成string作为Value，用index作为key，组成set集合
			0 NoVisit,Visit
			1 Absent,Present
			...
		 */
		val valueTypeSet:Set[(Int,String)] = BayesTools.getNodeValueMap(textfile).collect().toSet

		//广播每个节点取值种类
		val broadValueTpye = sc.broadcast(valueTypeSet)

		//记录求节点取值种类所花时间
		val getValueTpyeTime = System.currentTimeMillis()-startTime

		/*
			计算互信息矩阵
		 */
		val mutualInfoMatrix = MutualInformationUtils.getMutualInfoMatrix(textfile,numOfAttributes,valueTypeSet,scoreJedisPipeline,sc,numOfSamples)

		//记录计算互信息矩阵所花时间
		val getMIMatrixTime = System.currentTimeMillis()-getValueTpyeTime

		//通过互信息矩阵构造超结构
		val SS = getSSWithMutualInfo(mutualInfoMatrix,numOfAttributes)

		//广播超结构
		val broadSS = sc.broadcast(SS)

		//记录互信息构造超结构所花时间
		val getSSTime = System.currentTimeMillis()-getMIMatrixTime

		//通过超结构初始化BN结构种群,当前种群数量为numOfPopulation*2
		val BNMatrixPopulation:RDD[DenseMatrix[Int]] = initPopulationAllWithRemoveCycleAndSS(numOfPopulation * 2,numOfAttributes,sc,broadSS)
		var BNStructurePopulationRDD:RDD[BNStructure] = BNMatrixPopulation.map(m=> new BNStructure(m)).cache()

		//记录使用超结构初始化种群所花时间
		val getPopulationWithSSTime = System.currentTimeMillis()-getSSTime

		//对BN结构种群进行评分计算
		val score:BICScore = new BICScore(numOfAttributes,textfile)
		var BNStructurePopulationArray = score.calculateScoreParallelWithRedis(BNStructurePopulationRDD,textfile,broadValueTpye,sc,scoreJedisPipeline)

		//记录第一次计算评分所花时间
		val firstCalScoreTime = System.currentTimeMillis()-getPopulationWithSSTime

		//求出当前的最优个体
		curBestBN = getEliteIndividual(BNStructurePopulationArray,curBestBN)

		//记录求最优个体所花时间
		val getEliteTime = System.currentTimeMillis()-firstCalScoreTime

		//开始迭代
		while(countIterNum < numOfMaxInterator && countBestSameTimes < 30){
			println("第"+countIterNum+"代：" + curBestBN.score)

			//广播种群，当前种群数量为numOfPopulation*2
			val broadPopulation:Broadcast[Array[BNStructure]] = sc.broadcast(BNStructurePopulationArray)
			//将种群数组转化为分布式种群RDD
			BNStructurePopulationRDD = sc.parallelize(BNStructurePopulationArray)

			//进行分布式锦标赛选择与交叉算子
			val crossoveredPopulationRDD:RDD[BNStructure] = tournamentSelectionAndUniformCrossover(broadPopulation,tournamentSize,numOfPopulation,sc,numOfAttributes)

			//进行分布式突变算子，基于SS单点突变，若不在SS中，则不变易
			val mutationedPopulationRDD:RDD[BNStructure] = crossoveredPopulationRDD.map(eachBN =>{
				new BNStructure(singlePointMutationWithSS(eachBN.structure,broadSS,numOfAttributes))
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
		val iterateTime = System.currentTimeMillis() - getEliteTime

		//记录算法执行的时间
		val executeTime:Double = (System.currentTimeMillis()-startTime)/1000.0

		finalBNStructure = curBestBN
		//finalBNStructure.printBNStructure()

		//f1评分为评估学习的BN结构准确率
		val f1Score:Double = evaluateAccuracyOfTruePositive(sampleName,finalBNStructure.structure,sc)

		println("*****************************************************")
		println("textfile大小："+SizeEstimator.estimate(textfile))
		println()
		println("求节点取值种类耗时" + getValueTpyeTime/1000 + "s")
		println("计算互信息矩阵耗时" + getMIMatrixTime/1000 + "s")
		println("互信息构造超结构耗时" + getSSTime/1000 + "s")
		println("使用超结构初始化种群耗时" + getPopulationWithSSTime/1000 + "s")
		println("第一次计算评分耗时" + firstCalScoreTime/1000 + "s")
		println("求最优个体耗时" + getEliteTime/1000 + "s")
		println("演化迭代阶段总耗时" + iterateTime/1000 + "s")
		println("*****************************************************")
		println("F1score: " + f1Score)
		println("BICScore:" + finalBNStructure.score)
		println("Execute time: " + executeTime + "s")
		println("Stop iter: " + countIterNum)
		println("*****************************************************")

		val writer = new PrintWriter(new File("/Users/caiyiming/Downloads/"+"resultcancer.csv"))
		writer.flush()
		writer.println("构造超结构(s),"+"第一次评分(s)"/*+"总评分(s)," +"演化(s),"+	"总耗时(s),"	+"F1 Score,"+"BIC Score,"+"迭代数"*/)
		writer.println(getSSTime/1000+","+firstCalScoreTime/1000)
		writer.close()


		broadValueTpye.destroy()
		broadSS.destroy()
		scoreJedis.flushAll()
		scoreJedis.close()
	}




	def evaluateAccuracyOfTruePositive(stdModelName: String, curModel: DenseMatrix[Int], sc: SparkContext): Double = {

		//val stdModel:Array[Array[Int]] = CSVFileUtils.readStructureFromCsv(stdModelName)
		val stdModel: Array[Array[Int]] = sc.textFile("/Users/caiyiming/Documents/Sparkproject/Models/myCSV/" + stdModelName + ".csv").map(line => {
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
