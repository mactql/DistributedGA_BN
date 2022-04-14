package Utils

import Models.BNStructure
import breeze.linalg.DenseMatrix
import org.apache.spark.SparkContext

object EndUtils {

	/*
		求F1评分
		precision = 得到的模型的正确有向边数(matchPositive) 除以 得到的模型中所有边的数目(totalPositiveInPredict)
		sensitivity = 得到的模型的正确有向边数 除以 benchmark BN结构中拥有的边数(totalPositiveInBench)
		specificity：得到的模型的正确缺少的边数(matchNegative) 除以benchmark BN结构中缺少的边数(totalNegativeInBench)
	 */
	def evaluateAccuracyOfTruePositive(stdModelName:String, curModel:DenseMatrix[Int],sc:SparkContext): Double = {

		//val stdModel:Array[Array[Int]] = CSVFileUtils.readStructureFromCsv(stdModelName)
		val stdModel:Array[Array[Int]] = sc.textFile("/Users/caiyiming/BNDataSet/stdModels/myCSV/"+stdModelName+".csv").map(line=>{
			var temp:Array[String] = line.split(",")
			temp.map(t=>{
				if(t.equals("1"))
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
		var F1Score:Double = 0
		var sensitivity:Double = 0
		var specificity:Double = 0
		var matchPositive:Int = 0
		var matchNegative:Int = 0
		var totalPositiveInPredict:Int = 0
		var totalNegativeInPredict:Int = 0
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
		var precision:Double = 0
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
		if (precision+sensitivity == 0) {
			F1Score = 0
		} else {
			F1Score = 1.0 * 2 * precision * sensitivity / (precision+sensitivity)
		}
		F1Score
	}
}
