package Utils

import Models.ScoreCondition
import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{Map, Set}

object BayesTools {

	//对贝叶斯数据集和贝叶斯结构操作相关のutils


	/*
		将每个节点的取值种类用,连成string作为Value，用index作为key，组成set集合
		0 NoVisit,Visit
		1 Absent,Present
		...
	 */
	def getNodeValueMap(tf:Array[Array[String]]):Map[Int,String] = {
		//对每行数据的节点取值标记index并去重
		val temp:Array[(Int, String)] = tf.flatMap(_.zipWithIndex).distinct.map(z=>(z._2,z._1))
		val ans:mutable.Map[Int, String] = mutable.Map[Int, String]()

		//将相同index的节点取值用,进行合并
		temp.foreach(kv => {
			val index = kv._1
			val value = kv._2
			if(ans.contains(index)) {
				ans(index) = ans(index) + "," + value
			} else {
				ans(index) = "" + value
			}
		})
		ans
	}

	/*
		将每个节点的取值种类用,连成string作为Value，用index作为key，组成set集合
		0 NoVisit,Visit
		1 Absent,Present
		...
		实际上就是对数据集节点进行标号，并去重整理得到节点与取值的映射，多种取值用,隔开
	 */
	def getNodeValueMap(tf:RDD[Array[String]]):RDD[(Int,String)] = {
		tf.flatMap(_.zipWithIndex).distinct().map(z=>(z._2,z._1)).reduceByKey((x, y) => x+","+y).sortByKey(/*true,48*/).cache()
	}


	/*
		设置BN结构矩阵的边的方向，即节点是否是另一个节点的父亲
		edgeType=0表示两节点没有联系
		edgeType=1表示aij=1，即i是j的父节点，i指向j
		edgeType=2表示aji=1，即j是i的父节点，j指向i
	 */
	def setEdgeDirectType(denseMatrix: DenseMatrix[Int], x:Int, y:Int, edgeType:Int):DenseMatrix[Int] = {
		val newMatrix:DenseMatrix[Int] = denseMatrix.copy
		if(edgeType == 0) {
			newMatrix.update(x, y, 0)
			newMatrix.update(y, x, 0)
		} else if(edgeType == 1) {
			newMatrix.update(x, y, 1)
			newMatrix.update(y, x, 0)
		} else {
			newMatrix.update(x, y, 0)
			newMatrix.update(y, x, 1)
		}
		newMatrix
	}

	//和set对应
	def getEdgeDirectType(denseMatrix: DenseMatrix[Int], x:Int, y:Int):Int = {
		val xy:Int = denseMatrix(x, y)
		val yx:Int = denseMatrix(y, x)
		if(xy == 0 && yx == 0) {
			0
		} else if(xy == 1 && yx == 0) {
			1
		} else {
			2
		}
	}



	// 获得指定节点的父节点集合
	def getParentSet(matrix:DenseMatrix[Int],child:Int,numOfAttributes:Int): Set[Int] ={

		val parentSet = Set[Int]()
		for(i <- 0 until numOfAttributes){
			if(matrix(i,child)==1) {
				parentSet.add(i)
			}
		}
		parentSet
	}

	//把nodeValueSet(node,"val1,val2,...")转化成Set{<nodeI,val1>,<nodeI,val2>,...}
	def joinStringPair(U:mutable.Set[ScoreCondition],nodeValue:(Int,String)): mutable.Set[ScoreCondition] ={
		val ans:mutable.Set[ScoreCondition] = mutable.Set[ScoreCondition]()
		val nodeValuePairs = nodeValue._2.split(",")
		nodeValuePairs.foreach(targetValue=>{
			var tempSc = new ScoreCondition()
			tempSc.addKeyValue(nodeValue._1.toString,targetValue)
			ans.add(tempSc)
		})
		ScoreCondition.cartesianTwoSet(U,ans)
	}

}
