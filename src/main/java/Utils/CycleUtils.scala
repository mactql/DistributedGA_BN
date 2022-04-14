package Utils

import breeze.linalg.DenseMatrix

import scala.collection.{Set, mutable}
import scala.util.Random

object CycleUtils {

	def removeCycleWithDegree(matrix:DenseMatrix[Int], numOfAttribute:Int):DenseMatrix[Int] = {
		val nextBN = matrix.copy

		val cycleUtils:CycleUtils = new CycleUtils()
		cycleUtils.numOfAttribute = numOfAttribute
		var nodeList:List[Int] = (0 until numOfAttribute).toList
		var seq1:List[Int] = List[Int]()
		var seq2:List[Int] = List[Int]()

		//GR最小反馈弧集算法
		while(nodeList.nonEmpty) {

			//不断循环找剩余nodelist中出度最小的节点，如果是0就加入seq2并在nodelist中删除，否则就break
			var brk = false
			while (!brk && nodeList.nonEmpty) {
				var dpm:(Int, Int) = (-1, Integer.MAX_VALUE)
				nodeList.foreach(node => {
					val SetWithoutNode:Set[Int] = nodeList.filter(_!=node).toSet
					val curOutDegree:Int = cycleUtils.outDegree(nextBN, node, SetWithoutNode)
					if(curOutDegree < dpm._2) {
						dpm = (node, curOutDegree)
					}
				})
				if (dpm._2 == 0) {
					seq2 = dpm._1 :: seq2
					nodeList = nodeList.filter(_!=dpm._1)
				} else {
					brk = true
				}
			}

			//不断循环找剩余nodelist中入度最小的节点，如果是0就加入seq1并在nodelist中删除，否则就break
			brk = false
			while(!brk && nodeList.nonEmpty) {
				var dpm:(Int, Int) = (-1, Integer.MAX_VALUE)
				nodeList.foreach(node => {
					val SetWithoutNode:Set[Int] = nodeList.filter(_!=node).toSet
					val curInDegree:Int = cycleUtils.inDegree(nextBN, node, SetWithoutNode)
					if(curInDegree < dpm._2) {
						dpm = (node, curInDegree)
					}
				})
				if (dpm._2 == 0) {
					seq1 = seq1 :+ dpm._1
					nodeList = nodeList.filter(_!=dpm._1)
				} else {
					brk = true
				}
			}


			if(nodeList.nonEmpty) {

				//找到nodelist中出度-入度差值最大的节点，把它加入seq1，并在nodelist中删除
				var max_out_in = -1
				var add_node = -1
				for(node <- nodeList) {
					val SetWithoutNode:Set[Int] = nodeList.filter(_!=node).toSet
					val temp_max_inout = cycleUtils.outDegree(nextBN, node, SetWithoutNode) - cycleUtils.inDegree(nextBN, node, SetWithoutNode)
					if(temp_max_inout > max_out_in) {
						max_out_in = temp_max_inout
						add_node = node
					}
				}
				seq1 = seq1 :+ add_node
				nodeList = nodeList.filter(_!=add_node)
			}
		}

		//得到最优顶点序列seq
		val seq = seq1 ++ seq2

		// 删除后向边，即序列左反馈弧
		for(jI <- 2 until seq.size) {
			val j = seq(jI)
			val ind = jI-2
			for(kI <- 0 to ind) {
				val k = seq(kI)
				nextBN.update(j, k, 0)
			}
		}
		nextBN
	}

	//删除超过最大父节点数量的冗余边
	def limitParentNaive(matrix:DenseMatrix[Int], numOfAttribute:Int, maxParent:Int):DenseMatrix[Int] = {
		/*
			找出每个节点的父节点集合，删除超过maxParent的边
		 */
		val validBN = matrix.copy
		for (node <- 0 until numOfAttribute) {
			//把父节点过滤出来
			val parentSet: Array[Int] = Range(0, numOfAttribute).filter(matrix(_, node) == 1).toArray
			//删除冗余的边
			if (parentSet.length > maxParent) {
				val needRemoveLength = parentSet.length - maxParent
				val needRemoveSet: mutable.Set[Int] = mutable.Set()
				Range(0, needRemoveLength).foreach(index => {
					var tempParent = parentSet(Random.nextInt(parentSet.length))
					//是否这里会出现多次随机到已经选到过的节点的情况，优化？
					while (needRemoveSet.contains(tempParent)) {
						tempParent = parentSet(Random.nextInt(parentSet.length))
					}
					needRemoveSet.add(tempParent)
				})
				needRemoveSet.foreach(validBN.update(_, node, 0))
			}
		}
		validBN
	}
}
class CycleUtils{
	var numOfAttribute = 0

	def outDegree(m:DenseMatrix[Int], x:Int, resNode:Set[Int]):Int = {
		var out:Int = 0
		resNode.map(m(x, _)).sum
	}


	def inDegree(m:DenseMatrix[Int], x:Int, resNode:Set[Int]):Int = {
		resNode.map(m(_, x)).sum
	}
}
