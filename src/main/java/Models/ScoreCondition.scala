package Models

import java.util
import scala.collection.mutable

object ScoreCondition {

	//将两个集合做笛卡尔乘积，即所有的排列组合，用来求节点取值集合和其父节点取值集合所有的匹配
	def cartesianTwoSet(U:mutable.Set[ScoreCondition],X:mutable.Set[ScoreCondition]): mutable.Set[ScoreCondition] = {
		val ans = mutable.Set[ScoreCondition]()
		if(U.isEmpty)  X
		else if (X.isEmpty)  U
		else {
			U.foreach(u => {
				X.foreach(x => {
					val tempCondition = new ScoreCondition()
					tempCondition.conditions ++= u.conditions
					tempCondition.conditions ++= x.conditions
					ans.add(tempCondition)
				})
			})
			ans
		}
	}


}

class ScoreCondition extends java.io.Serializable{

	/*
		conditions是一个map，其中每个映射的key是节点index，value是当前节点的取值
		如 ("0","visit")
	 */
	var conditions:mutable.Map[String,String] = mutable.Map[String,String]()

	def addKeyValue(key:String,value:String):Unit = {
		conditions.put(key,value)
	}

	def tmpAddKeyValue(key:String,value:String):ScoreCondition = {
		val tmpCondition = new ScoreCondition()
		conditions.foreach(kv=>tmpCondition.addKeyValue(kv._1,kv._2))
		tmpCondition.addKeyValue(key,value)
		tmpCondition
	}

	//用来和一行样本数据的condition进行match，如果和这行数据全部相同，就true
	def matchData(sample:Array[String]):Boolean = {
		this.conditions.foreach(kv=>{
			if( !sample(kv._1.toInt).equals(kv._2)) return false
		})
		true
	}

	def getConditions:Map[String,String] = {conditions.toMap}

	override def toString: String = {
		var mapStringfy = new StringBuilder
		val arrayIndex:util.ArrayList[Int] = new util.ArrayList[Int]()
		val arrayValue:util.ArrayList[String] = new util.ArrayList[String]()
		conditions.foreach(kv=>{
			val nodeI:Int = kv._1.toInt
			val nodeIValue:String = kv._2
			if (arrayIndex.isEmpty){
				arrayIndex.add(nodeI)
				arrayValue.add(nodeIValue)
			}
			else{
				var i=0
				while (i<arrayIndex.size() && arrayIndex.get(i) < nodeI){
					i += 1
				}
				arrayIndex.add(i,nodeI)
				arrayValue.add(i,nodeIValue)
			}
		})
		for(i<- 0 until arrayIndex.size()){
			mapStringfy ++= arrayIndex.get(i) + "," + arrayValue.get(i) + ";"
		}
		mapStringfy.toString()
	}

	override def equals(obj: Any): Boolean = {
		if(super.equals(obj)) return true
		if(obj==null || getClass()!=obj.getClass()) return false
		val scoreCondition:ScoreCondition = obj.asInstanceOf[ScoreCondition]
		if(scoreCondition.conditions.size != this.conditions.size) return false
		else{
			this.conditions.foreach(need=>{
				if(!scoreCondition.conditions.contains(need._1) || !scoreCondition.conditions(need._1).equals(need._2)) return false
			})
			scoreCondition.conditions.foreach(need => {
				if(!this.conditions.contains(need._1) || !this.conditions(need._1).equals(need._2)) return false
			})
			true
		}
	}


	override def hashCode(): Int = {
		getConditions.map(kv=>{
			kv.hashCode()
		}).sum
	}
}
