package Models

import breeze.linalg.DenseMatrix

object SuperStructure {
	def getSSWithMutualInfo(mutualInfoMatrix:DenseMatrix[Double],numOfAttributes:Int):DenseMatrix[Int] = {
		val SS:DenseMatrix[Int] = DenseMatrix.zeros(numOfAttributes, numOfAttributes)
		for(i <- 0 until numOfAttributes) {
			val comparing:Array[Double] = (0 until numOfAttributes).map(mutualInfoMatrix(i, _)).toArray
			val curMaxMI:Double = comparing.max
			val alpha:Double = 0.05
			for(j <- 0 until numOfAttributes) {
				if(comparing(j) >= alpha * curMaxMI) {
					SS.update(i, j, 1)
					SS.update(j, i, 1)
				}
			}
		}
		SS
	}

}
