package RunableClass

import Experiences._

object Main {
	def main(args: Array[String]): Unit = {
		//exp1：基于标准GA的贝叶斯网络结构学习
		//SingleGA.run()

		//exp2：基于分布式GA的贝叶斯网络结构学习
		//DistributedGA.run()

		//exp3：测试全局并行化
		//parallelTest.run()

		//exp4：测试并行SS+串行GA+并行评分
		//ParallelSSSerialGAParallelScore.run()

		//exp5：测试并行SS+串行GA+串行评分
		//ParallelSSSerialGASerialScore.run()

		//exp6：测试串行SS+串行GA+并行评分
		SerialSSSerialGAParallelScore.run()


	}
}
