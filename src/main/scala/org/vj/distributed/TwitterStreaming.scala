
package org.vj.distributed

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource

object TwitterStreaming {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(params.getInt("parallelism", 1))
    val streamSource: DataStream[String] = env.addSource(new TwitterSource(params.getProperties))
    streamSource.print()
    env.execute("TwitterStreaming with Apache Flink")
  }
}
