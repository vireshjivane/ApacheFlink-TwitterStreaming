
package org.vj.distributed

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

object TwitterStreaming {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(params)
    env.setParallelism(params.getInt("parallelism", 1))
    val streamSource: DataStream[String] = env.addSource(new TwitterSource(params.getProperties))
    streamSource.flatMap(new ProcessTweetAndPrepareForWrite).print()
    env.execute("TwitterStreaming with Apache Flink")
  }

  private class SelectEnglishAndTokenizeFlatMap(followers_count: Int) extends FlatMapFunction[String, (String, Int)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val isEnglish = jsonNode.has("user") &&
        jsonNode.get("user").has("lang") &&
        jsonNode.get("user").get("lang").asText == "en"
      val hasText = jsonNode.has("text")
      (isEnglish, hasText, jsonNode) match {
        case (true, true, node) =>
          val arrayOfHasgtags = node.get("entities").get("hashtags")
          if (arrayOfHasgtags.isArray && arrayOfHasgtags.iterator().hasNext) {
            import scala.collection.JavaConversions._
            arrayOfHasgtags.foreach({ element =>
              val hashTag = element.get("text").asText()
              if (hashTag.matches("[a-zA-Z0-9]*") && jsonNode.get("user").get("followers_count").asInt > followers_count)
                out.collect(hashTag,1)
            })
          }
        case _ =>
      }
    }
  }

  private class ProcessTweetAndPrepareForWrite() extends FlatMapFunction[String, (String, Int, Int, Boolean, String, String,String)] {
    lazy val jsonParser = new ObjectMapper()
    override def flatMap(value: String, out: Collector[(String, Int, Int, Boolean, String,String,String)]): Unit = {
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val validTweet = jsonNode.has("user") && jsonNode.has("text") && jsonNode.has("geo") &&
        jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText == "en" &&
        jsonNode.get("user").has("followers_count") && jsonNode.get("user").get("followers_count").asInt() > 0 &&
        jsonNode.get("user").has("geo_enabled") && jsonNode.get("user").get("geo_enabled").asBoolean().equals(true) &&
        jsonNode.get("user").has("location") && !jsonNode.get("user").get("location").asText.equalsIgnoreCase("null") &&
        jsonNode.get("geo").has("coordinates") && !jsonNode.get("geo").get("coordinates").asText.equalsIgnoreCase("null")

      (validTweet, jsonNode) match {
        case (true, node) =>
          val location = node.get("user").get("location").asText()
          val followers_count = node.get("user").get("followers_count").asInt()
          val friends_count = if (node.get("user").has("friends_count")) node.get("user").get("friends_count").asInt() else 0
          val geo_enabled = node.get("user").get("geo_enabled").asBoolean()
          val coordinatesArray = node.get("geo").get("coordinates")
          import scala.collection.JavaConversions._
          val latitude = coordinatesArray.head.asText()
          val longitude = coordinatesArray.last.asText()
          val text = jsonNode.get("text").asText()
          out.collect((location, followers_count, friends_count, geo_enabled, latitude, longitude, text))
        case _ =>
      }
    }
  }
}
