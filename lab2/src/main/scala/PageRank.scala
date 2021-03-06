/**
 * Pagerank Algorithm   
 * 
 * Author: Weiqi Feng
 * Date: October 118, 2019
 * Email: fengweiqi@sjtu.edu.cn
 * Copyright 2019 Vic
 */
import org.apache.spark._
import org.apache.spark.graphx._

object PageRank {
  def main(args: Array[String]) {
        // Define appName and master
        val appName = "PageRank"
        // val master = "spark://10.10.10.1:7070"
        val master = "local[40]"
        // Create new spark context
        val conf = new SparkConf().setAppName(appName).setMaster(master)
        val sc = new SparkContext(conf)
        // Construct graph from "wiki-Vote.txt"
        val wikiFile = "/home/lxiang_stu3/Vic/GraphX-Pagerank/data/wiki-Vote.txt"
        val wikiGraph = GraphLoader.edgeListFile(sc, wikiFile).mapEdges(e => e.attr.toDouble).cache()
        // Initialize the PageRank graph with each edge attribute having
        // weight 1/outDegree and each vertex with attribute 1.0 (pageRank)
        var rankGraph: Graph[Double, Double] = wikiGraph.outerJoinVertices(wikiGraph.outDegrees) { 
                        (vid, vdata, deg) => deg.getOrElse(0) 
                    } .mapTriplets(
                        e => 1.0 / e.srcAttr, TripletFields.Src 
                    ).mapVertices {
                         (id, attr) => 1.0
                    }
        // Start iteraion
        var iteration = 0
        // Maximum iterations
        val numIter = 150
        // previous Rank Graph
        var prevRankGraph: Graph[Double, Double] = null
        // reset probability
        val resetProb = 0.15
        // Start iterating
        while (iteration < numIter) {
            rankGraph.cache()
            // get rank update value
            val rankUpdates = rankGraph.aggregateMessages[Double](
                ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)
            prevRankGraph = rankGraph
            // Update rank graph 
            rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
            (id, oldRank, msgSumOpt) => resetProb + (1.0 - resetProb) * msgSumOpt.getOrElse(0.0)
            }.cache()
            // Move to next iteration
            rankGraph.edges.foreachPartition(x => {})
            println(s"PageRank finished iteration $iteration.")
            prevRankGraph.vertices.unpersist(false)
            prevRankGraph.edges.unpersist(false)
            iteration += 1
        }
        // Print top20 IDs
        rankGraph.vertices.sortBy(
                pair => -pair._2
            ).map{
                case (id, attr) => id
            }.take(20).foreach(println)
        sc.stop()
  }
}