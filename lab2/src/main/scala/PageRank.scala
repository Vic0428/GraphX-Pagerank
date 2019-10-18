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
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageRank {
  def main(args: Array[String]) {
        // Define appName and master
        val appName = "My app"
        // Create new spark context
        val conf = new SparkConf().setAppName(appName)
        val sc = new SparkContext(conf)
        // Construct graph from "web-Google.txt"
        val wikiFile = "/home/lxiang_stu3/Vic/GraphX-Pagerank/data/wiki-Vote.txt"
        val wikiGraph = GraphLoader.edgeListFile(sc, googleFile).mapEdges(e => e.attr.toDouble).cache()
        // First associate the degree with each vertex
        // Second, set the weight on the edges based on the degree
        // Finally, set the vertex attibutes to (initialPR, delta=0)
        val pageRankGraph: Graph[(Double, Double), Double] = wikiGraph.outerJoinVertices(graph.outDegrees) {(vid, vdata, deg) => deg.getOrElse(0)
                            }.mapTriplets( e => 1.0 / e.srcAttr
                            ).mapVertices((id, attr) => (0.0, 0.0)
                            ).cache() 
        val resetProb = 0.15
        val tol = 0.001
        // Define the vertex functions
    def vertexProgram(id: VertexId, attr: (Double, Double), msgSum: Double): (Double, Double) = {
      val (oldPR, lastDelta) = attr
      val newPR = oldPR + (1.0 - resetProb) * msgSum
      (newPR, newPR - oldPR)
    }
        // Define send message program
        def sendMessage(edge: EdgeTriplet[(Double, Double), Double]) = {
            if (edge.srcAttr._2 > tol) {
                Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
            } else {
                Iterator.empty
            }
        }

        def mergeMessage(a: Double, b: Double): Double = {
            a + b
        }

        // The initial message received by all vertices in pageRank
        val initMessage = resetProb / (1.0 - resetProb)
        val vp = (id: VertexId, attr: (Double, Double), msgSum: Double) => vertexProgram(id, attr, msgSum)
        val ranks = Pregel(pageRankGraph, initMessage, activeDirection = EdgeDirection.Out)(
            vp, sendMessage, mergeMessage).mapVertices((vid, attr) => attr._1)

        sc.stop()
  }
}