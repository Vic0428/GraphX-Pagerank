/**
 * Single source shortest path using Pregel
 * 
 * Author: Weiqi Feng
 * Date: October 118, 2019
 * Email: fengweiqi@sjtu.edu.cn
 * Copyright 2019 Vic
 */
import org.apache.spark._
import org.apache.spark.graphx._

object SSSP {
  def main(args: Array[String]) {
        // Define appName and master
        val appName = "SSSP"
        val master = "local[40]"
        // Create new spark context
        val conf = new SparkConf().setAppName(appName)
        val sc = new SparkContext(conf)
        // Construct graph from "wiki-Vote.txt"
        val wikiFile = "/home/lxiang_stu3/Vic/GraphX-Pagerank/data/wiki-Vote.txt"
        val wikiGraph = GraphLoader.edgeListFile(sc, wikiFile).mapEdges(e => e.attr.toDouble).cache()
        // The ultimate source
        val sourceId: VertexId = 42
        // Initialize the graph so that all vertices expect the root have distance infinity
        val initGraph = wikiGraph.mapVertices((id, _) => 
                            if (id == sourceId) 0.0 else Double.PositiveInfinity)
        // Single source shortest path
        val sssp = initGraph.pregel(Double.PositiveInfinity) (
            // vertex program
            (id, dist, newDist) => math.min(dist, newDist),
            // send message
            triplet => {
                if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
                    Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
                } else {
                    Iterator.empty
                }
            },
            // Merge message
            (a, b) => math.min(a, b)
        )
        // Display information
        println(sssp.vertices.collect.mkString("\n"))
        sc.stop()
  }
}