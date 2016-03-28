import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
//
object Grapx01 {
	//def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
case class Person(name:String,age:Int)
def main(args: Array[String]) {
	println(new java.io.File( "." ).getCanonicalPath)
	val conf = new SparkConf(false) // skip loading external settings
	.setMaster("local") // could be "local[4]" for 4 threads
	.setAppName("Chapter 9")
	.set("spark.logConf", "true")
	val sc = new SparkContext(conf) // ("local","Chapter 9") if using directly
	println(s"Running Spark Version ${sc.version}")
	//
	val defaultPerson = Person("NA",0)
	val vertexList = List(
			(1L, Person("Alice", 18)),
			(2L, Person("Bernie", 17)),
			(3L, Person("Cruz", 15)),
			(4L, Person("Donald", 12)),
			(5L, Person("Ed", 15)),
			(6L, Person("Fran", 10)),
			(7L, Person("Genghis",854))
			)

	val edgeList = List(
			Edge(1L, 2L, 5),
			Edge(1L, 3L, 1),
			Edge(3L, 2L, 5),
			Edge(2L, 4L, 12),
			Edge(4L, 5L, 4),
			Edge(5L, 6L, 2),
			Edge(6L, 7L, 2),
			Edge(7L, 4L, 5),
			Edge(6L, 4L, 4)
			)

	val vertexRDD = sc.parallelize(vertexList)
	val edgeRDD = sc.parallelize(edgeList)
	val graph = Graph(vertexRDD, edgeRDD,defaultPerson)
	//
	println("Edges = " + graph.numEdges)
  println("Vertices = " + graph.numVertices)
  //
  val vertices = graph.vertices
  vertices.collect.foreach(println)
  val edges = graph.edges
  edges.collect.foreach(println)
  val triplets = graph.triplets
  triplets.take(3)
  triplets.map(t=>t.toString).collect().foreach(println)
  //
  val inDeg = graph.inDegrees // Followers
  inDeg.collect().foreach(println)
  val outDeg = graph.outDegrees // Follows
  outDeg.collect().foreach(println)
  val allDeg = graph.degrees
  allDeg.collect().foreach(println)
  //
  val g1 = graph.subgraph(epred = (edge) => edge.attr > 4)
  g1.triplets.collect.foreach(println)
  //
  // What's Wrong ?
  //
  val g2 = graph.subgraph(vpred = (id, person) => person.age > 21)
  g2.triplets.collect.foreach(println)
  //
  // Look ma, no edges !
  //
  g2.vertices.collect.foreach(println)
  g2.edges.collect.foreach(println)
  //
  val g3 = graph.subgraph(vpred = (id, person) => person.age >= 18)
  g3.triplets.collect.foreach(println)
  //
  // Just two disjoint vertices
  // If there are no edges, is it really a graph ?
  //
  g3.vertices.collect.foreach(println)
  // Community-Affiliation-Strengths
  val cc = graph.connectedComponents() // returns another graph
  cc.triplets.collect.foreach(println)
  graph.connectedComponents.vertices.map(_.swap).groupByKey.map(_._2).collect.foreach(println)
  cc.vertices.map(_._2).collect.distinct.size // No. of connected components
  //
  // list the components and its number of nodes in the descending order
  cc.vertices.groupBy(_._2).map(p=>(p._1,p._2.size)).
    sortBy(x=>x._2,false). // sortBy(keyFunc,ascending)
    collect()
  // strongly connected components
  val ccS = graph.stronglyConnectedComponents(10)
  ccS.triplets.collect
  ccS.vertices.map(_.swap).groupByKey.map(_._2).collect
  ccS.vertices.map(_._2).collect.distinct.size // No. of connected components
  //
  val triCounts = graph.triangleCount()
  val triangleCounts = triCounts.vertices.collect
  // Algorithms
  val oldestFollower = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age),//sendMsg
    (x,y) => math.max(x,y) //mergeMsg
    )
  oldestFollower.collect()
  //
  // #1
  val youngestFollower = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToDst(edgeContext.srcAttr.age),//sendMsg
      (x,y) => math.min(x,y) //mergeMsg
      )
  youngestFollower.collect()
  //
  // #2
  val youngestFollowee = graph.aggregateMessages[Int](
      edgeContext => edgeContext.sendToSrc(edgeContext.srcAttr.age),//sendMsg
      (x,y) => math.min(x,y) //mergeMsg
      )
  youngestFollowee.collect()
  //
  var iDegree = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToSrc(1),//sendMsg
    (x,y) => x+y //mergeMsg
    )
  iDegree.collect()
  graph.inDegrees.collect()
  //
  iDegree = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToDst(1),//sendMsg
    (x,y) => x+y //mergeMsg
    )
  iDegree.collect()
  graph.inDegrees.collect()
  //
  val oDegree = graph.aggregateMessages[Int](
    edgeContext => edgeContext.sendToSrc(1),//sendMsg
    (x,y) => x+y //mergeMsg
    )
  oDegree.collect()
  graph.outDegrees.collect()
  //
  val ranks = graph.pageRank(0.1).vertices
  val topVertices = ranks.sortBy(_._2,false).collect.foreach(println)
  //
  println("** That's All Folks ! **")
  println("** Done **")
  }
}