import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.collection.mutable._
import org.apache.spark.rdd.RDD

object AnalyzeGraph { 
	def main(args: Array[String]) { 
	val sc = new SparkContext(new SparkConf().setAppName("Analye Graph"))

	var base_path = "/user/dln"

 	if(args.length > 0) { base_path = args(0).trim() }

	println("Starting AnalyzeGraph")
	//val SourceID = "1002628280  "

	// Lets do all of the heavy lifting up front
	val Master_Graph_File = base_path+"/problem3/winklr-network.txt"
	val MasterGraph = GraphLoader.edgeListFile(sc,Master_Graph_File)
	MasterGraph.cache()
	MasterGraph.vertices.count()


	val MasterGraphPR = MasterGraph.pageRank(0.03)
	MasterGraphPR.cache()
	MasterGraphPR.vertices.count()
	// Now that that is done. 


	// This will be where we store everthing to output a file
	//
	//var Master_List = new ListBuffer[String]()
	var Global_Counter = 0
	//def record_to_list(argin1: String,argin2: (VertexId,Double)) { 

		// Convert the input arguments to the vertex to check followed by the total path length
	//	val Source_Vertex = argin1.trim().toLong
	//	val (vertin,number): (Long,Double) = argin2
	//	val tmpString = number.toString+","+Source_Vertex+","+vertin
	//	Master_List += tmpString
	//}


	def short_path_from_me(SourceID:String ) { 

		// Small Graph that contains only this user, followed by who they clicked on

		//val input_file = "/user/dln/problem3/inGraph/"+SourceID.trim()+".graph"
		val input_file = base_path+"/problem3/inGraph/"+SourceID.trim()+".graph"

		var ClickPairGraph = GraphLoader.edgeListFile(sc,input_file)
		ClickPairGraph.cache()
		
		// we don't need the full pagerank graph, so here we have a smaller one
		var MaskedMasterGraphPR = MasterGraphPR.mask(ClickPairGraph)
		MaskedMasterGraphPR.cache()

		// Using the argument passed in we create a graph using that vertexId as a point of origin
		var OriginGraph = MasterGraph.mapVertices((id, _) => if (id == SourceID.trim().toLong) 0.0 else Double.PositiveInfinity)

		var ShortestPathFromSource = OriginGraph.pregel(Double.PositiveInfinity)(
  			(id, dist, newDist) => math.min(dist, newDist), 
  			triplet => {  // Send Message
    				if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      					Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    				} else {
      					Iterator.empty
    				}
  			},
  			(a,b) => math.min(a,b)
  		)

		var PathGraph  = ShortestPathFromSource.mask(ClickPairGraph)

		var Influence = PathGraph.joinVertices(MasterGraph.inDegrees)((id,pathlength,indeg) => (1/pathlength)*indeg)

		var central_influence = Influence.joinVertices(MaskedMasterGraphPR.vertices)((id,dist,pagerank) => dist*pagerank)

		//
		// We want to eliminate the infinite, follow someone that there is in fact a path to
		//
		println("Processing " + central_influence.vertices.filter(_._2 < Double.PositiveInfinity).count())
		//central_influence.vertices.filter(_._2 < Double.PositiveInfinity).collect()foreach(record_to_list(SourceID,_))
		val save_file_name = base_path+"/problem3/OutGraph/"+SourceID.trim()+".data"
		central_influence.vertices.filter(_._2 < Double.PositiveInfinity).saveAsTextFile(save_file_name)

		// Let's purge some things from memory
		// As a general rule the Garbage collector should remove these things. 
	 	// However, this is a long process, and we should probably help out the garbage collector

		OriginGraph = OriginGraph.subgraph(vpred = (id,attr) => id == -1)
		PathGraph = PathGraph.subgraph(vpred = (id,attr) => id == -1)
		Influence = Influence.subgraph(vpred = (id,attr) => id == -1)
		ClickPairGraph = ClickPairGraph.subgraph(vpred = (id,attr) => id == -1)
		ShortestPathFromSource = ShortestPathFromSource.subgraph(vpred = (id,attr) => id == -1) 
		central_influence = central_influence.subgraph(vpred = (id,attr) => id == -1)

		// Calling gc every few iterations will be slower, but speed is not necessarily the goal
		Global_Counter += 1
		if((Global_Counter % 20) == 0) {
			System.gc
			println("Collecting Garbage")
		}
	}

	val from_vertex_file = base_path+"/problem3/from_vertices.dat"
	val raw_data = sc.textFile(from_vertex_file)
	raw_data.cache()
	println("Total Rows to process: " + raw_data.count())
	raw_data.collect().foreach(short_path_from_me)

	// At this point the Master_Map has been updated with all of the from and to vertices along with a recommendation score
	//val New_List = Master_List.sorted.reverse
	//val vertRDD = sc.parallelize(New_List)

	//vertRDD.saveAsTextFile("problem3/Suggest_Followers")

	sc.stop()
	}
}
