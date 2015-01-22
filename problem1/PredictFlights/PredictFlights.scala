import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.optimization.{SquaredL2Updater,L1Updater }
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd._
import scala.collection.mutable._

// load the historical data previously formatted in libSVM format
object PredictFlights { 
	def main(args: Array[String]) { 
	var base_path = "/user/dln"
	if(args.length > 0) {
		base_path = args(0).trim() } 


	val sc = new SparkContext(new SparkConf().setAppName("PredictFlights"))


	var Master_List = ListBuffer[String]()
	var Recover_File_List = ListBuffer[String]()


	def record_prediction(arg1: (Long,Double),arg2: Double) { 
		// Parse the passed in data to some useful variables
		val (flightid,score): (Long,Double) = arg1
		val auroc: Double = arg2
		// The Score multipled by the auroc for this particular point of origin will be
		// used later for sorting
		val sort_order = score*auroc
	        // Let's make a string here to add to the Master List buffer	
		val tmpString = sort_order.toString +","+flightid.toString
		Master_List += tmpString
	}

	def train_and_predict(airport: String) { 
		//val input_file = "/user/dln/problem1/svm/"+airport+".svm_input"
		//val predict_file = "/user/dln/problem1/svm/"+airport+".svm_predict"

		val input_file = base_path+"/problem1/svm/"+airport+".svm_input"
		val predict_file = base_path+"/problem1/svm/"+airport+".svm_predict"

		val tst1 = sc.textFile(input_file)
		// check the size of the input file to avoid an error upon opening an empty file
		if (tst1.count() == 0 ){ 
			// We are just not going to do anything for this airport
			// However, before we break out of here, record the predictionfile
			Recover_File_List += predict_file
			return
		}
		val data = MLUtils.loadLibSVMFile(sc, input_file)
		data.cache()
		val tst2 = sc.textFile(predict_file)
		if (tst2.count() == 0 ){ 
			// chances are if we are here, we are good, however, just in case anythin odd happens
			// don't freak out over an empty file.
			return
		}
		// load the prediction data previously formatted in libSVM format
		val predict = MLUtils.loadLibSVMFile(sc,predict_file)
		predict.cache()
		// Split the data into mostly training data
		val splits = data.randomSplit(Array(0.82, 0.18), seed = 11241969L)

		val training_input = splits(0).cache()
		val test_input = splits(1).cache()

		val general_scaler = new StandardScaler(withMean = true, withStd = true).fit(training_input.map(x => x.features))
		val training_scaled_data = training_input.map(x => LabeledPoint(x.label, general_scaler.transform(Vectors.dense(x.features.toArray))))
		training_scaled_data.cache
		val test_scaled_data = test_input.map(x => LabeledPoint(x.label, general_scaler.transform(Vectors.dense(x.features.toArray))))
		test_scaled_data.cache
		val predict_scaled_data = predict.map(x => LabeledPoint(x.label, general_scaler.transform(Vectors.dense(x.features.toArray))))
		predict_scaled_data.cache

		// Build the SVM model
		val svmSGD = new SVMWithSGD()
		val numIterations = 450
		svmSGD.optimizer.setNumIterations(numIterations).setRegParam(1.0).setStepSize(1.0)
		val model = svmSGD.run(training_scaled_data)
		val TestScoreAndLabels = test_scaled_data.map { point =>
  			val score = model.predict(point.features)
  			(score,point.label)
		}

		val testmetrics = new BinaryClassificationMetrics(TestScoreAndLabels)
		val testauROC = testmetrics.areaUnderROC()
		println("For Airport: "+ airport)
		println("Area under the ROC = " + testauROC)
		val PublishScoreAndLabels = predict_scaled_data.map { point =>
  			val score = model.predict(point.features)
  			(point.label.toLong,score)
 		}
		PublishScoreAndLabels.collect.foreach(record_prediction(_,testauROC))
		data.unpersist()
		training_scaled_data.unpersist()
		test_scaled_data.unpersist()
		predict_scaled_data.unpersist()
		System.gc
	}
	def add_to_master(label_data: LabeledPoint) { 
		val tmpString = "0.0,"+label_data.label.toLong.toString
		Master_List += tmpString
	}
		
	def process_recover_file(predict_file: String) { 
		val predict = MLUtils.loadLibSVMFile(sc,predict_file)
		predict.cache()
		predict.collect().foreach(add_to_master)
	}

	//val driver = sc.textFile("/user/dln/problem1/driver/origin.dat")
	val driver_file_name = base_path+"/problem1/driver/origin.dat"
	val driver = sc.textFile(driver_file_name)

	driver.collect().foreach(train_and_predict)
	// after we have "accurately" predicted the possible delays
	// for airports that have data. Let's check the Recover_File_List. 
	// for each flight assigned to an airport that is "New" assume that there should be 
	// no delays this accounts for 318 flights.  
	Recover_File_List.foreach(process_recover_file)

	
	val New_List = Master_List.sorted.reverse

	val Flight_ListRDD = sc.parallelize(New_List)
	Flight_ListRDD.saveAsTextFile("problem1/Flight_List")

	}
}
