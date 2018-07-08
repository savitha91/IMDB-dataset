import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import sqlContext.implicits._
import org.apache.spark.sql._

/**
 * Mode is yarn mode. 199 partitions were created
 */
class Movies {
   def main(args: Array[String]): Unit = {    
     val conf = new SparkConf().setAppName("My App") 
      val sc = new SparkContext(conf)
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     
     case class Movie(movieId: Int, title: String, genres: Seq[String])
     case class Rating(userId: Int, movieId: Int, rating: Float, timestamp:String)
     case class User(userId:Int, gender:String, age:Int, occupation:String, zip:String)
   
     def parseMovie(lines:String):Movie={
       val fields = lines.split("::")
       assert(fields.size == 3)
       Movie(fields(0).toInt,fields(1),Seq(fields(2)))
     }
     
     def parseUser(lines:String):User={
       val fields = lines.split("::")
       assert(fields.size == 5)
       User(fields(0).toInt,fields(1),fields(2).toInt, fields(3),fields(4))  
     }
     
      def parseRating(lines:String):Rating={
       val fields = lines.split("::")
       assert(fields.size == 4)
       Rating(fields(0).toInt,fields(1).toInt,fields(2).toFloat, fields(3))  
     }
     
     val movieDf = sc.textFile("/user/cloudera/SparkAssign/movies.dat").map(parseMovie).toDF()
     val userDf = sc.textFile("/user/cloudera/SparkAssign/users.dat").map(parseUser).toDF()
     val ratingDf = sc.textFile("/user/cloudera/SparkAssign/ratings.dat").map(parseRating).toDF()
     
     //create tables
     movieDf.registerTempTable("Movie")
     userDf.registerTempTable("User")
     ratingDf.registerTempTable("Rating")
     }
     }
