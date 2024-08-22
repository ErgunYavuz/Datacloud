import org . apache . spark . _
import com . datastax . spark . connector . _
import org . apache . spark . sql . _
import com.datastax.oss.driver.api.core.CqlSession

object Essai extends App {
    org.apache.log4j.Logger.getLogger( "org.apache.spark").setLevel(org.apache.log4j.Level.OFF)
    val conf = new SparkConf().setAppName ( "SparkonCassandra" ).setMaster ( "local[4]" )
        .set("spark.cassandra.connection.host" ,"localhost")
    val sc = new SparkContext ( conf )
    val rdd = sc . cassandraTable ( "test" , "kv" )
    println ( rdd . count )
    println ( rdd . first )
    println ( rdd . map ( _ . getInt ( "value" )). reduce ( _ + _ ))
    val rdd2 = sc . parallelize ( Seq (( "key3" , 3) , ( "key4" , 4)))
    rdd2 . saveToCassandra ( "test" , "kv" , SomeColumns ( "key" , "value" ))
    sc . stop ()
}