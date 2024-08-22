package revision

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import scala.reflect.ClassTag

object Util {
    val hdfs = FileSystem.get(new Configuration)

    def recmapreduce[K,V](sc:SparkContext,
        pathin:Path,
        mapfunction:(String,String)=>TraversableOnce[(K,V)],
        reduceop:(V,V)=>V)
    (implicit kt: ClassTag[K], vt: ClassTag[V]):RDD[(K,V)]={
        if(hdfs.getFileStatus(pathin).isFile()){//est-ce un fichier régulier
            val a = sc.textFile(pathin.toString())
            val b = a.flatMap(mapfunction(_,pathin.toString()))
            return b.reduceByKey(reduceop)
            }else{//cas d’un répertoire
            /*la méthode listStatus renvoie un Array[FileStatus].
* ce qui permet d’avoir la liste des fichiers dans le répertoire
*/
            val c = hdfs.listStatus(pathin).map(s=> s.getPath)
            val d = c.map(p=>recmapreduce(sc, p, mapfunction, reduceop))
            val e = d.reduce(_ union _)
            return e.reduceByKey(reduceop,numPartitions=1)
            }
        }
    }