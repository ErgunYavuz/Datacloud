package revision

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs
import org.apache.hadoop.shaded.com.squareup.okhttp.internal.io.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class BigGraph [T](edges: RDD[edge[T]]){
    def this(sc : SparkContext, url : String, f: String => T)= {
        this(sc.textFile(url).map(_.split(" ")).map(x=> (f(x(0)), f(x(1)))).map(x => edge(x._1, x._2)))
    }
    def save (url: String, f: T =>String): Unit ={
        val rdd = edges.map(x => f(x.source)+" "+f(x.dest))
        rdd.saveAsTextFile(url)
    }

    def vertices():RDD[T] ={
        val rdd1 = edges.map(x => x.source)
        val rdd2 = edges.map(x => x.dest)
        val rdd3 = rdd1.union(rdd2).distinct()
        rdd3
    }

    def neighbors() : RDD[(T, Iterable[T])] =
        edges.map(x=> x.toCouple).groupByKey()

    def TC: BigGraph[T] = {
        var res = edges
        implicit var n;
        val e = edges.map(_.reverse).map(_.toCouple).cache
        var oldCount = 0L
        var nextCount = res.count()
        do {
            oldCount = nextCount
            val a = res.map(_.toCouple).join(e)
            val b = a.map(c => edge(c._2._2, c._2._1))
            val c = res.union(b)
            res = c.distinct(edges.getNumPartitions).cache
            nextCount = res.count()
        }
        while (nextCount != oldCount)
        return new BigGraph(res);
    }

    import org.apache.hadoop.fs.FileSystem
    import org.apache.hadoop.fs.Path
    import org.apache.hadoop.conf.Configuration

    def

    def recmapreduce[K, V](sc: SparkContext,pathin: Path
    , mapfunction: (String, String) => TraversableOnce[(K, V)]
    , reduceop: (V, V) => V)(implicit kt: ClassTag[K], vt: ClassTag[V]): RDD[(K, V)] = {
        val hdfs = FileSystem.get(new Configuration())
        val c = hdfs.listStatus(pathin).map(s=> s.getPath)
        val d = c.map(p=>recmapreduce(sc, p, mapfunction, reduceop))
    }

}


