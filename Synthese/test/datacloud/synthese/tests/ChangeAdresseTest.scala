package datacloud.synthese.tests

import org.apache.spark.SparkContext
import org.junit.Assert._
import org.junit.Test

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._

import datacloud.synthese.ImmatUtil
import scala.util.Random
import org.apache.spark.SparkConf


class ChangeAdresseTest {
  val sparkconf=new SparkConf().setAppName("test immat").setMaster("local[*]")
  @Test
  def test:Unit={
    val random=new Random(System.currentTimeMillis())
    println(CassandraSchemaBuilder.nb_proprios)
    val idproprio = "proprio"+random.nextInt(CassandraSchemaBuilder.nb_proprios)
    val nouvelle_adresse="nouvelle_adresse"+idproprio
    val sc =new SparkContext(sparkconf)
    ImmatUtil.changeAdresse(idproprio, nouvelle_adresse,sc)
    
    var cqlsession:CqlSession=null       
    try{
        cqlsession= CqlSession.builder().withKeyspace("immat").build();
        val select =  selectFrom("proprietaire").all()
                            .whereColumn("id")
                            .isEqualTo(literal(idproprio))
        val res = cqlsession.execute(select.build()).all()
        assertEquals(1,res.size())
        val adresse_lue = res.get(0).getString("adresse")
        assertEquals(nouvelle_adresse,adresse_lue)
    }finally{
        cqlsession.close()
        sc.stop()
    }
    
  }
  
}