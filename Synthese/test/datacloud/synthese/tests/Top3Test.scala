package datacloud.synthese.tests

import java.util.HashMap

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.junit.Test

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder._
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder._
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable

import datacloud.synthese.ImmatUtil

class  Top3Test {
  val sparkconf=new SparkConf().setAppName("test immat").setMaster("local[*]")
  @Test
  def testtop3:Unit={
   val sc =new SparkContext(sparkconf)
   Array(2016,2017).foreach(annee=>{
     val counters = scala.collection.mutable.Map[String,Int]()
     
     val computed  = ImmatUtil.topThree(annee, sc)
     var cqlsession:CqlSession=null
      try{
        cqlsession= CqlSession.builder().withKeyspace("immat").build();
        val select =  selectFrom("immatriculation").all().allowFiltering()
                            .whereColumn("date_mise_circu")
                            .isEqualTo(literal(annee))
        val res = cqlsession.execute(select.build()).all().asScala
        val x =res.map(row=>row.getString("idmodele")).map(idmodele=>{
          val selectmodele = selectFrom("modelevehicule").all()
                                    .whereColumn("id")
                                    .isEqualTo(literal(idmodele))
          val resmodele = cqlsession.execute(selectmodele.build()).one()
          val marque = resmodele.getString("marque")
          val cpt = counters.getOrElse(marque, 0)
          counters.put(marque, cpt+1)
        })
        val tmp =counters.toSeq.sortBy(_._2).map(_._1).reverse
        val expected = tmp.slice(0,3)
        
        assertEquals(expected.size,computed.size)
        var i=0
        for(c<- computed){
          assertEquals(expected(i), c)
          i+=1
        }
        
        assertEquals(expected.size,computed.size)
        
    }finally{
        cqlsession.close()
    }
     
   })
    sc.stop()
  }
}