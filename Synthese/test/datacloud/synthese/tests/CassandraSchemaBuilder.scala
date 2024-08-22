package datacloud.synthese.tests

import java.util.HashMap

import scala.util.Random

import org.apache.spark.SparkConf

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto
import com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropTable
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable


object CassandraSchemaBuilder extends App {
  
  def nb_proprios=50
  def nb_modeles=20
  def nb_immatriculations=300
  val random=new Random(20)
  
  
  createSchema
  fillTables
  
  def fillTables={
    
   
    //creation des proprios
    
    val threadspriopro= Range.apply(0, nb_proprios).map(i=>{
      new Runnable(){
        def run:Unit={
           var cqlsession:CqlSession=null       
            try{
             cqlsession= CqlSession.builder().withKeyspace("immat").build();
             val id="proprio"+i
             val nom="nom"+random.nextInt(50)
             val prenom="prenom"+random.nextInt(50) 
             
             var insert = insertInto("Proprietaire").value("id", literal(id))
                                                    .value("nom", literal(nom))
                                                    .value("prenom", literal(prenom))
                                                    .value("date_naissance", literal("datenaissance_"+id) )
                                                    .value("adresse", literal("addresse_"+id) )
            
             cqlsession.execute(insert.build())
            }finally{
             cqlsession.close()
            }
        }
      }
    }).map(r=> new Thread(r))
    threadspriopro.foreach(_.start())
    
    
     //creation des modeles
    
    val threadsmodeles= Range.apply(0, nb_modeles).map(i=>{
      new Runnable(){
        def run:Unit={
           var cqlsession:CqlSession=null       
            try{
             cqlsession= CqlSession.builder().withKeyspace("immat").build();
             val id="modele"+i
             val marque="marque"+random.nextInt(10)
             val modele="modele"+random.nextInt(10) 
             val annee_sortie=random.nextInt(5)+2010
             var insert = insertInto("ModeleVehicule").value("id", literal(id))
                                                    .value("marque", literal(marque))
                                                    .value("modele", literal(modele))
                                                    .value("annee_sortie", literal(annee_sortie) )
                                                    
            
             cqlsession.execute(insert.build())
            }finally{
             cqlsession.close()
            }
        }
      }
    }).map(r=> new Thread(r))
     threadsmodeles.foreach(_.start())
    
    //creation des immatriculations
    
    val statuts=Array("circulation","vole","detruit")
    val threadsimmatriculations= Range.apply(0, nb_immatriculations).map(i=>{
      new Runnable(){
        def run:Unit={
           var cqlsession:CqlSession=null       
            try{
             cqlsession= CqlSession.builder().withKeyspace("immat").build();
             val numero="immatriculation"+i
             val date_mise_circu=2015+random.nextInt(3)
             val statut=statuts(random.nextInt(statuts.length))
             val idmodele="modele"+random.nextInt(nb_modeles)
             val map = new HashMap[Int,String]
             for(x<-0 to (random.nextInt(3)+1)){
               map.put(date_mise_circu+x, "proprio"+random.nextInt(nb_proprios) )
             }
             
             
             val annee_sortie=random.nextInt(10)+2010
             var insert = insertInto("Immatriculation").value("numero", literal(numero))
                                                    .value("date_mise_circu", literal(date_mise_circu))
                                                    .value("statut", literal(statut))
                                                    .value("idmodele", literal(idmodele) )
                                                    .value("proprios", literal(map) )
                                                    
            
             cqlsession.execute(insert.build())
            }finally{
             cqlsession.close()
            }
        }
      }
    }).map(r=> new Thread(r))
    threadsimmatriculations.foreach(_.start())
    
    
    
    
   
    threadspriopro.foreach(_.join())
    threadsmodeles.foreach(_.join())
    threadsimmatriculations.foreach(_.join())
    
  }
  
  def createSchema:Unit={
       var cqlsession:CqlSession=null 
    try{
       cqlsession= CqlSession.builder().build();
       val createKs:CreateKeyspace  = createKeyspace("immat").ifNotExists().withSimpleStrategy(1);
       cqlsession.execute(createKs.build())
      

       val dropTableImmat = dropTable("immat", "Immatriculation").ifExists()
       cqlsession.execute(dropTableImmat.build())
       var createTableImmat:CreateTable = createTable("immat", "Immatriculation").ifNotExists().withPartitionKey("numero", DataTypes.ASCII)
       createTableImmat=createTableImmat.withColumn("date_mise_circu", DataTypes.INT)
       createTableImmat=createTableImmat.withColumn("statut", DataTypes.ASCII)
       createTableImmat=createTableImmat.withColumn("idmodele", DataTypes.ASCII)
       createTableImmat=createTableImmat.withColumn("proprios", DataTypes.mapOf(DataTypes.INT, DataTypes.ASCII))
       cqlsession.execute(createTableImmat.build())
       
       
       val dropTableModele=dropTable("immat", "ModeleVehicule").ifExists()
       cqlsession.execute(dropTableModele.build())
       var createTableModele:CreateTable = createTable("immat", "ModeleVehicule").ifNotExists().withPartitionKey("id", DataTypes.ASCII)
       createTableModele=createTableModele.withColumn("marque", DataTypes.ASCII)
       createTableModele=createTableModele.withColumn("modele", DataTypes.ASCII)
       createTableModele=createTableModele.withColumn("annee_sortie", DataTypes.INT)
       cqlsession.execute(createTableModele.build())
       
       
       val dropTableProprietaire=dropTable("immat", "Proprietaire").ifExists()
       cqlsession.execute(dropTableProprietaire.build())
       var createTableProprietaire:CreateTable = createTable("immat", "Proprietaire").ifNotExists().withPartitionKey("id", DataTypes.ASCII)
       createTableProprietaire=createTableProprietaire.withColumn("nom", DataTypes.ASCII)
       createTableProprietaire=createTableProprietaire.withColumn("prenom", DataTypes.ASCII)
       createTableProprietaire=createTableProprietaire.withColumn("date_naissance", DataTypes.ASCII)
       createTableProprietaire=createTableProprietaire.withColumn("adresse", DataTypes.ASCII)
       cqlsession.execute(createTableProprietaire.build())
       
       
     }finally{
       cqlsession.close()
     }
  }
  
}

