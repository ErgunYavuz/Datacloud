package datacloud.spark.core.matrix.test

import java.nio.file.Files
import org.junit.Test
import org.junit.Assert._
import datacloud.spark.core.matrix.VectorInt
import datacloud.spark.core.matrix.MatrixIntAsRDD
import datacloud.spark.core.matrix.MatrixIntAsRDD._
import java.io.PrintWriter
import java.io.FileWriter

class TransposeTest extends AbstractTest {
  
  @Test
      def testmaking:Unit={
          val sizes = List(4,10,50,500)
      		for(nb <- sizes){
      				val nb_partitions= Math.sqrt(nb).ceil.toInt
      				val file1 = Files.createTempFile("matrice", ".dat")
      				val marray1 =  Util.createRandomMatrixInt(nb, nb, file1 )
      				val matrdd1 = makeFromFile(file1.toUri().toASCIIString(), nb_partitions, sc)
       				      				
      				val mattranspose = matrdd1 .transpose()
      				
      				val file_expected = Files.createTempFile("matrice", ".dat")
      				val w= new PrintWriter(new FileWriter(file_expected.toFile()))
              for(i<-0 until nb){
                for(j <- 0 until nb){
                  if(j>0) w.print(" ")
                  w.print(marray1(j)(i))
                }
                if(i < nb-1)w.println("")
              }   
              w.close
      				val mat_expected = makeFromFile(file_expected.toUri().toASCIIString(), nb_partitions, sc)	      				
      				assertEquals(mat_expected,mattranspose)
      				file1.toFile().delete()
      				file_expected.toFile().delete()
      		}
      }
}