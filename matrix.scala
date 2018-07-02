// import org.apache.spark.SparkConf
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._

import scala.collection.Iterator
import scala.util.{Random => rand}



object Matrix {
  def mulLazy(m1: Iterator[Array[Double]], m2: Matrix): Iterator[Array[Double]] = {
    m1.grouped(8).map { group =>
      group.par.map(m2.mulRow).toIterator
    }.flatten
  }
}

class Matrix(val rowVectors: Array[Array[Double]]) {
  val columns = rowVectors.head.size
  val rows = rowVectors.size

  private def mulRow(otherRow: Array[Double]): Array[Double] = {
    val rowVectors = this.rowVectors
    val result = Array.ofDim[Double](columns)
    var i = 0
    while(i < otherRow.size) {
      val value = otherRow(i)
      if(value != 0) { //optimization for sparse matrix
        val row = rowVectors(i)
        var col = 0
        while(col < result.size) {
          result(col) += value * row(col)
          col += 1
        }
      }
      i += 1
    }
    result
  }

  def *(other: Matrix): Matrix = {
    new Matrix(rowVectors.par.map(other.mulRow).toArray)
  }

  def equals(other: Matrix): Boolean = {
    java.util.Arrays.deepEquals(this.rowVectors.asInstanceOf[Array[Object]], other.rowVectors.asInstanceOf[Array[Object]])
  }

  override def equals(other: Any): Boolean = {
    if(other.isInstanceOf[Matrix]) equals(other.asInstanceOf[Matrix]) else false
  }

  override def toString = {
    rowVectors.map(_.mkString(", ")).mkString("\n")
  }
}

def randMatrix(rows: Int, columns: Int): Matrix = {
  new Matrix((1 to rows).map(_ => Array.fill(columns)(rand.nextDouble * 100)).toArray)
}

def sparseRandMatrix(rows: Int, columns: Int, ratio: Double): Matrix = {
  new Matrix((1 to rows).map(_ => Array.fill(columns)(if(rand.nextDouble > ratio) 0 else rand.nextDouble * 100)).toArray)
}

val N = 2000

val m1 = sparseRandMatrix(N, N, 0.1) // only 10% of the numbers will be different from 0
val m2 = randMatrix(N, N)

val m3 = m1.rowVectors.toIterator

val m12 = time("m1 * m2")(m1 * m2)
val m32 = time("m3 * m2")(Matrix.mulLazy(m3, m2)) //doesn't take much time because the matrix multiplication is lazy

println(m32)

println("m12 == m32 = " + (new Matrix(m32.toArray) == m12))
