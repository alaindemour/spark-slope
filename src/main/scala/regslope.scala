
package metrics

import java.time._

import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row


class RegSlope extends UserDefinedAggregateFunction {

  val t0 = LocalDate.of(2000,1,1)

  def xtime(y : Long ,m : Long): Long = {
    val t = LocalDate.of(y.toInt ,m.toInt ,1)
    val timecoord = t0.until(t)
    timecoord.toTotalMonths
  }


  override def inputSchema: StructType = StructType(
    Array(
      StructField("year", LongType, true),
      StructField("production", LongType, true)
    ))

  override def bufferSchema : StructType =
    StructType(
      Array(
        StructField("sumx", DoubleType) ,
        StructField("sumy", DoubleType),
        StructField("sumxy", DoubleType),
        StructField("sumx2", DoubleType),
        StructField("i", LongType)
      ))

  // output type of the aggregatation function.
  override def dataType: DataType = DoubleType

  //
  override def deterministic: Boolean = true

  // initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0 // sum x
    buffer(1) = 0.0 // sum y
    buffer(2) = 0.0 // sum xy
    buffer(3) = 0.0 // sum x2
    buffer(4) = 0L // count i
  }

  // how to update the buffer given an input row
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val y = input.getAs[Long](0)
    val m : Long = 2
    val p = input.getAs[Long](1)
    val x = xtime(y,m)
    buffer(0) = buffer.getAs[Double](0) + x// sum x
    buffer(1) = buffer.getAs[Double](1) + p // sum y
    buffer(2) = buffer.getAs[Double](2) + x * p // sum xy
    buffer(3) = buffer.getAs[Double](3) + x * x // sum x2
    buffer(4) = buffer.getAs[Long](4) + 1
  }

  // how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
    buffer1(3) = buffer1.getAs[Double](3) + buffer2.getAs[Double](3)
    buffer1(4) = buffer1.getAs[Long](4) + buffer2.getAs[Long](4)
  }

  // final value, we have all the sums and we compute the slope
  override def evaluate(buffer: Row): Any = {
    val sumx = buffer.getDouble(0)
    val sumy = buffer.getDouble(1)
    val sumxy = buffer.getDouble(2)
    val sumx2 = buffer.getDouble(3)
    val i = buffer.getLong(4)
    (i * sumxy - (sumx * sumy)) / (i * sumx2 - (sumx * sumx))
  }

}

