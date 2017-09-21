// Copyright 2016,2017 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.spark.environment

import com.twitter.scalding.parquet.scrooge.ScroogeReadSupport
import com.twitter.scrooge.ThriftStruct

import commbank.grimlock.framework.{ Cell, Persist }
import commbank.grimlock.framework.environment.{ Context => FwContext }

import commbank.grimlock.spark.environment.implicits.Implicits

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job

import org.apache.parquet.hadoop.ParquetInputFormat

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.{ classTag, ClassTag }

import shapeless.HList

/**
 * Spark operating context state.
 *
 * @param spark The Spark context.
 */
case class Context(spark: SparkContext) extends FwContext[Context] {
  type E[A] = Context.E[A]

  type U[A] = Context.U[A]

  def loadText[
    P <: HList
  ](
    file: String,
    parser: Persist.TextParser[Cell[P]]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val rdd = spark.textFile(file).flatMap { case s => parser(s) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: HList
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, Cell[P]]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val rdd = spark.sequenceFile[K, V](file).flatMap { case (k, v) => parser(k, v) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadParquet[
    T <: ThriftStruct : Manifest,
    P <: HList
  ](
    file: String,
    parser: Persist.ParquetParser[T, Cell[P]]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val job = Job.getInstance()

    ParquetInputFormat.setReadSupportClass(job, classOf[ScroogeReadSupport[T]])

    val rdd = spark.newAPIHadoopFile(
      file,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      classTag[T].runtimeClass.asInstanceOf[Class[T]],
      job.getConfiguration
    ).flatMap { case (_, v) => parser(v) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  val implicits = Implicits()

  def empty[T : ClassTag]: Context.U[T] = spark.parallelize(List.empty[T])

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = spark.parallelize(seq)

  def nop(): Unit = ()
}

/** Companion object to `Context` with implicit. */
object Context {
  /** Type for user defined data. */
  type E[A] = A

  /** Type for distributed data. */
  type U[A] = RDD[A]
}

