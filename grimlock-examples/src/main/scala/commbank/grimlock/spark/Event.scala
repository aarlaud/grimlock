// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.spark.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.nlp._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.transform._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1 }

// Define a simple event (structured) data type. It has an id, a type, a start time and duration. It applies to one or
// more instances and has a detailed information field.
case class ExampleEvent(
  eventId: String,
  eventType: String,
  startTime: java.util.Date,
  duration: Long,
  instances: List[String],
  details: String
)

object ExampleEvent {
  // Function to read a file with event data.
  def load(file: String)(implicit ctx: Context): RDD[Cell[StringValue :: HNil]] = ctx
    .spark
    .textFile(file)
    .flatMap { case line =>
      ExampleEventCodec.decode(line)
        .map(ev => Cell(Position(ev.eventId), Content(ExampleEventSchema, ExampleEventCodec.box(ev))))
    }
}

// Define a schema that specifies what legal values are for the example event. For this example, all events are valid.
case object ExampleEventSchema extends Schema[ExampleEvent] {
  val classification = StructuredType

  def validate(value: Value[ExampleEvent]): Boolean = true
}

// Define a value that wraps the event.
case class ExampleEventValue(value: ExampleEvent) extends Value[ExampleEvent] {
  val codec = ExampleEventCodec

  def compare[V <: Value[_]](that: V): Option[Int] = that match {
    case ExampleEventValue(e) => Option(compare(e))
    case _ => None
  }
}

// Define a codec for dealing with the example event. Note that comparison, for this example, is simply comparison
// on the event id.
case object ExampleEventCodec extends Codec[ExampleEvent] {
  type V = ExampleEventValue

  val date = None
  val numeric = None
  val integral = None

  def box(value: ExampleEvent) = ExampleEventValue(value)

  def compare(x: ExampleEvent, y: ExampleEvent): Int = x.eventId.compare(y.eventId)

  def decode(str: String): Option[ExampleEvent] = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parts = str.split("#")

    Option(
      ExampleEvent(
        parts(0),
        parts(1),
        dfmt.parse(parts(2)),
        parts(3).toLong,
        parts(4).split(",").toList,
        parts(5)
      )
    )
  }

  def encode(value: ExampleEvent): String = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    value.eventId + "#" +
    value.eventType + "#" +
    dfmt.format(value.startTime) + "#" +
    value.duration.toString +
    value.instances.mkString(",") + "#" +
    value.details
  }

  def toShortString: String = "example.event"
}

// Transformer for denormalising events; that is, create a separate cell in the matrix for each (event, instance) pair.
// Assumes that the initial position is 1D with event id (as is the output from `load` above).
case class Denormalise[
  P <: HList,
  Q <: HList
](
)(implicit
  ev: Position.AppendConstraints[P, StringValue, Q]
) extends Transformer[P, Q] {
  def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = cell.content.value match {
    case eev @ Value[ExampleEvent] =>
      for { iid <- eev.value.instances } yield Cell(cell.position.append(iid), cell.content)
    case _ => List()
  }
}

// For each event, get the details out. Split the details string, apply filtering, and (optionally) add ngrams. Then
// simply return the count for each term (word or ngram) in the document (i.e. event).
case class WordCounts[
  P <: HList,
  Q <: HList
](
  minLength: Long = Long.MinValue,
  ngrams: Int = 1,
  separator: String = "_",
  stopwords: List[String] = Stopwords.English
)(implicit
  ev: Position.AppendConstraints[P, StringValue, Q]
) extends Transformer[P, Q] {
  def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = cell.content.value match {
    case eev @ Value[ExampleEvent] =>
      // Get words from details. Optionally filter by length and/or stopwords.
      val words = eev.value.details
        .toLowerCase
        .split("""[ ,!.?;:"'#)($+></\\=~_&-@\[\]%`{}]+""")
        .toList
        .filterNot { case word =>
          word.isEmpty || word.exists(Character.isDigit) || word.length < minLength || stopwords.contains(word)
        }
      // Get terms from words. Optionally add ngrams.
      val terms = if (ngrams > 1) words ++ words.sliding(ngrams).map(_.mkString(separator)).toList else words

      // Return the term and it's count in the document.
      terms
        .groupBy(identity)
        .map { case (k, v) => Cell(cell.position.append(k), Content(DiscreteSchema[Long](), v.size)) }
        .toList
    case _ => List()
  }
}

// Simple tf-idf example (input data is same as tf-idf example here: http://en.wikipedia.org/wiki/Tf%E2%80%93idf).
object InstanceCentricTfIdf {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read event data, then de-normalises the events and return a 2D matrix (event id x instance id).
    val data = ExampleEvent.load(s"${path}/exampleEvents.txt")
      .transform(Denormalise())

    // For each event, append the word counts to the 3D matrix. The result is a 3D matrix (event id x instance id x word
    // count). Then aggregate out the event id. The result is a 2D matrix (instance x word count) where the counts are
    // the sums over all events.
    val tf = data
      .transform(WordCounts(stopwords = List()))
      .summarise(Along(_0))(Sums())

    // Get the number of instances (i.e. documents)
    val n = tf
      .size(_0)
      .compact(Over(_0))

    // Define extractor to get data out of map.
    def extractN[P <: HList] = ExtractWithKey[P, LongValue, Content](0).andThenPresent(_.value.asDouble)

    // Using the number of documents, compute Idf:
    //  1/ Compute document frequency;
    //  2/ Apply Idf transformation (using document count);
    //  3/ Compact into a Map for use in Tf-Idf below.
    val idf = tf
      .summarise(Along(_0))(Counts())
      .transformWithValue(n, Idf(extractN, (df, n) => math.log10(n / df)))
      .compact(Over(_0))

    // Define extractor to get data out of tf/idf map.
    def extract[
      P <: HList,
      D <: Nat,
      V <: Value[_]
    ](
      dimension: D
    )(implicit
      ev: Position.IndexConstraints[P, D, V]
    ) = ExtractWithDimension[P, D, V, Content](dimension).andThenPresent(_.value.asDouble)

    // Apply TfIdf to the term frequency matrix with the Idf values, then save the results to file.
    //
    // Uncomment one of the 3 lines below to try different tf-idf versions.
    tf
      //.transform(BooleanTf())
      //.transform(LogarithmicTf())
      //.transformWithValue(tf.summarise(Along(_1))(Maximum()).compact(Over(_0)), AugmentedTf(extract(_0)))
      .transformWithValue(idf, TfIdf(extract(_1)))
      .saveAsText(ctx, s"./demo.${output}/tfidf_entity.out", Cell.toShortString(true, "|"))
      .toUnit
  }
}

