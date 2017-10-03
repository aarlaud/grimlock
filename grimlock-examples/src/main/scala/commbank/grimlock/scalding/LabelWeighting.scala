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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.transform._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ ::, HList, HNil }
import shapeless.nat.{ _0, _1 }

// Simple transformer that adds weight to a label.
case class AddWeight[
  P <: HList,
  Q <: HList
](implicit
  ev: Position.AppendConstraints[P, StringValue, Q]
) extends TransformerWithValue[P, Q] {
  type V = Map[Position[StringValue :: HNil], Content]

  // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
  // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = List(
    Cell(cell.position.append("label"), cell.content),
    Cell(cell.position.append("weight"), ext(Position(cell.content.value.toShortString)))
  )
}

class LabelWeighting(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read labels and melt the date into the instance id to generate a 1D matrix.
  val labels = ctx
    .loadText(
      s"${path}/exampleLabels.txt",
      Cell.shortStringParser(
        StringCodec :: StringCodec :: HNil,
        Content.decoder(DoubleCodec, ContinuousSchema[Double]()),
        "|"
      )
    )
    .data // Keep only the data (ignoring errors).
    .melt(_1, _0, Value.concatenate[StringValue, StringValue](":"))

  // Compute histogram over the label values.
  val histogram = labels
    .histogram(Along(_0))(Locate.AppendContentString, false)

  // Compute the total number of labels and compact result into a Map.
  val sum = labels
    .size(_0)
    .compact(Over(_0))

  // Define extract object to get data out of sum/min map.
  def extractor[
    P <: HList,
    K <: Value[_]
  ](
    key: Position[K :: HNil]
  ) = ExtractWithKey[P, K, Content](key).andThenPresent(_.value.asDouble)

  // Compute the ratio of (total number of labels) / (count for each label).
  val ratio = histogram
    .transformWithValue(sum, Fraction(extractor(0), true))

  // Find the minimum ratio, and compact the result into a Map.
  val min = ratio
    .summarise(Along(_0))(Minimum().andThenRelocate(_.position.append("min").toOption))
    .compact(Over(_0))

  // Divide the ratio by the minimum ratio, and compact the result into a Map.
  val weights = ratio
    .transformWithValue(min, Fraction(extractor("min")))
    .compact(Over(_0))

  // Re-read labels and add the computed weight.
  ctx
    .loadText(
      s"${path}/exampleLabels.txt",
      Cell.shortStringParser(
        StringCodec :: StringCodec :: HNil,
        Content.decoder(DoubleCodec, ContinuousSchema[Double]()),
        "|"
      )
    )
    .data // Keep only the data (ignoring errors).
    .transformWithValue(weights, AddWeight())
    .saveAsText(ctx, s"./demo.${output}/weighted.out", Cell.toShortString(true, "|"))
    .toUnit
}

