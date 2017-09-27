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
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

class Scoring(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
  val (data, _) = ctx
    .loadText(s"${path}/exampleInput.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

  // Read the statistics (ignoring errors) from the PipelineDataPreparation example.
  val stats = ctx
    .loadText(s"./demo.${output}/stats.out", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
    .data
    .compact(Over(_0))

  // Read externally learned weights (ignoring errors).
  val weights = ctx
    .loadText(s"${path}/exampleWeights.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
    .data
    .compact(Over(_0))

  // Define extract object to get data out of statistics map.
  def extractStat[
    P <: HList,
    V <: Value[_]
  ](
    key: String
  )(implicit
    ev: Position.IndexConstraints[P, _1, V]
  ) = ExtractWithDimensionAndKey[P, _1, V, StringValue, Content](_1, key).andThenPresent(_.value.asDouble)

  // Define extract object to get data out of weights map.
  def extractWeight[
    P <: HList,
    V <: Value[_]
  ](implicit
    ev: Position.IndexConstraints[P, _1, V]
  ) = ExtractWithDimension[P, _1, V, Content](_1).andThenPresent(_.value.asDouble)

  // For the data do:
  //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
  //  2/ Compute the scored (as a weighted sum);
  //  3/ Save the results.
  data
    .transformWithValue(
      stats,
      Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
      Binarise(Locate.RenameDimensionWithContent(_1)),
      Clamp(extractStat("min"), extractStat("max"))
        .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd")))
    )
    .summariseWithValue(Over(_0))(weights, WeightedSums(extractWeight))
    .saveAsText(ctx, s"./demo.${output}/scores.out", Cell.toShortString(true, "|"))
    .toUnit
}

