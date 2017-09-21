// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.spark.distribution

import commbank.grimlock.framework.{ Cell, Locate, Matrix => FwMatrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distribution.{
  ApproximateDistribution => FwApproximateDistribution,
  CountMap,
  Quantiles,
  QuantileImpl,
  StreamingHistogram,
  TDigest
}
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.metadata.{ CategoricalType, DiscreteSchema, NumericType }
import commbank.grimlock.framework.position.{ Position, Slice }

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.tuner.MapMapSideJoin
import commbank.grimlock.spark.environment.tuner.SparkImplicits._
import commbank.grimlock.spark.Persist

import shapeless.{ =:!=, HList, HNil, Nat }
import shapeless.ops.hlist.Length
import shapeless.ops.nat.GT

/** Trait for computing approximate distributions from a matrix. */
trait ApproximateDistribution[
  P <: HList
] extends FwApproximateDistribution[P, Context]
  with Persist[Cell[P]] { self: FwMatrix[P, Context] =>
  def histogram[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    name: Locate.FromSelectedAndContent[S, Q],
    filter: Boolean
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[S, M],
    ev3: GT[L, M],
    ev4: FwApproximateDistribution.HistogramTuner[Context.U, T],
    ev5: Position.ListConstraints[Q]
  ): Context.U[Cell[Q]] = data
    .filter { case c => (!filter || c.content.schema.classification.isOfType(CategoricalType)) }
    .flatMap { case c => name(slice.selected(c.position), c.content) }
    .tunedSize(tuner)
    .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }

  def quantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantiles.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: FwApproximateDistribution.QuantilesTuner[Context.U, T],
    ev6: Position.ListConstraints[S]
  ): Context.U[Cell[Q]] = {
    val msj = Option(MapMapSideJoin[Position[S], Double, Long]())
    val qnt = QuantileImpl[P, S, Q](probs, quantiser, name, nan)

    val prep = data
      .collect { case c if (!filter || c.content.schema.classification.isOfType(NumericType)) =>
        (slice.selected(c.position), qnt.prepare(c))
      }

    prep
      .tunedJoin(tuner, prep.map { case (sel, _) => sel }.tunedSize(tuner), msj)
      .map { case (s, (q, c)) => ((s, c), q) }
      .tunedStream(tuner, (key, itr) => QuantileImpl.stream(qnt, key, itr.toList.sorted.toIterator))
      .map { case (_, c) => c }
  }

  def countMapQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantiles.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: FwApproximateDistribution.CountMapQuantilesTuner[Context.U, T],
    ev6: Position.ListConstraints[S]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.classification.isOfType(NumericType))
        Option((slice.selected(c.position), CountMap.from(c.content.value.asDouble.getOrElse(Double.NaN))))
      else
        None
    }
    .tunedReduce(tuner, CountMap.reduce)
    .flatMap { case (pos, t) => CountMap.toCells(t, probs, pos, quantiser, name, nan) }

  def tDigestQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    compression: Double,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: FwApproximateDistribution.TDigestQuantilesTuner[Context.U, T],
    ev6: Position.ListConstraints[S]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.classification.isOfType(NumericType))
        c.content.value.asDouble.flatMap { case d =>
          TDigest.from(d, compression).map { case td => (slice.selected(c.position), td) }
        }
      else
        None
    }
    .tunedReduce(tuner, TDigest.reduce)
    .flatMap { case (pos, t) => TDigest.toCells(t, probs, pos, name, nan) }

  def uniformQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    count: Long,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: FwApproximateDistribution.UniformQuantilesTuner[Context.U, T],
    ev6: Position.ListConstraints[S]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.classification.isOfType(NumericType))
        c.content.value.asDouble.map { case d => (slice.selected(c.position), StreamingHistogram.from(d, count)) }
      else
        None
    }
    .tunedReduce(tuner, StreamingHistogram.reduce)
    .flatMap { case (pos, t) => StreamingHistogram.toCells(t, count, pos, name, nan) }
}

