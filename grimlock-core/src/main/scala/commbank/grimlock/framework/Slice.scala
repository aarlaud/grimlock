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

package commbank.grimlock.framework.position

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.ops.hlist.{ At, IsHCons, Prepend, Split }

/** Trait that encapsulates dimension on which to operate. */
sealed trait Slice[P <: HList] {
  /**
   * Return type of the `selected` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type S <: HList

  /**
   * Return type of the `remainder` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type R <: HList

  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: Position[P]): Position[S]

  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: Position[P]): Position[R]
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed,
 * it is performed using a `Position[Val :: HNil]` consisting of the coordinate at index `dimension`.
 *
 * @param dimension Dimension of the selected coordinate.
 */
case class Over[
  P <: HList,
  D <: Nat,
  Val,
  Pre <: HList,
  Suf <: HList,
  Tai <: HList,
  Out <: HList
](
  dimension: D
)(implicit
  ev1: At.Aux[P, D, Val],
  ev2: Split.Aux[P, D, Pre, Suf],
  ev3: IsHCons.Aux[Suf, _, Tai],
  ev4: Prepend.Aux[Pre, Tai, Out]
) extends Slice[P] {
  type S = Val :: HNil
  type R = Out

  def selected(pos: Position[P]): Position[S] = Position(pos(dimension))
  def remainder(pos: Position[P]): Position[R] = pos.remove(dimension)
}

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position[Out]` consisting of all coordinates except that at index
 * `dimension`.
 *
 * @param dimension Dimension of the coordinate to exclude.
 */
case class Along[
  P <: HList,
  D <: Nat,
  Val,
  Pre <: HList,
  Suf <: HList,
  Tai <: HList,
  Out <: HList
](
  dimension: D
)(implicit
  ev1: At.Aux[P, D, Val],
  ev2: Split.Aux[P, D, Pre, Suf],
  ev3: IsHCons.Aux[Suf, _, Tai],
  ev4: Prepend.Aux[Pre, Tai, Out]
) extends Slice[P] {
  type S = Out
  type R = Val :: HNil

  def selected(pos: Position[P]): Position[S] = pos.remove(dimension)
  def remainder(pos: Position[P]): Position[R] = Position(pos(dimension))
}

