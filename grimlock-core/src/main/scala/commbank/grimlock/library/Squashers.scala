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

package commbank.grimlock.library.squash

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.squash.Squasher

import  scala.reflect.classTag

import shapeless.{ HList, Nat }
import shapeless.ops.nat.{ LTEq, ToInt }

private[squash] object PreservingPosition {
  type T = (Value, Content[_])

  val tTag = classTag[T]

  def prepare[P <: HList, D <: Nat : ToInt](cell: Cell[P], dim: D)(implicit ev: LTEq[D, P]): Option[T] =
    Option((cell.position(dim), cell.content))

  def reduce(maximum: Boolean)(lt: T, rt: T): T = {
    val (min, max) = if (Value.ordering.compare(lt._1, rt._1) > 0) (rt, lt) else (lt, rt)

    if (maximum) max else min
  }

  def present(t: T): Option[Content[_]] = Option(t._2)
}

/** Reduce two cells preserving the cell with maximal value for the coordinate of the dimension being squashed. */
case class PreservingMaximumPosition[P <: HList]() extends Squasher[P] {
  type T = PreservingPosition.T

  val tTag = PreservingPosition.tTag

  def prepare[
    D <: Nat : ToInt
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: LTEq[D, P]
  ): Option[T] = PreservingPosition.prepare(cell, dim)

  def reduce(lt: T, rt: T): T = PreservingPosition.reduce(true)(lt, rt)

  def present(t: T): Option[Content[_]] = PreservingPosition.present(t)
}

/** Reduce two cells preserving the cell with minimal value for the coordinate of the dimension being squashed. */
case class PreservingMinimumPosition[P <: HList]() extends Squasher[P] {
  type T = PreservingPosition.T

  val tTag = PreservingPosition.tTag

  def prepare[
    D <: Nat : ToInt
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: LTEq[D, P]
  ): Option[T] = PreservingPosition.prepare(cell, dim)

  def reduce(lt: T, rt: T): T = PreservingPosition.reduce(false)(lt, rt)

  def present(t: T): Option[Content[_]] = PreservingPosition.present(t)
}

/** Reduce two cells preserving the cell whose coordinate matches `keep`. */
case class KeepSlice[P <: HList](keep: Value) extends Squasher[P] {
  type T = Content[_]

  val tTag = classTag[T]

  def prepare[
    D <: Nat : ToInt
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: LTEq[D, P]
  ): Option[T] = if (cell.position(dim) equ keep) Option(cell.content) else None

  def reduce(lt: T, rt: T): T = lt

  def present(t: T): Option[Content[_]] = Option(t)
}

