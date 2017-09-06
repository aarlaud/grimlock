// Copyright 2016 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.{ HList, Nat }
import shapeless.nat._1
import shapeless.ops.hlist.Length
import shapeless.ops.nat.GT

/** Trait for compacting a cell to a `Map`. */
trait Compactable[P <: HList, V[_ <: HList]] extends java.io.Serializable {
  /**
   * Convert a single cell to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) to compact.
   * @param cell  The cell to compact.
   *
   * @return A `Map` with the compacted cell.
   */
  def toMap(slice: Slice[P], cell: Cell[P]): Map[Position[slice.S], V[slice.R]] = Map(
    slice.selected(cell.position) -> compact(slice.remainder(cell.position), cell.content)
  )

  /**
   * Combine two compacted cells.
   *
   * @param x The left map to combine.
   * @param y The right map to combine.
   *
   * @return The combined map.
   */
  def combineMaps[
    S <: HList,
    R <: HList
  ](
    x: Map[Position[S], V[R]],
    y: Map[Position[S], V[R]]
  ): Map[Position[S], V[R]] = x ++ y.map { case (k, v) => k -> combine(x.get(k), v) }

  protected def compact[R <: HList, D](rem: Position[R], con: Content[D]): V[R]
  protected def combine[R <: HList](x: Option[V[R]], y: V[R]): V[R]
}

/** Companion object to the `Compactable` trait. */
object Compactable {
  /** Compacted content for `Position1D`. */
  type V1[R <: HList] = Content[_]

  /** Compacted content for `Position[P]` with `P` greater than `_1`. */
  type VX[R <: HList] = Map[Position[R], Content[_]]

  /** A `Compactable[_1, V1]` for `Position1D`. */
  implicit def compactable1D[
    P <: HList,
    L <: Nat
  ](implicit
    ev1: Length.Aux[P, L],
    ev2: L =:= _1
  ): Compactable[P, V1] = new Compactable[P, V1] {
    protected def compact[R <: HList](rem: Position[R], con: V1[R]): V1[R] = con
    protected def combine[R <: HList](x: Option[V1[R]], y: V1[R]): V1[R] = y
  }

  /** A `Compactable[P, VX]` for positions `P` greater than `_1`. */
  implicit def compactableXD[
    P <: HList,
    L <: Nat
  ](implicit
    ev1: Length.Aux[P, L],
    ev2: GT[L, _1]
  ): Compactable[P, VX] = new Compactable[P, VX] {
    protected def compact[R <: HList, D](rem: Position[R], con: Content[D]): VX[R] = Map(rem -> con)
    protected def combine[R <: HList](x: Option[VX[R]], y: VX[R]): VX[R] = x.map(_ ++ y).getOrElse(y)
  }
}

