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

package commbank.grimlock.framework.extract

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.{ HList, HNil, Nat }

/** Trait for extracting data from a user provided value given a cell. */
trait Extract[P <: HList, V, T] extends java.io.Serializable { self =>
  /**
   * Extract value for the given cell.
   *
   * @param cell The cell for which to extract a value.
   * @param ext  The user provided data from which to extract.
   *
   * @return Optional value (if present) or `None` otherwise.
   */
  def extract(cell: Cell[P], ext: V): Option[T]

  /**
   * Operator for transforming the returned value.
   *
   * @param presenter The function to apply and transform the returned value.
   *
   * @return An extract that runs `this` and then transfors the returned value.
   */
  def andThenPresent[X](presenter: (T) => Option[X]): Extract[P, V, X] = new Extract[P, V, X] {
    def extract(cell: Cell[P], ext: V) = self.extract(cell, ext).flatMap(r => presenter(r))
  }
}

/** Companion object for the `Extract` trait. */
object Extract {
  /** Converts a `(Cell[P], V) => Option[T]` to a `Extract[P, V, T]`. */
  implicit def funcToExtract[P <: HList, V, T](e: (Cell[P], V) => Option[T]): Extract[P, V, T] = new Extract[P, V, T] {
    def extract(cell: Cell[P], ext: V): Option[T] = e(cell, ext)
  }
}

/**
 * Extract from a `Map[Position[V :: HNil], T]` using the provided key.
 *
 * @param The key used for extracting from the map.
 */
case class ExtractWithKey[
  P <: HList,
  V,
  T
](
  key: Position[V :: HNil]
) extends Extract[P, Map[Position[V :: HNil], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[V :: HNil], T]): Option[T] = ext.get(key)
}

/** Extract from a `Map[Position[V :: HNil], T]` using a dimension from the cell. */
trait ExtractWithDimension[P <: HList, V, T] extends Extract[P, Map[Position[V :: HNil], T], T] { }

/** Companion object to `ExtractWithDimension`. */
object ExtractWithDimension {
  /**
   * Extract from a `Map[Position[V :: HNil], T]` using a dimension from the cell.
   *
   * @param dim Dimension used for extracting from the map.
   */
  def apply[
    P <: HList,
    D <: Nat,
    V,
    T
  ](
    dimension: D
  )(implicit
    ev: At.Aux[P, D, V]
  ): ExtractWithDimension[P, T] = new ExtractWithDimension[P, T] {
    def extract(cell: Cell[P], ext: Map[Position[V :: HNil], T]): Option[T] = ext
      .get(Position(cell.position(dimensino)))
  }
}

/**
 * Extract from a `Map[Position[V :: HNil], Map[Position[W :: HNil], T]]` using a dimension from the cell
 * and the provided key.
 */
trait ExtractWithDimensionAndKey[
  P <: HList,
  V,
  W,
  T
] extends Extract[P, Map[Position[V :: HNil], Map[Position[W :: HNil], T]], T] { }

/** Companion object to `ExtractWithDimensionAndKey`. */
object ExtractWithDimensionAndKey {
  /**
   * Extract from a `Map[Position[V :: HNil], Map[Position[W :: HNil], T]]` using a dimension from the cell
   * and the provided key.
   *
   * @param dim Dimension used for extracting from the outer map.
   * @param key The key used for extracting from the inner map.
   */
  def apply[
    P <: HList,
    D <: Nat,
    V,
    W,
    T
  ](
    dimension: D,
    key: Position[W :: HNil]
  )(implicit
    ev: At.Aux[P, D, V]
  ): ExtractWithDimensionAndKey[P, T] = new ExtractWithDimensionAndKey[P, T] {
    def extract(cell: Cell[P], ext: Map[Position[V :: HNil], Map[Position[W :: HNil], T]]): Option[T] = ext
      .get(Position(cell.position(dimension)))
      .flatMap(_.get(key))
  }
}

/** Extract from a `Map[Position[P], T]` using the position of the cell. */
case class ExtractWithPosition[P <: HList, T]() extends Extract[P, Map[Position[P], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[P], T]): Option[T] = ext.get(cell.position)
}

/**
 * Extract from a `Map[Position[P], Map[Position[V :: HNil], T]]` using the position of the cell and the provided key.
 *
 * @param key The key used for extracting from the inner map.
 */
case class ExtractWithPositionAndKey[
  P <: HList,
  V,
  T
](
  key: Position[V :: HNil]
) extends Extract[P, Map[Position[P], Map[Position[V :: HNil], T]], T] {
  def extract(cell: Cell[P], ext: Map[Position[P], Map[Position[V :: HNil], T]]): Option[T] = ext
    .get(cell.position)
    .flatMap(_.get(key))
}

/** Extract from a `Map[Position[S], T]` using the selected position(s) of the cell. */
trait ExtractWithSelected[P <: HList, S <: HList, T] extends Extract[P, Map[Position[S], T], T] { }

/** Companion object to `ExtractWithSelected`. */
object ExtractWithSelected {
  /**
   * Extract from a `Map[Position[slice.S], T]` using the selected position(s) of the cell.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   */
  def apply[
    P <: HList,
    T
  ](
    slice: Slice[P]
  ): ExtractWithSelected[P, slice.S, T] = new ExtractWithSelected[P, slice.S, T] {
    def extract(cell: Cell[P], ext: Map[Position[slice.S], T]): Option[T] = ext.get(slice.selected(cell.position))
  }
}

/**
 * Extract from a `Map[Position[S], Map[Position[V :: HNil], T]]` using the selected position(s) of the cell and
 * the provided key.
 */
trait ExtractWithSelectedAndKey[
  P <: HList,
  S <: HList,
  V,
  T
] extends Extract[P, Map[Position[S], Map[Position[V :: HNil], T]], T] { }

/** Companion object to `ExtractWithSelectedAndKey`. */
object ExtractWithSelectedAndKey {
  /**
   * Extract from a `Map[Position[slice.S], Map[Position[V :: HNil], T]]` using the selected position(s) of the
   * cell and the provided key.
   *
   * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
   *              into the map.
   * @param key   The key used for extracting from the inner map.
   */
  def apply[
    P <: HList,
    V,
    T
  ](
    slice: Slice[P],
    key: Position[V :: HNil]
  ): ExtractWithSelectedAndKey[P, slice.S, T] = new ExtractWithSelectedAndKey[P, slice.S, T] {
    def extract(cell: Cell[P], ext: Map[Position[slice.S], Map[Position[V :: HNil], T]]): Option[T] = ext
      .get(slice.selected(cell.position))
      .flatMap(_.get(key))
  }
}

/** Extract from a `Map[Position[S], Map[Position[R], T]]` using the selected and remainder position(s) of the cell. */
trait ExtractWithSlice[
  P <: HList,
  S <: HList,
  R <: HList,
  T
] extends Extract[P, Map[Position[S], Map[Position[R], T]], T] { }

/** Companion object to `ExtractWithSlice`. */
object ExtractWithSlice {
  /**
   * Extract from a `Map[Position[slice.S], Map[Position[slice.R], T]]` using the selected and remainder position(s)
   * of the cell.
   *
   * @param slice The slice used to extract the selected and remainder position(s) from the cell which are used
   *              as the keys into the outer and inner maps.
   */
  def apply[
    P <: HList,
    T
  ](
    slice: Slice[P]
  ): ExtractWithSlice[P, slice.S, slice.R, T] = new ExtractWithSlice[P, slice.S, slice.R, T] {
    def extract(cell: Cell[P], ext: Map[Position[slice.S], Map[Position[slice.R], T]]): Option[T] = ext
      .get(slice.selected(cell.position))
      .flatMap(_.get(slice.remainder(cell.position)))
  }
}

