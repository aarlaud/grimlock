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

package commbank.grimlock.framework.transform

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content

import shapeless.{ HList, Nat }
import shapeless.ops.hlist.Length
import shapeless.ops.nat.GTEq

/** Trait for transformations from `P` to `Q`. */
trait Transformer[P <: HList, Q <: HList] extends TransformerWithValue[P, Q] { self =>
  type V = Any

  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = present(cell)

  /**
   * Present the transformed content(s).
   *
   * @param cell The cell to transform.
   *
   * @return A `TraversableOnce` of transformed cells.
   */
  def present(cell: Cell[P]): TraversableOnce[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThen[X <: HList](that: Transformer[Q, X]) = new Transformer[P, X] {
    def present(cell: Cell[P]): TraversableOnce[Cell[X]] = self.present(cell).flatMap(that.present(_))
  }

  /**
   * Operator for preparing content prior to transforming.
   *
   * @param preparer The function to apply prior to transforming.
   *
   * @return A transformer that prepares the content and then runs `this`.
   */
  override def withPrepare[D](preparer: (Cell[P]) => Content[D]) = new Transformer[P, Q] {
    def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = self.present(cell.mutate(preparer))
  }

  /**
   * Operator for transforming and then updating the contents.
   *
   * @param mutator The mutation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate[D](mutator: (Cell[Q]) => Content[D]) = new Transformer[P, Q] {
    def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = self.present(cell).map(_.mutate(mutator))
  }

  /**
   * Operator for transforming and then relocating the contents.
   *
   * @param locator The relocation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then relocates the contents.
   */
  override def andThenRelocate[
    X <: HList,
    L <: Nat,
    M <: Nat
  ](
    locator: Locate.FromCell[Q, X]
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[X, M],
    ev3: GTEq[M, L]
  ) = new Transformer[P, X] {
    def present(cell: Cell[P]): TraversableOnce[Cell[X]] = self
      .present(cell)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }
}

/** Companion object for the `Transformer` type class. */
object Transformer {
  /** Converts a `(Cell[P]) => Cell[Q]` to a `Transformer[P, Q]`. */
  implicit def funcToTransformer[P <: HList, Q <: HList](func: (Cell[P]) => Cell[Q]) = new Transformer[P, Q] {
    def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = List(func(cell))
  }

  /** Converts a `(Cell[P]) => List[Cell[Q]]` to a `Transformer[P, Q]`. */
  implicit def funcListToTransformer[P <: HList, Q <: HList](func: (Cell[P]) => List[Cell[Q]]) = new Transformer[P, Q] {
    def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = func(cell)
  }

  /** Converts a `List[Transformer[P, Q]]` to a single `Transformer[P, Q]`. */
  implicit def listToTransformer[
    P <: HList,
    Q <: HList
  ](
    transformers: List[Transformer[P, Q]]
  ) = new Transformer[P, Q] {
    def present(cell: Cell[P]): TraversableOnce[Cell[Q]] = transformers.flatMap(_.present(cell))
  }
}

/** Trait for transformations from `P` to `Q` that use a user supplied value. */
trait TransformerWithValue[P <: HList, Q <: HList] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Present the transformed content(s).
   *
   * @param cell The cell to transform.
   * @param ext  Externally provided data needed for the transformation.
   *
   * @return A `TraversableOnce` of transformed cells.
   */
  def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]]

  /**
   * Operator for chaining transformations.
   *
   * @param that The transformation to perform after `this`.
   *
   * @return A transformer that runs `this` and then `that`.
   */
  def andThenWithValue[
    X <: HList
  ](
    that: TransformerWithValue[Q, X] { type V >: self.V }
  ) = new TransformerWithValue[P, X] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[X]] = self
      .presentWithValue(cell, ext)
      .flatMap(that.presentWithValue(_, ext))
  }

  /**
   * Operator for preparing content prior to transforming.
   *
   * @param preparer The function to apply prior to transforming.
   *
   * @return A transformer that prepares the content and then runs `this`.
   */
  def withPrepare[D](preparer: (Cell[P]) => Content[D]) = new TransformerWithValue[P, Q] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(cell.mutate(preparer), ext)
  }

  /**
   * Operator for transforming and then updating the contents.
   *
   * @param mutator The mutation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then updates the resulting contents.
   */
  def andThenMutate[D](mutator: (Cell[Q]) => Content[D]) = new TransformerWithValue[P, Q] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(cell, ext)
      .map(_.mutate(mutator))
  }

  /**
   * Operator for transforming and then relocating the contents.
   *
   * @param locator The relocation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then relocates the contents.
   */
  def andThenRelocate[
    X <: HList,
    L <: Nat,
    M <: Nat
  ](
    locator: Locate.FromCell[Q, X]
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[X, M],
    ev3: GTEq[M, L]
  ) = new TransformerWithValue[P, X] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[X]] = self
      .presentWithValue(cell, ext)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }

  /**
   * Operator for preparing content prior to transforming.
   *
   * @param preparer The function to apply prior to transforming.
   *
   * @return A transformer that prepares the content and then runs `this`.
   */
  def withPrepareWithValue[D](preparer: (Cell[P], V) => Content[D]) = new TransformerWithValue[P, Q] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(Cell(cell.position, preparer(cell, ext)), ext)
  }

  /**
   * Operator for transforming and then updating the contents.
   *
   * @param mutator The mutation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue[D](mutator: (Cell[Q], V) => Content[D]) = new TransformerWithValue[P, Q] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(cell, ext)
      .map(c => Cell(c.position, mutator(c, ext)))
  }

  /**
   * Operator for transforming and then relocating the contents.
   *
   * @param locator The relocation to apply after the transformation.
   *
   * @return A transformer that runs `this` and then relocates the contents.
   */
  def andThenRelocateWithValue[
    X <: HList,
    L <: Nat,
    M <: Nat
  ](
    locator: Locate.FromCellWithValue[Q, X, V]
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[X, M],
    ev3: GTEq[M, L]
  ) = new TransformerWithValue[P, X] {
    type V = self.V

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[X]] = self
      .presentWithValue(cell, ext)
      .flatMap(c => locator(c, ext).map(Cell(_, c.content)))
  }
}

/** Companion object for the `TransformerWithValue` trait. */
object TransformerWithValue {
  /** Converts a `(Cell[P], W) => Cell[Q]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def funcToTransformerWithValue[
    P <: HList,
    Q <: HList,
    W
  ](
    func: (Cell[P], W) => Cell[Q]
  ): TransformerWithValue[P, Q] { type V >: W } = new TransformerWithValue[P, Q] {
    type V = W

    def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = List(func(cell, ext))
  }

  /** Converts a `(Cell[P], W) => List[Cell[Q]]` to a `TransformerWithValue[P, Q] { type V >: W }`. */
  implicit def funcListToTransformerWithValue[
    P <: HList,
    Q <: HList,
    W
  ](
    func: (Cell[P], W) => List[Cell[Q]]
  ): TransformerWithValue[P, Q] { type V >: W } = new TransformerWithValue[P, Q] {
    type V = W

    def presentWithValue(cell: Cell[P], ext: W): TraversableOnce[Cell[Q]] = func(cell, ext)
  }

  /**
   * Converts a `List[TransformerWithValue[P, Q] { type V >: W }]` to a single
   * `TransformerWithValue[P, Q] { type V >: W }`.
   */
  implicit def listToTransformerWithValue[
    P <: HList,
    Q <: HList,
    W
  ](
    transformers: List[TransformerWithValue[P, Q] { type V >: W }]
  ): TransformerWithValue[P, Q] { type V >: W } = new TransformerWithValue[P, Q] {
    type V = W

    def presentWithValue(cell: Cell[P], ext: V): TraversableOnce[Cell[Q]] = transformers
      .flatMap(_.presentWithValue(cell, ext))
  }
}

