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

package commbank.grimlock.framework.partition

import commbank.grimlock.framework.{ Cell, Persist }
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner

import scala.reflect.ClassTag

import shapeless.Nat

/** Trait for partitioners. */
trait Partitioner[P <: Nat, I] extends PartitionerWithValue[P, I] {
  type V = Any

  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = assign(cell)

  /**
   * Assign the cell to a partition.
   *
   * @param cell The cell to assign to a partition.
   *
   * @return Zero or more partitition identifiers.
   */
  def assign(cell: Cell[P]): TraversableOnce[I]
}

/** Companion object for the `Partitioner` trait. */
object Partitioner {
  /** Converts a `(Cell[P]) => I` to a `Partitioner[P, S]`. */
  implicit def funcToPartitioner[P <: Nat, I](func: (Cell[P]) => I) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = List(func(cell))
  }

  /** Converts a `(Cell[P]) => List[S]` to a `Partitioner[P, I]`. */
  implicit def funcListToPartitioner[P <: Nat, I](func: (Cell[P]) => List[I]) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = func(cell)
  }

  /** Converts a `List[Partitioner[P, I]]` to a single `Partitioner[P, I]`. */
  implicit def listToPartitioner[P <: Nat, I](partitioners: List[Partitioner[P, I]]) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = partitioners.flatMap(_.assign(cell))
  }
}

/** Trait for partitioners that use a user supplied value. */
trait PartitionerWithValue[P <: Nat, I] {
  /** Type of the external value. */
  type V

  /**
   * Assign the cell to a partition using a user supplied value.
   *
   * @param cell The cell to assign to a partition.
   * @param ext  The user supplied value.
   *
   * @return Zero or more partitition identifiers.
   */
  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I]
}

/** Companion object for the `PartitionerWithValue` trait. */
object PartitionerWithValue {
  /** Converts a `(Cell[P], W) => I` to a `PartitionerWithValue[P, S] { type V >: W }`. */
  implicit def funcToPartitionerWithValue[P <: Nat, I, W](func: (Cell[P], W) => I) = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = List(func(cell, ext))
  }

  /** Converts a `(Cell[P], W) => List[I]` to a `PartitionerWithValue[P, I] { type V >: W }`. */
  implicit def funcListToPartitionerWithValue[
    P <: Nat,
    I,
    W
  ](
    func: (Cell[P], W) => List[I]
  ) = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = func(cell, ext)
  }

  /**
   * Converts a `List[PartitionerWithValue[P, I] { type V >: W }]` to a single
   * `PartitionerWithValue[P, I] { type V >: W }`.
   */
  implicit def listToPartitionerWithValue[
    P <: Nat,
    I,
    W
  ](
    partitioners: List[PartitionerWithValue[P, I] { type V >: W }]
  ) = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = partitioners
      .flatMap(_.assignWithValue(cell, ext).toList)
  }
}

/** Trait that represents the partitions of matrices */
trait Partitions[P <: Nat, I, U[_], E[_], C <: Context[U, E]] extends Persist[(I, Cell[P]), U, E, C] {
  /**
   * Add a partition.
   *
   * @param id        The partition identifier.
   * @param partition The partition to add.
   *
   * @return A `U[(I, Cell[P])]` containing existing and new paritions.
   */
  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])]

  /**
   * Apply function `fn` to all partitions.
   *
   * @param fn      The function to apply to each partition.
   * @param exclude List of partition ids to exclude from applying `fn` to.
   * @param tuner   The tuner for the job.
   *
   * @return A `U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   *
   * @note This will pull all partition ids into memory, so only use this if there is sufficient memory
   *       available to keep all (distinct) partition ids in memory.
   */
  def forAll[
    Q <: Nat,
    T <: Tuner
  ](
    fn: (I, U[Cell[P]]) => U[Cell[Q]],
    exclude: List[I] = List(),
    tuner: T
  )(implicit
    ev1: ClassTag[I],
    ev2: Partitions.ForAllTuners[U, T]
  ): U[(I, Cell[Q])]

  /**
   * Apply function `fn` to each partition in `ids`.
   *
   * @param ids   List of partition ids to apply `fn` to.
   * @param fn    The function to apply to each partition.
   *
   * @return A `U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   */
  def forEach[Q <: Nat](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])]

  /**
   * Return the data for the partition `id`.
   *
   * @param id The partition for which to get the data.
   *
   * @return A `U[Cell[P]]`; that is a matrix.
   */
  def get(id: I): U[Cell[P]]

  /**
   * Return the partition identifiers.
   *
   * @param tuner The tuner for the job.
   */
  def ids[T <: Tuner](tuner: T)(implicit ev1: ClassTag[I], ev2: Partitions.IdsTuners[U, T]): U[I]

  /**
   * Merge partitions into a single matrix.
   *
   * @param ids List of partition keys to merge.
   *
   * @return A `U[Cell[P]]` containing the merge partitions.
   */
  def merge(ids: List[I]): U[Cell[P]]

  /**
   * Remove a partition.
   *
   * @param id The identifier for the partition to remove.
   *
   * @return A `U[(I, Cell[P])]` with the selected parition removed.
   */
  def remove(id: I): U[(I, Cell[P])]

  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `(I, Cell[P])` to string.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[(I, Cell[P])]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: Persist.TextWriter[(I, Cell[P])] = Partitions.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuners[U, T]
  ): U[(I, Cell[P])]
}

/** Companion object to `Partitions` with types, implicits, etc. */
object Partitions {
  /** Trait for tuners permitted on a call to `forAll`. */
  trait ForAllTuners[U[_], T <: Tuner]

  /** Trait for tuners permitted on a call to `ids`. */
  trait IdsTuners[U[_], T <: Tuner]

  /**
   * Return function that returns a string representation of a partition.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString[
    I,
    P <: Nat
  ](
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  ): ((I, Cell[P])) => TraversableOnce[String] = (t: (I, Cell[P])) =>
    List(t._1.toString + separator + (if (verbose) t._2.toString else t._2.toShortString(separator, descriptive)))

  /**
   * Return function that returns a JSON representations of a partition.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param separator   The separator to use between various both JSON strings.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   *
   * @note The key and cell are separately encoded and then combined using the separator.
   */
  def toJSON[
    I,
    P <: Nat
  ](
    pretty: Boolean = false,
    separator: String = ",",
    descriptive: Boolean = true
  ): ((I, Cell[P])) => TraversableOnce[String] = (t: (I, Cell[P])) =>
    List(t._1.toString + separator + t._2.toJSON(pretty, descriptive))
}

