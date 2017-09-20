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

package commbank.grimlock.framework

import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distance.PairwiseDistance
import commbank.grimlock.framework.distribution.ApproximateDistribution
import commbank.grimlock.framework.encoding.{ LongValue, Value }
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.{ Reducers, Tuner }
import commbank.grimlock.framework.pairwise.{ Comparer, Operator, OperatorWithValue }
import commbank.grimlock.framework.partition.{ Partitioner, PartitionerWithValue }
import commbank.grimlock.framework.position.{ Position, Positions, Slice }
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }
import commbank.grimlock.framework.squash.{ Squasher, SquasherWithValue }
import commbank.grimlock.framework.statistics.Statistics
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }
import commbank.grimlock.framework.utility.{ Escape, Quote }
import commbank.grimlock.framework.window.{ Window, WindowWithValue }

import org.apache.hadoop.io.Writable

import shapeless.{ ::, =:!=, HList, HNil, IsDistinctConstraint, Nat }
import shapeless.ops.hlist.Length
import shapeless.ops.nat.{ LTEq, GT, GTEq, Pred, ToInt }

/** Trait for common matrix operations. */
trait Matrix[P <: HList, C <: Context[C]] extends Persist[Cell[P], C]
  with ApproximateDistribution[P, C]
  with Statistics[P, C] {
  /**
   * Change the variable type of `positions` in a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to change.
   * @param tuner     The tuner for the job.
   * @param positions The position(s) within the dimension(s) to change.
   * @param schema    The schema to change to.
   * @param writer    The writer to call in case or change error.
   *
   * @return A `C#U[Cell[P]]` with the changed contents and a `C#U[String]` with errors.
   */
  def change[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    positions: C#U[Position[S]],
    schema: Content.Decoder,
    writer: Persist.TextWriter[Cell[P]]
  )(implicit
    ev: Matrix.ChangeTuner[C#U, T]
  ): (C#U[Cell[P]], C#U[String])

  /**
   * Compacts a matrix to a `Map`.
   *
   * @return A `C#E[Map[Position[P], Content]]` containing the Map representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def compact(): C#E[Map[Position[P], Content]]

  /**
   * Compact a matrix to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) along which to convert.
   * @param tuner The tuner for the job.
   *
   * @return A `C#E[Map[Position[S], V[R]]]` containing the Map
   *         representation of this matrix.
   *
   * @note Avoid using this for very large matrices.
   */
  def compact[
    S <: HList,
    R <: HList,
    T <: Tuner,
    V[_ <: HList]
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(implicit
    ev1: S =:!= HNil,
    ev2: Compactable[P, V],
    ev3: Matrix.CompactTuner[C#U, T]
  ): C#E[Map[Position[S], V[R]]]

  /**
   * Return contents of a matrix at `positions`.
   *
   * @param positions The positions for which to get the contents.
   * @param tuner     The tuner for the job.
   *
   * @return A `C#U[Cell[P]]` of the `positions` together with their content.
   */
  def get[T <: Tuner](positions: C#U[Position[P]], tuner: T)(implicit ev: Matrix.GetTuner[C#U, T]): C#U[Cell[P]]

  /**
   * Returns the matrix as in in-memory list of cells.
   *
   * @param context The operating context.
   *
   * @return A `List[Cell[P]]` of the cells.
   *
   * @note Avoid using this for very large matrices.
   */
  def materialise(context: C): List[Cell[P]]

  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Position[S]]` of the distinct position(s).
   */
  def names[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(implicit
    ev1: S =:!= HNil,
    ev2: Positions.NamesTuner[C#U, T]
  ): C#U[Position[S]]

  /**
   * Compute pairwise values between all pairs of values given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param tuner     The tuner for the job.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param operators The pairwise operator(s) to apply.
   *
   * @return A `C#U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwise[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    comparer: Comparer,
    operators: Operator[P, Q]*
  )(implicit
    ev1: S =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[R, M],
    ev4: GT[L, M],
    ev5: Matrix.PairwiseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Compute pairwise values between all pairs of values given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param tuner     The tuner for the job.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param value     The user supplied value.
   * @param operators The pairwise operator(s) to apply.
   *
   * @return A `C#U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    comparer: Comparer,
    value: C#E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: S =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[R, M],
    ev4: GT[L, M],
    ev5: Matrix.PairwiseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param tuner     The tuner for the job.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param operators The pairwise operator(s) to apply.
   *
   * @return A `C#U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseBetween[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    comparer: Comparer,
    that: C#U[Cell[P]],
    operators: Operator[P, Q]*
  )(implicit
    ev1: S =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[R, M],
    ev4: GT[L, M],
    ev5: Matrix.PairwiseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Compute pairwise values between all values of this and that given a slice with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) along which to compute values.
   * @param tuner     The tuner for the job.
   * @param comparer  Defines which element the pairwise operations should apply to.
   * @param that      Other matrix to compute pairwise values with.
   * @param value     The user supplied value.
   * @param operators The pairwise operator(s) to apply.
   *
   * @return A `C#U[Cell[Q]]` where the content contains the pairwise values.
   */
  def pairwiseBetweenWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    comparer: Comparer,
    that: C#U[Cell[P]],
    value: C#E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: S =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[R, M],
    ev4: GT[L, M],
    ev5: Matrix.PairwiseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Relocate the coordinates of the cells.
   *
   * @param locate Function that relocates coordinates.
   *
   * @return A `C#U[Cell[Q]]` where the cells have been relocated.
   */
  def relocate[
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](
    locate: Locate.FromCell[P, Q]
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: GTEq[L, M]
  ): C#U[Cell[Q]]

  /**
   * Relocate the coordinates of the cells using user a suplied value.
   *
   * @param value  A `E` holding a user supplied value.
   * @param locate Function that relocates coordinates.
   *
   * @return A `C#U[Cell[Q]]` where the cells have been relocated.
   */
  def relocateWithValue[
    Q <: HList,
    W,
    L <: Nat,
    M <: Nat
  ](
    value: C#E[W],
    locate: Locate.FromCellWithValue[P, Q, W]
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: GTEq[L, M]
  ): C#U[Cell[Q]]

  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `Cell[P]` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[Cell[P]]`; that is it returns `data`.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[Cell[P]],
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[Cell[P]]

  /**
   * Set the `values` in a matrix.
   *
   * @param values The values to set.
   * @param tuner  The tuner for the job.
   *
   * @return A `C#U[Cell[P]]` with the `values` set.
   */
  def set[T <: Tuner](values: C#U[Cell[P]], tuner: T)(implicit ev: Matrix.SetTuner[C#U, T]): C#U[Cell[P]]

  /**
   * Returns the shape of the matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[LongValue :: HNil]]`. The position consists of a long value of the dimension
   *         index. The content has the actual size in it as a discrete variable.
   */
  def shape[T <: Tuner](tuner: T)(implicit ev: Matrix.ShapeTuner[C#U, T]): C#U[Cell[LongValue :: HNil]]

  /**
   * Returns the size of the matrix in dimension `dim`.
   *
   * @param dim      The dimension for which to get the size.
   * @param distinct Indicates if each coordinate in dimension `dim` occurs only once. If this is the case, then
   *                 enabling this flag has better run-time performance.
   * @param tuner    The tuner for the job.
   *
   * @return A `C#U[Cell[LongValue :: HNil]]`. The position consists of a long value of the dimension index. The
   *         content has the actual size in it as a discrete variable.
   */
  def size[
    D <: Nat,
    T <: Tuner,
    L <: Nat
  ](
    dim: D,
    distinct: Boolean = false,
    tuner: T
  )(implicit
    ev1: Length.Aux[P, L],
    ev2: LTEq[D, L],
    ev3: Matrix.SizeTuner[C#U, T]
  ): C#U[Cell[LongValue :: HNil]]

  /**
   * Slice a matrix.
   *
   * @param slice     Encapsulates the dimension(s) to slice.
   * @param tuner     The tuner for the job.
   * @param keep      Indicates if the `positions` should be kept or removed.
   * @param positions The position(s) within the dimension(s) to slice.
   *
   * @return A `C#U[Cell[P]]` of the remaining content.
   */
  def slice[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    keep: Boolean,
    positions: C#U[Position[S]]
  )(implicit
    ev: Matrix.SliceTuner[C#U, T]
  ): C#U[Cell[P]]

  /**
   * Create window based derived data.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param tuner     The tuner for the job.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param windows   The window function(s) to apply to the content.
   *
   * @return A `C#U[Cell[Q]]` with the derived data.
   */
  def slide[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    ascending: Boolean,
    windows: Window[P, S, R, Q]*
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: Matrix.SlideTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Create window based derived data with a user supplied value.
   *
   * @param slice     Encapsulates the dimension(s) to slide over.
   * @param tuner     The tuner for the job.
   * @param ascending Indicator if the data should be sorted ascending or descending.
   * @param value     A `E` holding a user supplied value.
   * @param windows   The window function(s) to apply to the content.
   *
   * @return A `C#U[Cell[Q]]` with the derived data.
   */
  def slideWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    ascendig: Boolean,
    value: C#E[W],
    windows: WindowWithValue[P, S, R, Q] { type V >: W }*
  )(implicit
    ev1: R =:!= HNil,
    ev2: Length.Aux[Q, L],
    ev3: Length.Aux[S, M],
    ev4: GT[L, M],
    ev5: Matrix.SlideTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Partition a matrix according to `partitioner`.
   *
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `C#U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def split[I](partitioners: Partitioner[P, I]*): C#U[(I, Cell[P])]

  /**
   * Partition a matrix according to `partitioner` using a user supplied value.
   *
   * @param value        A `E` holding a user supplied value.
   * @param partitioners Assigns each position to zero, one or more partition(s).
   *
   * @return A `C#U[(I, Cell[P])]` where `I` is the partition for the corresponding tuple.
   */
  def splitWithValue[I, W](value: C#E[W], partitioners: PartitionerWithValue[P, I] { type V >: W }*): C#U[(I, Cell[P])]

  /**
   * Stream this matrix through `command` and apply `script`.
   *
   * @param command   The command to stream (pipe) the data through.
   * @param files     A list of text files that will be available to `command`. Note that all files must be
   *                  located in the same directory as which the job is started.
   * @param writer    Function that converts a cell to a string (prior to streaming it through `command`).
   * @param parser    Function that parses the resulting string back to a cell.
   * @param reducers  The number of reducers to use.
   * @param hash      Function maps each cell's position to an integer. Use this function to tag cells
   *                  that should be sent to the same reducer.
   *
   * @return A `C#U[Cell[Q]]` with the new data as well as a `C#U[String]` with any parse errors.
   *
   * @note The `command` must be installed on each node of the cluster.
   */
  def stream[
    Q <: HList
  ](
    command: String,
    files: List[String],
    writer: Persist.TextWriter[Cell[P]],
    parser: Persist.TextParser[Cell[Q]],
    reducers: Reducers = Reducers(1),
    hash: (Position[P]) => Int = _ => 0
  ): (C#U[Cell[Q]], C#U[String])

  /**
   * Stream this matrix through `command` and apply `script`, after grouping all data by `slice`.
   *
   * @param slice     Encapsulates the dimension(s) along which to stream.
   * @param command   The command to stream (pipe) the data through.
   * @param files     A list of text files that will be available to `command`. Note that all files must be
   *                  located in the same directory as which the job is started.
   * @param writer    Function that converts a group of cells to a string (prior to streaming it through `command`).
   * @param parser    Function that parses the resulting string back to a cell.
   * @param reducers  The number of reducers to use.
   *
   * @return A `C#U[Cell[Q]]` with the new data as well as a `C#U[String]` with any parse errors.
   *
   * @note The `command` must be installed on each node of the cluster. The implementation assumes that all
   *       values for a given slice fit into memory.
   */
  def streamByPosition[
    S <: HList,
    R <: HList,
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R]
  )(
    command: String,
    files: List[String],
    writer: Persist.TextWriterByPosition[Cell[P]],
    parser: Persist.TextParser[Cell[Q]],
    reducers: Reducers = Reducers(1)
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[S, M],
    ev3: GTEq[L, M]
  ): (C#U[Cell[Q]], C#U[String])

  /**
   * Sample a matrix according to some `sampler`. It keeps only those cells for which `sampler` returns true.
   *
   * @param samplers Sampling function(s).
   *
   * @return A `C#U[Cell[P]]` with the sampled cells.
   */
  def subset(samplers: Sampler[P]*): C#U[Cell[P]]

  /**
   * Sample a matrix according to some `sampler` using a user supplied value. It keeps only those cells for which
   * `sampler` returns true.
   *
   * @param value    A `E` holding a user supplied value.
   * @param samplers Sampling function(s).
   *
   * @return A `C#U[Cell[P]]` with the sampled cells.
   */
  def subsetWithValue[W](value: C#E[W], samplers: SamplerWithValue[P] { type V >: W }*): C#U[Cell[P]]

  /**
   * Summarise a matrix and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param tuner       The tuner for the job.
   * @param aggregators The aggregator(s) to apply to the data.
   *
   * @return A `C#U[Cell[Q]]` with the aggregates.
   */
  def summarise[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    aggregators: Aggregator[P, S, Q]*
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[S, M],
    ev3: GTEq[L, M],
    ev4: Aggregator.Validate[P, S, Q],
    ev5: Matrix.SummariseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Summarise a matrix, using a user supplied value, and return the aggregates.
   *
   * @param slice       Encapsulates the dimension(s) along which to aggregate.
   * @param tuner       The tuner for the job.
   * @param value       A `E` holding a user supplied value.
   * @param aggregators The aggregator(s) to apply to the data.
   *
   * @return A `C#U[Cell[Q]]` with the aggregates.
   */
  def summariseWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    value: C#E[W],
    aggregators: AggregatorWithValue[P, S, Q] { type V >: W }*
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[S, M],
    ev3: GTEq[L, M],
    ev4: AggregatorWithValue.Validate[P, S, Q, W],
    ev5: Matrix.SummariseTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Convert all cells to key value tuples.
   *
   * @param writer The writer to convert a cell to key value tuple.
   *
   * @return A `C#U[(K, V)]` with all cells as key value tuples.
   */
  def toSequence[K <: Writable, V <: Writable](writer: Persist.SequenceWriter[Cell[P], K, V]): C#U[(K, V)]

  /**
   * Convert all cells to strings.
   *
   * @param writer The writer to convert a cell to string.
   *
   * @return A `C#U[String]` with all cells as string.
   */
  def toText(writer: Persist.TextWriter[Cell[P]]): C#U[String]

  /**
   * Merge all dimensions into a single.
   *
   * @param melt A function that melts the coordinates to a single valueable.
   *
   * @return A `C#U[CellPosition[_1]]]` where all coordinates have been merged into a single position.
   */
  def toVector[V <: Value[_]](melt: (List[Value[_]]) => V): C#U[Cell[V :: HNil]]

  /**
   * Transform the content of a matrix.
   *
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `C#U[Cell[Q]]` with the transformed cells.
   */
  def transform[
    Q <: HList,
    L <: Nat,
    M <: Nat
  ](
    transformers: Transformer[P, Q]*
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: GTEq[L, M]
  ): C#U[Cell[Q]]

  /**
   * Transform the content of a matrix using a user supplied value.
   *
   * @param value        A `E` holding a user supplied value.
   * @param transformers The transformer(s) to apply to the content.
   *
   * @return A `C#U[Cell[Q]]` with the transformed cells.
   */
  def transformWithValue[
    Q <: HList,
    W,
    L <: Nat,
    M <: Nat
  ](
    value: C#E[W],
    transformers: TransformerWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: GTEq[L, M]
  ): C#U[Cell[Q]]

  /**
   * Returns the variable type of the content(s) for a given `slice`.
   *
   * @param slice    Encapsulates the dimension(s) for this the types are to be returned.
   * @param tuner    The tuner for the job.
   * @param specific Indicates if the most specific type should be returned, or it's generalisation (default).
   *
   * @return A `C#U[Cell[S]]` of the distinct position(s) together with their type.
   */
  def types[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    specific: Boolean
  )(implicit
    ev1: S =:!= HNil,
    ev2: Matrix.TypesTuner[C#U, T]
  ): C#U[Cell[S]]

  /**
   * Return the unique (distinct) contents of an entire matrix.
   *
   * @param tuner The tuner for the job.
   *
   * @note Comparison is performed based on the string representation of the `Content`.
   */
  def unique[T <: Tuner](tuner: T)(implicit ev: Matrix.UniqueTuner[C#U, T]): C#U[Content]

  /**
   * Return the unique (distinct) contents along a dimension.
   *
   * @param slice Encapsulates the dimension(s) along which to find unique contents.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[(Position[S], Content)]` consisting of the unique values for each selected position.
   *
   * @note Comparison is performed based on the string representation of the `S` and `Content`.
   */
  def uniqueByPosition[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(implicit
    ev1: S =:!= HNil,
    ev2: Matrix.UniqueTuner[C#U, T]
  ): C#U[(Position[S], Content)]

  /**
   * Query the contents of a matrix and return the positions of those that match the predicate.
   *
   * @param predicate The predicate used to filter the contents.
   *
   * @return A `C#U[Position[P]]` of the positions for which the content matches `predicate`.
   */
  def which(predicate: Cell.Predicate[P]): C#U[Position[P]]

  /**
   * Query the contents of one of more positions of a matrix and return the positions of those that match the
   * corresponding predicates.
   *
   * @param slice      Encapsulates the dimension(s) to query.
   * @param tuner      The tuner for the job.
   * @param predicates The position(s) within the dimension(s) to query together with the predicates used to
   *                   filter the contents.
   *
   * @return A `C#U[Position[P]]` of the positions for which the content matches predicates.
   */
  def whichByPosition[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    predicates: List[(C#U[Position[S]], Cell.Predicate[P])]
  )(implicit
    ev: Matrix.WhichTuner[C#U, T]
  ): C#U[Position[P]]

  // TODO: Add label join operations
  // TODO: Add read/write[CSV|Hive|HBase|VW|LibSVM] operations
  // TODO: Add machine learning operations (SVD/finding cliques/etc.) - use Spark instead?
}

/** Companion object to `Matrix` with types, implicits, etc. */
object Matrix {
  /** Trait for tuners permitted on a call to `change`. */
  trait ChangeTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `compact` functions. */
  trait CompactTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `domain`. */
  trait DomainTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `fill` with hetrogeneous data. */
  trait FillHeterogeneousTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `fill` with homogeneous data. */
  trait FillHomogeneousTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `get`. */
  trait GetTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `join`. */
  trait JoinTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `pairwise` functions. */
  trait PairwiseTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `reshape` functions. */
  trait ReshapeTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `saveAsCSV`. */
  trait SaveAsCSVTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `saveAsIV`. */
  trait SaveAsIVTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `saveAsVW*`. */
  trait SaveAsVWTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `shape`. */
  trait ShapeTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `set` functions. */
  trait SetTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `shape`. */
  trait SizeTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `slice`. */
  trait SliceTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `slide` functions. */
  trait SlideTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `squash` functions. */
  trait SquashTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `summarise` functions. */
  trait SummariseTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `types`. */
  trait TypesTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `unique` functions. */
  trait UniqueTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `which` functions. */
  trait WhichTuner[U[_], T <: Tuner] extends java.io.Serializable
}

/** Trait for matrix operations on matrices with more than 1 dimension. */
trait MultiDimensionMatrix[P <: HList, C <: Context[C]] extends PairwiseDistance[P, C] {
  /**
   * Join two matrices.
   *
   * @param slice Encapsulates the dimension(s) along which to join.
   * @param tuner The tuner for the job.
   * @param that  The matrix to join with.
   *
   * @return A `C#U[Cell[P]]` consisting of the inner-join of the two matrices.
   */
  // TODO: Add inner/left/right/outer join functionality?
  def join[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    that: C#U[Cell[P]]
  )(implicit
    ev: Matrix.JoinTuner[C#U, T]
  ): C#U[Cell[P]]

  /**
   * Melt one dimension of a matrix into another.
   *
   * @param dim   The dimension to melt
   * @param into  The dimension to melt into
   * @param merge The function for merging two coordinates.
   *
   * @return A `C#U[Cell[Q]]` with one fewer dimension.
   *
   * @note A melt coordinate is always a string value constructed from the string representation of the `dim` and
   *       `into` coordinates.
   */
  def melt[
    D <: Nat,
    I <: Nat,
    T <% V,
    V <: Value[_],
    X <: Value[_],
    Y <: Value[_],
    Q <: HList
  ](
    dim: D,
    into: I,
    merge: (X, Y) => T
  )(implicit
    ev: Position.MeltConstraints[P, D, I, V, X, Y, Q]
  ): C#U[Cell[Q]]

  /**
   * Reshape a coordinate into it's own dimension.
   *
   * @param dim        The dimension to reshape.
   * @param coordinate The coordinate (in `dim`) to reshape into its own dimension.
   * @param locate     A locator that defines the coordinate for the new dimension.
   * @param tuner      The tuner for the job.
   *
   * @return A `C#U[Cell[Q]]` with reshaped dimensions.
   */
  def reshape[
    D <: Nat : ToInt,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    dim: D,
    coordinate: Value[_],
    locate: Locate.FromCellAndOptionalValue[P, Q],
    tuner: T
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: LTEq[D, M],
    ev4: GT[L, M],
    ev5: Matrix.ReshapeTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Squash a dimension of a matrix.
   *
   * @param dim      The dimension to squash.
   * @param squasher The squasher that reduces two cells.
   * @param tuner    The tuner for the job.
   *
   * @return A `C#U[Cell[Q]]` with the dimension `dim` removed.
   */
  def squash[
    D <: Nat,
    Q <: HList,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    dim: D,
    squasher: Squasher[P],
    tuner: T
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: LTEq[D, M],
    ev4: Pred.Aux[M, L],
    ev5: Matrix.SquashTuner[C#U, T]
  ): C#U[Cell[Q]]

  /**
   * Squash a dimension of a matrix with a user supplied value.
   *
   * @param dim      The dimension to squash.
   * @param value    The user supplied value.
   * @param squasher The squasher that reduces two cells.
   * @param tuner    The tuner for the job.
   *
   * @return A `C#U[Cell[Q]]` with the dimension `dim` removed.
   */
  def squashWithValue[
    D <: Nat : ToInt,
    Q <: HList,
    W,
    T <: Tuner,
    L <: Nat,
    M <: Nat
  ](
    dim: D,
    value: C#E[W],
    squasher: SquasherWithValue[P] { type V >: W },
    tuner: T
  )(implicit
    ev1: Length.Aux[Q, L],
    ev2: Length.Aux[P, M],
    ev3: LTEq[D, M],
    ev4: Pred.Aux[M, L],
    ev5: Matrix.SquashTuner[C#U, T]
  ): C#U[Cell[Q]]
}

/** Trait for matrix methods that depend on the number of dimensions. */
trait SetDimensionMatrix[P <: HList, C <: Context[C]] extends Persist[Cell[P], C] {
  /**
   * Return all possible positions of a matrix.
   *
   * @param tuner The tuner for the job.
   */
  def domain[T <: Tuner](tuner: T)(implicit ev: Matrix.DomainTuner[C#U, T]): C#U[Position[P]]

  /**
   * Fill a matrix with `values` for a given `slice`.
   *
   * @param slice  Encapsulates the dimension(s) on which to fill.
   * @param tuner  The tuner for the job.
   * @param values The content to fill a matrix with.
   *
   * @return A `C#U[Cell[P]]` where all missing values have been filled in.
   *
   * @note This joins `values` onto this matrix, as such it can be used for imputing missing values. As
   *       the join is an inner join, any positions in the matrix that aren't in `values` are filtered
   *       from the resulting matrix.
   */
  def fillHeterogeneous[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(
    values: C#U[Cell[S]]
  )(implicit
    ev: Matrix.FillHeterogeneousTuner[C#U, T]
  ): C#U[Cell[P]]

  /**
   * Fill a matrix with `value`.
   *
   * @param value The content to fill a matrix with.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[P]]` where all missing values have been filled in.
   */
  def fillHomogeneous[
    T <: Tuner
  ](
    value: Content,
    tuner: T
  )(implicit
    ev: Matrix.FillHomogeneousTuner[C#U, T]
  ): C#U[Cell[P]]

  /**
   * Persist as a sparse matrix file (index, value).
   *
   * @param context    The operating context.
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name.
   * @param separator  Column separator to use in dictionary file.
   * @param tuner      The tuner for the job.
   *
   * @return A `C#U[Cell[P]]`; that is it returns `data`.
   */
  def saveAsIV[
    T <: Tuner
  ](
    context: C,
    file: String,
    dictionary: String = "%1$s.dict.%2$d",
    separator: String = "|",
    tuner: T
  )(implicit
    ev: Matrix.SaveAsIVTuner[C#U, T]
  ): C#U[Cell[P]]
}

/** Trait for 1D specific operations. */
trait Matrix1D[V1 <: Value[_], C <: Context[C]] extends SetDimensionMatrix[V1 :: HNil, C] {
}

/** Trait for 2D specific operations. */
trait Matrix2D[V1 <: Value[_], V2 <: Value[_], C <: Context[C]] extends SetDimensionMatrix[V1 :: V2 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_]
  ](
    dim1: D1,
    dim2: D2
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: HNil, D2, W2],
    ev3: D1 =:!= D2
  ): C#U[Cell[W1 :: W2 :: HNil]]

  /**
   * Persist as a CSV file.
   *
   * @param slice       Encapsulates the dimension that makes up the columns.
   * @param tuner       The tuner for the job.
   * @param context     The operating context.
   * @param file        File to write to.
   * @param separator   Column separator to use.
   * @param escapee     The method for escaping the separator character.
   * @param writeHeader Indicator of the header should be written to a separate file.
   * @param header      Postfix for the header file name.
   * @param writeRowId  Indicator if row names should be written.
   * @param rowId       Column name of row names.
   *
   * @return A `C#U[Cell[V1 :: V2 :: HNil]]`; that is it returns `data`.
   */
  def saveAsCSV[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: C,
    file: String,
    separator: String = "|",
    escapee: Escape = Quote("|"),
    writeHeader: Boolean = true,
    header: String = "%s.header",
    writeRowId: Boolean = true,
    rowId: String = "id"
  )(implicit
    ev: Matrix.SaveAsCSVTuner[C#U, T]
  ): C#U[Cell[V1 :: V2 :: HNil]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param tuner      The tuner for the job.
   * @param context    The operating context.
   * @param file       File to write to.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `C#U[Cell[V1 :: V2 :: HNil]]`; that is it returns `data`.
   */
  def saveAsVW[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: C,
    file: String,
    dictionary: String = "%s.dict",
    tag: Boolean = false,
    separator: String = "|"
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, T]
  ): C#U[Cell[V1 :: V2 :: HNil]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided labels.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param tuner      The tuner for the job.
   * @param context    The operating context.
   * @param file       File to write to.
   * @param labels     The labels.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `C#U[Cell[V1 :: V2 :: HNil]]`; that is it returns `data`.
   *
   * @note The labels are joined to the data keeping only those examples for which data and a label are available.
   */
  def saveAsVWWithLabels[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: C,
    file: String,
    labels: C#U[Cell[S]],
    dictionary: String = "%s.dict",
    tag: Boolean = false,
    separator: String = "|"
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, T]
  ): C#U[Cell[V1 :: V2 :: HNil]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided importance weights.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param tuner      The tuner for the job.
   * @param context    The operating context.
   * @param file       File to write to.
   * @param importance The importance weights.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `C#U[Cell[V1 :: V2 :: HNil]]`; that is it returns `data`.
   *
   * @note The weights are joined to the data keeping only those examples for which data and a weight are available.
   */
  def saveAsVWWithImportance[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: C,
    file: String,
    importance: C#U[Cell[S]],
    dictionary: String = "%s.dict",
    tag: Boolean = false,
    separator: String = "|"
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, T]
  ): C#U[Cell[V1 :: V2 :: HNil]]

  /**
   * Persist a `Matrix2D` as a Vowpal Wabbit file with the provided labels and importance weights.
   *
   * @param slice      Encapsulates the dimension that makes up the columns.
   * @param tuner      The tuner for the job.
   * @param context    The operating context.
   * @param file       File to write to.
   * @param labels     The labels.
   * @param importance The importance weights.
   * @param dictionary Pattern for the dictionary file name, use `%``s` for the file name.
   * @param tag        Indicator if the selected position should be added as a tag.
   * @param separator  Separator to use in dictionary.
   *
   * @return A `C#U[Cell[V1 :: V2 :: HNil]]`; that is it returns `data`.
   *
   * @note The labels and weights are joined to the data keeping only those examples for which data and a label
   *       and weight are available.
   */
  def saveAsVWWithLabelsAndImportance[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: C,
    file: String,
    labels: C#U[Cell[S]],
    importance: C#U[Cell[S]],
    dictionary: String = "%s.dict",
    tag: Boolean = false,
    separator: String = "|"
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, T]
  ): C#U[Cell[V1 :: V2 :: HNil]]
}

/** Trait for 3D specific operations. */
trait Matrix3D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, D3, W3],
    ev4: IsDistinctConstraint[D1 :: D2 :: D3 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: HNil]]
}

/** Trait for 4D specific operations. */
trait Matrix4D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  V4 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, D4, W4],
    ev5: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: HNil]]
}

/** Trait for 5D specific operations. */
trait Matrix5D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  V4 <: Value[_],
  V5 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    D5 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_],
    W5 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, D4, W4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, D5, W5],
    ev6: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: HNil]]
}

/** Trait for 6D specific operations. */
trait Matrix6D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  V4 <: Value[_],
  V5 <: Value[_],
  V6 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    D5 <: Nat,
    D6 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_],
    W5 <: Value[_],
    W6 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D4, W4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D5, W5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, D6, W6],
    ev7: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: HNil]]
}

/** Trait for 7D specific operations. */
trait Matrix7D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  V4 <: Value[_],
  V5 <: Value[_],
  V6 <: Value[_],
  V7 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    D5 <: Nat,
    D6 <: Nat,
    D7 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_],
    W5 <: Value[_],
    W6 <: Value[_],
    W7 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D4, W4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D5, W5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D6, W6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, D7, W7],
    ev8: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: HNil]]
}

/** Trait for 8D specific operations. */
trait Matrix8D[
  V1,
  V2,
  V3,
  V4,
  V5,
  V6,
  V7,
  V8,
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   * @param dim8 Dimension to use for the eighth coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    D5 <: Nat,
    D6 <: Nat,
    D7 <: Nat,
    D8 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_],
    W5 <: Value[_],
    W6 <: Value[_],
    W7 <: Value[_],
    W8 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D4, W4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D5, W5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D6, W6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D7, W7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, D8, W8],
    ev9: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: D8 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: W8 :: HNil]]
}

/** Trait for 9D specific operations. */
trait Matrix9D[
  V1 <: Value[_],
  V2 <: Value[_],
  V3 <: Value[_],
  V4 <: Value[_],
  V5 <: Value[_],
  V6 <: Value[_],
  V7 <: Value[_],
  V8 <: Value[_],
  V9 <: Value[_],
  C <: Context[C]
] extends SetDimensionMatrix[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, C] {
  /**
   * Permute the order of the coordinates in a position.
   *
   * @param dim1 Dimension to use for the first coordinate.
   * @param dim2 Dimension to use for the second coordinate.
   * @param dim3 Dimension to use for the third coordinate.
   * @param dim4 Dimension to use for the fourth coordinate.
   * @param dim5 Dimension to use for the fifth coordinate.
   * @param dim6 Dimension to use for the sixth coordinate.
   * @param dim7 Dimension to use for the seventh coordinate.
   * @param dim8 Dimension to use for the eighth coordinate.
   * @param dim9 Dimension to use for the ninth coordinate.
   */
  def permute[
    D1 <: Nat,
    D2 <: Nat,
    D3 <: Nat,
    D4 <: Nat,
    D5 <: Nat,
    D6 <: Nat,
    D7 <: Nat,
    D8 <: Nat,
    D9 <: Nat,
    W1 <: Value[_],
    W2 <: Value[_],
    W3 <: Value[_],
    W4 <: Value[_],
    W5 <: Value[_],
    W6 <: Value[_],
    W7 <: Value[_],
    W8 <: Value[_],
    W9 <: Value[_]
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8,
    dim9: D9
  )(implicit
    ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D1, W1],
    ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D2, W2],
    ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D3, W3],
    ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D4, W4],
    ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D5, W5],
    ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D6, W6],
    ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D7, W7],
    ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D8, W8],
    ev9: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, D9, W9],
    ev10: IsDistinctConstraint[D1 :: D2 :: D3 :: D4 :: D5 :: D6 :: D7 :: D8 :: D9 :: HNil]
  ): C#U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: W8 :: W9 :: HNil]]
}

/**
 * Convenience type for access results from `load` methods that return the data and any parse errors.
 *
 * @param data   The parsed matrix.
 * @param errors Any parse errors.
 */
case class MatrixWithParseErrors[P <: HList, U[_]](data: U[Cell[P]], errors: U[String])

