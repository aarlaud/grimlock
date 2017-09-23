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

package commbank.grimlock.scalding

import commbank.grimlock.framework.{
  Cell,
  Compactable,
  Locate,
  Matrix => FwMatrix,
  Matrix1D => FwMatrix1D,
  Matrix2D => FwMatrix2D,
  Matrix3D => FwMatrix3D,
  Matrix4D => FwMatrix4D,
  Matrix5D => FwMatrix5D,
  Matrix6D => FwMatrix6D,
  Matrix7D => FwMatrix7D,
  Matrix8D => FwMatrix8D,
  Matrix9D => FwMatrix9D,
  MultiDimensionMatrix => FwMultiDimensionMatrix,
  Persist => FwPersist,
  Stream
}
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ LongValue, Value }
import commbank.grimlock.framework.environment.tuner.{
  Binary,
  Default,
  InMemory,
  Redistribute,
  Reducers,
  Ternary,
  Tuner
}
import commbank.grimlock.framework.metadata.{ DiscreteSchema, NominalSchema, Type }
import commbank.grimlock.framework.pairwise.{ Comparer, Operator, OperatorWithValue }
import commbank.grimlock.framework.partition.{ Partitioner, PartitionerWithValue }
import commbank.grimlock.framework.position.{ Position, Positions => FwPositions, Slice }
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }
import commbank.grimlock.framework.squash.{ Squasher, SquasherWithValue }
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }
import commbank.grimlock.framework.utility.Escape
import commbank.grimlock.framework.window.{ Window, WindowWithValue }

import commbank.grimlock.scalding.distance.PairwiseDistance
import commbank.grimlock.scalding.distribution.ApproximateDistribution
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.{ MapMapSideJoin, SetMapSideJoin }
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.statistics.Statistics

import com.twitter.algebird.Semigroup
import com.twitter.scalding.typed.ValuePipe

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

import shapeless.{ ::, =:!=, HList, HNil, IsDistinctConstraint, Nat, Witness }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8 }
import shapeless.ops.nat.ToInt

/** Case class matrix operations using a `TypedPipe[Cell[P]]`. */
case class Matrix[
  P <: HList
](
  data: Context.U[Cell[P]]
) extends FwMatrix[P, Context]
  with Persist[Cell[P]]
  with ApproximateDistribution[P]
  with Statistics[P] {
  def change[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = InMemory()
  )(
    positions: Context.U[Position[S]],
    decoder: Content.Decoder,
    writer: FwPersist.TextWriter[Cell[P]]
  )(implicit
    ev: FwMatrix.ChangeTuner[Context.U, T]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    import slice.slc

    val msj = Option(SetMapSideJoin[Position[S], Cell[P]]())

    val result = data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .flatMap {
        case (_, (c, Some(_))) => decoder(c.content.value.toShortString)
          .map { case con => List(Right(Cell(c.position, con))) }
          .getOrElse(writer(c).map { case e => Left(e) })
        case (_, (c, None)) => List(Right(c))
      }

    (result.collect { case Right(cell) => cell }, result.collect { case Left(error) => error })
  }

  def compact(): Context.E[Map[Position[P], Content]] = {
    val semigroup = new Semigroup[Map[Position[P], Content]] {
      def plus(l: Map[Position[P], Content], r: Map[Position[P], Content]) = l ++ r
    }

    ValuePipe(Map[Position[P], Content]())
      .leftCross(data.map { case c => Map(c.position -> c.content) }.sum(semigroup))
      .map { case (e, m) => m.getOrElse(e) }
  }

  def compact[
    S <: HList,
    R <: HList,
    T <: Tuner,
    V[_ <: HList]
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Compactable[P, V],
    ev3: FwMatrix.CompactTuner[Context.U, T]
  ): Context.E[Map[Position[S], V[R]]] = {
    import slice.slc

    val semigroup = new Semigroup[Map[Position[S], V[R]]] {
      def plus(l: Map[Position[S], V[R]], r: Map[Position[S], V[R]]) = l ++ r
    }

    ValuePipe(Map[Position[S], V[R]]())
      .leftCross(
        data
          .map { case c => (slice.selected(c.position), ev2.toMap(slice, c)) }
          .tunedReduce(tuner, (l, r) => ev2.combineMaps(l, r))
          .values
          .sum(semigroup)
      )
      .map { case (e, m) => m.getOrElse(e) }
  }

  def get[
    T <: Tuner
  ](
    positions: Context.U[Position[P]],
    tuner: T = InMemory()
  )(implicit
    ev1: Position.ListConstraints[P],
    ev2: FwMatrix.GetTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val msj = Option(SetMapSideJoin[Position[P], Cell[P]]())

    data
      .map { case c => (c.position, c) }
      .tunedJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .map { case (_, (c, _)) => c }
  }

  def materialise(context: Context): List[Cell[P]] = data
    .toIterableExecution
    .waitFor(context.config, context.mode)
    .getOrElse(Iterable.empty)
    .toList

  def names[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: FwPositions.NamesTuner[Context.U, T]
  ): Context.U[Position[S]] = {
    import slice.slc

    data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(tuner)
  }

  def pairwise[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    comparer: Comparer,
    operators: Operator[P, Q]*
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Position.GreaterThanConstraints[Q, R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    comparer: Comparer,
    value: Context.E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Position.GreaterThanConstraints[Q, R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner)
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
  }

  def pairwiseBetween[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: Context.U[Cell[P]],
    operators: Operator[P, Q]*
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Position.GreaterThanConstraints[Q, R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: Context.U[Cell[P]],
    value: Context.E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Position.GreaterThanConstraints[Q, R],
    ev3: FwMatrix.PairwiseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner)
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
  }

  def relocate[
    Q <: HList
  ](
    locate: Locate.FromCell[P, Q]
  )(implicit
    ev: Position.GreaterEqualConstraints[Q, P]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c => locate(c).map { case p => Cell(p, c.content) } }

  def relocateWithValue[
    Q <: HList,
    W
  ](
    value: Context.E[W],
    locate: Locate.FromCellWithValue[P, Q, W]
  )(implicit
    ev: Position.GreaterEqualConstraints[Q, P]
  ): Context.U[Cell[Q]] = data.flatMapWithValue(value) { case (c, vo) =>
    vo.flatMap { case v => locate(c, v).map { case p => Cell(p, c.content) } }
  }

  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[Cell[P]] = saveText(context, file, writer, tuner)

  def set[
    T <: Tuner
  ](
    values: Context.U[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev1: Position.ListConstraints[P],
    ev2: FwMatrix.SetTuner[Context.U, T]
  ): Context.U[Cell[P]] = data
    .map { case c => (c.position, c) }
    .tunedOuterJoin(tuner, values.map { case c => (c.position, c) })
    .flatMap { case (_, (co, cn)) => cn.orElse(co) }

  def shape[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev1: Position.ListConstraints[P],
    ev2: FwMatrix.ShapeTuner[Context.U, T]
  ): Context.U[Cell[LongValue :: HNil]] = data
    .flatMap { case c => c.position.asList.map { case c => c.toString }.zipWithIndex }
    .tunedDistinct(tuner)
    .map { case (s, i) => i }
    .tunedSize(tuner)
    .map { case (i, c) => Cell(Position(i + 1), Content(DiscreteSchema[Long](), c)) }

  def size[
    D <: Nat : ToInt,
    T <: Tuner,
    V <: Value[_] : ClassTag
  ](
    dim: D,
    distinct: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: FwMatrix.SizeTuner[Context.U, T]
  ): Context.U[Cell[LongValue :: HNil]] = {
    val coords = data.map { case c => c.position(dim) }
    val dist = if (distinct) coords else coords.tunedDistinct(tuner)(Value.ordering())

    dist
      .map { case c => 1L }
      .sum
      .map { case sum => Cell(Position(Nat.toInt[D]), Content(DiscreteSchema[Long](), sum)) }
  }

  def slice[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = InMemory()
  )(
    keep: Boolean,
    positions: Context.U[Position[S]]
  )(implicit
    ev: FwMatrix.SliceTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    import slice.slc

    val msj = Option(SetMapSideJoin[Position[S], Cell[P]]())

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  def slide[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    ascending: Boolean,
    windows: Window[P, S, R, Q]*
  )(implicit
    ev1: Position.NonEmptyConstraints[R],
    ev2: Position.GreaterThanConstraints[Q, S],
    ev3: FwMatrix.SlideTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    import slice.{ slc, rlc }

    val window: Window[P, S, R, Q] = if (windows.size == 1) windows.head else windows.toList

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMap {
        case (p, Some((_, to))) => to.flatMap { case o => window.present(p, o) }
        case _ => List()
      }
  }

  def slideWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    ascending: Boolean,
    value: Context.E[W],
    windows: WindowWithValue[P, S, R, Q] { type V >: W }*
  )(implicit
    ev1: Position.NonEmptyConstraints[R],
    ev2: Position.GreaterThanConstraints[Q, S],
    ev3: FwMatrix.SlideTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    import slice.{ slc, rlc }

    val window: WindowWithValue[P, S, R, Q] { type V >: W } =
      if (windows.size == 1) windows.head else windows.toList

    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.map { case v => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, v))) }
      }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMapWithValue(value) {
        case ((p, Some((_, to))), Some(v)) => to.flatMap { case o => window.presentWithValue(p, o, v) }
        case _ => List()
      }
  }

  def split[I](partitioners: Partitioner[P, I]*): Context.U[(I, Cell[P])] = {
    val partitioner: Partitioner[P, I] = if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
  }

  def splitWithValue[
    I,
    W
  ](
    value: Context.E[W],
    partitioners: PartitionerWithValue[P, I] { type V >: W }*
  ): Context.U[(I, Cell[P])] = {
    val partitioner: PartitionerWithValue[P, I] { type V >: W } =
      if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMapWithValue(value) { case (c, vo) =>
      vo.toList.flatMap { case v => partitioner.assignWithValue(c, v).map { case q => (q, c) } }
    }
  }

  def stream[
    Q <: HList
  ](
    command: String,
    files: List[String],
    writer: FwPersist.TextWriter[Cell[P]],
    parser: FwPersist.TextParser[Cell[Q]],
    reducers: Reducers,
    hash: (Position[P]) => Int
  ): (Context.U[Cell[Q]], Context.U[String]) = {
    val tuner = Default(reducers)

    val result = data
      .flatMap { case c => writer(c).map { case s => (hash(c.position) % reducers.reducers, s) } }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def streamByPosition[
    S <: HList,
    R <: HList,
    Q <: HList
  ](
    slice: Slice[P, S, R]
  )(
    command: String,
    files: List[String],
    writer: FwPersist.TextWriterByPosition[Cell[P]],
    parser: FwPersist.TextParser[Cell[Q]],
    reducers: Reducers
  )(implicit
    ev: Position.GreaterEqualConstraints[Q, S]
  ): (Context.U[Cell[Q]], Context.U[String]) = {
    import slice.slc

    val tuner = Default(reducers)
    val murmur = new scala.util.hashing.MurmurHash3.ArrayHashing[Value[_]]()
    val (rows, _) = Util.pivot(data, slice, tuner)

    val result = rows
      .flatMap { case (key, list) => writer(list.map { case (_, v) => v })
        .map { case s => (murmur.hash(key.asList.toArray) % reducers.reducers, s) }
      }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def subset(samplers: Sampler[P]*): Context.U[Cell[P]] = {
    val sampler: Sampler[P] = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](value: Context.E[W], samplers: SamplerWithValue[P] { type V >: W }*): Context.U[Cell[P]] = {
    val sampler: SamplerWithValue[P] { type V >: W } = if (samplers.size == 1) samplers.head else samplers.toList

    data.filterWithValue(value) { case (c, vo) => vo.map { case v => sampler.selectWithValue(c, v) }.getOrElse(false) }
  }

  def summarise[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    aggregators: Aggregator[P, S, Q]*
  )(implicit
    ev1: Aggregator.Validate[P, S, Q],
    ev2: Position.GreaterEqualConstraints[Q, S],
    ev3: FwMatrix.SummariseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    import slice.slc

    val aggregator = ev1.check(aggregators)

    data
      .flatMap { case c => aggregator.prepare(c).map { case t => (slice.selected(c.position), t) } }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMap { case (p, t) => aggregator.present(p, t) }
  }

  def summariseWithValue[
    S <: HList,
    R <: HList,
    Q <: HList,
    W,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    value: Context.E[W],
    aggregators: AggregatorWithValue[P, S, Q] { type V >: W }*
  )(implicit
    ev1: AggregatorWithValue.Validate[P, S, Q, W],
    ev2: Position.GreaterEqualConstraints[Q, S],
    ev3: FwMatrix.SummariseTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    import slice.slc

    val aggregator = ev1.check(aggregators)

    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => aggregator.prepareWithValue(c, v).map { case t => (slice.selected(c.position), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.toList.flatMap { case v => aggregator.presentWithValue(p, t, v) }
      }
  }

  def toSequence[
    K <: Writable,
    V <: Writable
  ](
    writer: FwPersist.SequenceWriter[Cell[P], K, V]
  ): Context.U[(K, V)] = data.flatMap { case c => writer(c) }

  def toText(writer: FwPersist.TextWriter[Cell[P]]): Context.U[String] = data.flatMap { case c => writer(c) }

  def toVector[
    V <: Value[_]
  ](
    melt: (Position[P]) => V
  ): Context.U[Cell[V :: HNil]] = data.map { case Cell(p, c) => Cell(Position(melt(p)), c) }

  def transform[
    Q <: HList
  ](
    transformers: Transformer[P, Q]*
  )(implicit
    ev: Position.GreaterEqualConstraints[Q, P]
  ): Context.U[Cell[Q]] = {
    val transformer: Transformer[P, Q] = if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMap { case c => transformer.present(c) }
  }

  def transformWithValue[
    Q <: HList,
    W
  ](
    value: Context.E[W],
    transformers: TransformerWithValue[P, Q] { type V >: W }*
  )(implicit
    ev: Position.GreaterEqualConstraints[Q, P]
  ): Context.U[Cell[Q]] = {
    val transformer: TransformerWithValue[P, Q] { type V >: W } =
      if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMapWithValue(value) { case (c, vo) => vo.toList.flatMap { case v => transformer.presentWithValue(c, v) } }
  }

  def types[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    specific: Boolean
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: FwMatrix.TypesTuner[Context.U, T]
  ): Context.U[Cell[S]] = {
    import slice.slc

    data
      .map { case Cell(p, c) => (slice.selected(p), c.schema.classification) }
      .tunedReduce(tuner, (lt, rt) => lt.getCommonType(rt))
      .map { case (p, t) => Cell(p, Content(NominalSchema[Type](), if (specific) t else t.getRootType)) }
  }

  def unique[T <: Tuner](tuner: T = Default())(implicit ev: FwMatrix.UniqueTuner[Context.U, T]): Context.U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map { case c => c.content }
      .tunedDistinct(tuner)(ordering)
  }

  def uniqueByPosition[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: FwMatrix.UniqueTuner[Context.U, T]
  ): Context.U[(Position[S], Content)] = {
    val ordering = new Ordering[Cell[S]] {
      def compare(l: Cell[S], r: Cell[S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  def which(predicate: Cell.Predicate[P]): Context.U[Position[P]] = data
    .collect { case c if predicate(c) => c.position }

  def whichByPosition[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = InMemory()
  )(
    predicates: List[(Context.U[Position[S]], Cell.Predicate[P])]
  )(implicit
    ev: FwMatrix.WhichTuner[Context.U, T]
  ): Context.U[Position[P]] = {
    import slice.slc

    val msj = Option(MapMapSideJoin[Position[S], Cell[P], List[Cell.Predicate[P]]]())

    val pp = predicates
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)
      .map { case (pos, pred) => (pos, List(pred)) }
      .tunedReduce(tuner, _ ++ _)

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(tuner, pp, msj)
      .collect { case (_, (c, lst)) if (lst.exists(pred => pred(c))) => c.position }
  }

  private def pairwiseTuples[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    comparer: Comparer,
    ldata: Context.U[Cell[P]],
    rdata: Context.U[Cell[P]],
    tuner: T
  ): Context.U[(Cell[P], Cell[P])] = {
    import slice.slc

    tuner match {
      case InMemory(_) =>
        ldata
          .tunedCross[Cell[P]](
            tuner,
            (lc, rc) => comparer.keep(slice.selected(lc.position), slice.selected(rc.position)),
            rdata
          )
      case _ =>
        def msj[V] = Option(MapMapSideJoin[Position[S], Cell[P], V]())

        val (ct, lt, rt) = tuner match {
          case Ternary(a, s, r) => (a, s, r)
          case _ => (Default(), Default(), Default())
        }

        ldata
          .map { case c => (slice.selected(c.position), c) }
          .tunedJoin(
            lt,
            ldata
              .map { case Cell(p, _) => slice.selected(p) }
              .tunedDistinct(lt)
              .tunedCross[Position[S]](
                ct,
                (lp, rp) => comparer.keep(lp, rp),
                rdata.map { case Cell(p, _) => slice.selected(p) }.tunedDistinct(rt)
              )
              .forceToDisk, // TODO: Should this be configurable?
            msj
          )
          .map { case (_, (lc, rp)) => (rp, lc) }
          .tunedJoin(rt, rdata.map { case c => (slice.selected(c.position), c) }, msj)
          .map { case (_, (lc, rc)) => (lc, rc) }
    }
  }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: HNil]]`. */
case class Matrix1D[
  V1 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: HNil, _0, V1]
) extends FwMatrix1D[V1, Context]
  with MatrixXD[V1 :: HNil] {
  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: HNil]] = {
    def msj[V] = Option(MapMapSideJoin[Position[V1 :: HNil], V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (c.position, c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: HNil]] = coordinates[_0, V1](tuner)
    .map { case c1 => Position(c1) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: HNil]]`. */
case class Matrix2D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: HNil, _1, V2]
) extends FwMatrix2D[V1, V2, Context]
  with MatrixXD[V1 :: V2 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: HNil]] = data.map { case cell =>
    Cell(Position(cell.position(dim1) :: cell.position(dim2) :: HNil), cell.content)
  }

  def saveAsCSV[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    separator: String,
    escapee: Escape,
    writeHeader: Boolean,
    header: String,
    writeRowId: Boolean,
    rowId: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY],
    ev3: FwMatrix.SaveAsCSVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    val (pt, rt) = tuner match {
      case Binary(p, r) => (p, r)
      case r @ Redistribute(_) => (Default(), r)
      case p @ Default(_) => (p, Default())
      case _ => (Default(), Default())
    }

    val (pivoted, columns) = Util.pivot(data, slice, pt)

    if (writeHeader)
      columns
        .map { case lst =>
          (if (writeRowId) escapee.escape(rowId) + separator else "") + lst
            .map { case p => escapee.escape(p(_0).toShortString) }
            .mkString(separator)
        }
        .toTypedPipe
        .tunedSaveAsText(context, Redistribute(1), header.format(file))

    pivoted
      .map { case (p, lst) =>
        (if (writeRowId) escapee.escape(p(_0).toShortString) + separator else "") + lst
          .map { case (_, v) => v.map { case c => escapee.escape(c.content.value.toShortString) }.getOrElse("") }
          .mkString(separator)
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  def saveAsVW[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY],
    ev3: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(slice, tuner)(context, file, None, None, tag, dictionary, separator)

  def saveAsVWWithLabels[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    labels: Context.U[Cell[S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY],
    ev3: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(
    slice,
    tuner
  )(
    context,
    file,
    Option(labels),
    None,
    tag,
    dictionary,
    separator
  )

  def saveAsVWWithImportance[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    importance: Context.U[Cell[S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY],
    ev3: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(
    slice,
    tuner
  )(
    context,
    file,
    None,
    Option(importance),
    tag,
    dictionary,
    separator
  )

  def saveAsVWWithLabelsAndImportance[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T = Default()
  )(
    context: Context,
    file: String,
    labels: Context.U[Cell[S]],
    importance: Context.U[Cell[S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY],
    ev3: FwMatrix.SaveAsVWTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = saveVW(
    slice,
    tuner
  )(
    context,
    file,
    Option(labels),
    Option(importance),
    tag,
    dictionary,
    separator
  )

  private def saveVW[
    S <: HList,
    R <: HList,
    T <: Tuner,
    VX <: Value[_],
    VY <: Value[_]
  ](
    slice: Slice[V1 :: V2 :: HNil, S, R],
    tuner: T
  )(
    context: Context,
    file: String,
    labels: Option[Context.U[Cell[S]]],
    importance: Option[Context.U[Cell[S]]],
    tag: Boolean,
    dictionary: String,
    separator: String
  )(implicit
    ev1: Position.IndexConstraints[S, _0, VX],
    ev2: Position.IndexConstraints[R, _0, VY]
  ): Context.U[Cell[V1 :: V2 :: HNil]] = {
    import slice.slc

    val msj = Option(MapMapSideJoin[Position[S], String, Cell[S]]())

    val (pt, jt, rt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case Binary(t @ Default(_), r @ Redistribute(_)) => (t, t, r)
      case Binary(p, j) => (p, j, Default())
      case t @ Default(Reducers(_)) => (t, t, Default())
      case _ => (Default(), Default(), Default())
    }

    val (pivoted, columns) = Util.pivot(data, slice, pt)
    val dict = columns.map { case l => l.zipWithIndex.toMap }

    dict
      .flatMap { case m => m.map { case (p, i) => p(_0).toShortString + separator + i } }
      .tunedSaveAsText(context, Redistribute(1), dictionary.format(file))

    val features = pivoted
      .flatMapWithValue(dict) { case ((key, lst), dct) =>
        dct.map { case d =>
          (
            key,
            lst
              .flatMap { case (p, v) =>
                v.flatMap { case c => c.content.value.asDouble.map { case w => d(p) + ":" + w } }
              }
              .mkString((if (tag) key(_0).toShortString else "") + "| ", " ", "")
          )
        }
      }

    val weighted = importance match {
      case Some(imp) => features
        .tunedJoin(jt, imp.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => features
    }

    val examples = labels match {
      case Some(lab) => weighted
        .tunedJoin(jt, lab.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .map { case (c1, c2) => Position(c1, c2) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: HNil]]`. */
case class Matrix3D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: HNil, _2, V3]
) extends FwMatrix3D[V1, V2, V3, Context]
  with MatrixXD[V1 :: V2 :: V3 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: HNil]] = data.map { case cell =>
    Cell(Position(cell.position(dim1) :: cell.position(dim2) :: cell.position(dim3) :: HNil), cell.content)
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[V1 :: V2 :: V3 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .map { case ((c1, c2), c3) => Position(c1, c2, c3) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]`. */
case class Matrix4D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
) extends FwMatrix4D[V1, V2, V3, V4, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: HNil]] = data.map { case cell =>
    Cell(
      Position(cell.position(dim1) :: cell.position(dim2) :: cell.position(dim3) :: cell.position(dim4) :: HNil),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) =>
        i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .map { case (((c1, c2), c3), c4) => Position(c1, c2, c3, c4) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]`. */
case class Matrix5D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag,
  V5 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
  ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
) extends FwMatrix5D[V1, V2, V3, V4, V5, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: HNil]] = data.map { case cell =>
    Cell(
      Position(
        cell.position(dim1) ::
        cell.position(dim2) ::
        cell.position(dim3) ::
        cell.position(dim4) ::
        cell.position(dim5) ::
        HNil
      ),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_4)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) =>
        i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .tunedCross[V5](tuner, (_, _) => true, coordinates[_4, V5](tuner))
    .map { case ((((c1, c2), c3), c4), c5) => Position(c1, c2, c3, c4, c5) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]`. */
case class Matrix6D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag,
  V5 <: Value[_] : ClassTag,
  V6 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
  ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
  ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
) extends FwMatrix6D[V1, V2, V3, V4, V5, V6, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: HNil]] = data.map { case cell =>
    Cell(
      Position(
        cell.position(dim1) ::
        cell.position(dim2) ::
        cell.position(dim3) ::
        cell.position(dim4) ::
        cell.position(dim5) ::
        cell.position(dim6) ::
        HNil
      ),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_4)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_5)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .tunedCross[V5](tuner, (_, _) => true, coordinates[_4, V5](tuner))
    .tunedCross[V6](tuner, (_, _) => true, coordinates[_5, V6](tuner))
    .map { case (((((c1, c2), c3), c4), c5), c6) => Position(c1, c2, c3, c4, c5, c6) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]`. */
case class Matrix7D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag,
  V5 <: Value[_] : ClassTag,
  V6 <: Value[_] : ClassTag,
  V7 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
  ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
  ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
  ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
) extends FwMatrix7D[V1, V2, V3, V4, V5, V6, V7, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: HNil]] = data.map { case cell =>
    Cell(
      Position(
        cell.position(dim1) ::
        cell.position(dim2) ::
        cell.position(dim3) ::
        cell.position(dim4) ::
        cell.position(dim5) ::
        cell.position(dim6) ::
        cell.position(dim7) ::
        HNil
      ),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_4)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_5)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_6)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .tunedCross[V5](tuner, (_, _) => true, coordinates[_4, V5](tuner))
    .tunedCross[V6](tuner, (_, _) => true, coordinates[_5, V6](tuner))
    .tunedCross[V7](tuner, (_, _) => true, coordinates[_6, V7](tuner))
    .map { case ((((((c1, c2), c3), c4), c5), c6), c7) => Position(c1, c2, c3, c4, c5, c6, c7) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]`. */
case class Matrix8D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag,
  V5 <: Value[_] : ClassTag,
  V6 <: Value[_] : ClassTag,
  V7 <: Value[_] : ClassTag,
  V8 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
  ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
  ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
  ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
  ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
) extends FwMatrix8D[V1, V2, V3, V4, V5, V6, V7, V8, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: W8 :: HNil]] = data.map { case cell =>
    Cell(
      Position(
        cell.position(dim1) ::
        cell.position(dim2) ::
        cell.position(dim3) ::
        cell.position(dim4) ::
        cell.position(dim5) ::
        cell.position(dim6) ::
        cell.position(dim7) ::
        cell.position(dim8) ::
        HNil
      ),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_4)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_5)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_6)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_7)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(_7, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .tunedCross[V5](tuner, (_, _) => true, coordinates[_4, V5](tuner))
    .tunedCross[V6](tuner, (_, _) => true, coordinates[_5, V6](tuner))
    .tunedCross[V7](tuner, (_, _) => true, coordinates[_6, V7](tuner))
    .tunedCross[V8](tuner, (_, _) => true, coordinates[_7, V8](tuner))
    .map { case (((((((c1, c2), c3), c4), c5), c6), c7), c8) => Position(c1, c2, c3, c4, c5, c6, c7, c8) }
}

/** Rich wrapper around a `TypedPipe[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]`. */
case class Matrix9D[
  V1 <: Value[_] : ClassTag,
  V2 <: Value[_] : ClassTag,
  V3 <: Value[_] : ClassTag,
  V4 <: Value[_] : ClassTag,
  V5 <: Value[_] : ClassTag,
  V6 <: Value[_] : ClassTag,
  V7 <: Value[_] : ClassTag,
  V8 <: Value[_] : ClassTag,
  V9 <: Value[_] : ClassTag
](
  data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]
)(implicit
  ev1: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
  ev2: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
  ev3: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
  ev4: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
  ev5: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
  ev6: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
  ev7: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
  ev8: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
  ev9: Position.IndexConstraints[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
) extends FwMatrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9, Context]
  with MatrixXD[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] {
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
  ): Context.U[Cell[W1 :: W2 :: W3 :: W4 :: W5 :: W6 :: W7 :: W8 :: W9 :: HNil]] = data.map { case cell =>
    Cell(
      Position(
        cell.position(dim1) ::
        cell.position(dim2) ::
        cell.position(dim3) ::
        cell.position(dim4) ::
        cell.position(dim5) ::
        cell.position(dim6) ::
        cell.position(dim7) ::
        cell.position(dim8) ::
        cell.position(dim9) ::
        HNil
      ),
      cell.content
    )
  }

  def saveAsIV[
    T <: Tuner
  ](
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.SaveAsIVTuner[Context.U, T]
  ): Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]] = {
    def msj[K, V] = Option(MapMapSideJoin[K, V, Long]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_0)), c) }
      .tunedJoin(jt, saveDictionary(_0, context, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_1)), (c, i)) }
      .tunedJoin(jt, saveDictionary(_1, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_2)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(_2, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_3)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(_3, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_4)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(_4, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_5)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(_5, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_6)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(_6, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_7)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(_7, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (Position(c.position(_8)), (c, i, j, k, l, m, n, o, p)) }
      .tunedJoin(jt, saveDictionary(_8, context, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        q + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(context, rt, file)

    data
  }

  protected def naturalDomain(
    tuner: Tuner
  ): Context.U[Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]] = coordinates[_0, V1](tuner)
    .tunedCross[V2](tuner, (_, _) => true, coordinates[_1, V2](tuner))
    .tunedCross[V3](tuner, (_, _) => true, coordinates[_2, V3](tuner))
    .tunedCross[V4](tuner, (_, _) => true, coordinates[_3, V4](tuner))
    .tunedCross[V5](tuner, (_, _) => true, coordinates[_4, V5](tuner))
    .tunedCross[V6](tuner, (_, _) => true, coordinates[_5, V6](tuner))
    .tunedCross[V7](tuner, (_, _) => true, coordinates[_6, V7](tuner))
    .tunedCross[V8](tuner, (_, _) => true, coordinates[_7, V8](tuner))
    .tunedCross[V9](tuner, (_, _) => true, coordinates[_8, V9](tuner))
    .map { case ((((((((c1, c2), c3), c4), c5), c6), c7), c8), c9) => Position(c1, c2, c3, c4, c5, c6, c7, c8, c9) }
}

/** Trait for XD specific implementations. */
trait MatrixXD[P <: HList] extends Persist[Cell[P]] {
  def domain[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev: FwMatrix.DomainTuner[Context.U, T]
  ): Context.U[Position[P]] = naturalDomain(tuner)

  def fillHeterogeneous[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    values: Context.U[Cell[S]]
  )(implicit
    ev1: Position.ListConstraints[P],
    ev2: FwMatrix.FillHeterogeneousTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    import slice.slc

    val msj = Option(MapMapSideJoin[Position[S], Position[P], Content]())

    val (dt, vt, jt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case _ => (Default(), Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (slice.selected(p), p) }
      .tunedJoin(vt, values.map { case c => (c.position, c.content) }, msj)
      .map { case (_, (p, c)) => (p, Cell(p, c)) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  def fillHomogeneous[
    T <: Tuner
  ](
    value: Content,
    tuner: T = Default()
  )(implicit
    ev1: Position.ListConstraints[P],
    ev2: FwMatrix.FillHomogeneousTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    val (dt, jt) = tuner match {
      case Binary(f, s) => (f, s)
      case _ => (Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (p, ()) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  protected def coordinates[
    D <: Nat : ToInt,
    V <: Value[_] : ClassTag
  ](
    tuner: Tuner
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: Witness.Aux[D]
  ): Context.U[V] = data.map { case c => c.position(ev2.value) }.tunedDistinct(tuner)(Value.ordering())

  protected def getSaveAsIVTuners(tuner: Tuner): (Tuner, Tuner) = tuner match {
    case Binary(j, r) => (j, r)
    case _ => (Default(), Default())
  }

  protected def naturalDomain(tuner: Tuner): Context.U[Position[P]]

  protected def saveDictionary[
    D <: Nat : ToInt,
    V <: Value[_] : ClassTag
  ](
    dim: D,
    context: Context,
    file: String,
    dictionary: String,
    separator: String,
    tuner: Tuner
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: Witness.Aux[D]
  ): Context.U[(Position[V :: HNil], Long)] = {
    val numbered = coordinates[D, V](tuner)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (c, i)) => (Position(c), i.toLong) }

    numbered
      .map { case (Position(c :: HNil), i) => c.toShortString + separator + i }
      .tunedSaveAsText(context, Redistribute(1), dictionary.format(file, Nat.toInt[D]))

    numbered
  }
}

/** Case class for methods that change the number of dimensions using a `TypedPipe[Cell[P]]`. */
case class MultiDimensionMatrix[
  P <: HList
](
  data: Context.U[Cell[P]]
) extends FwMultiDimensionMatrix[P, Context]
  with PairwiseDistance[P] {
  def join[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    that: Context.U[Cell[P]]
  )(implicit
    ev: FwMatrix.JoinTuner[Context.U, T]
  ): Context.U[Cell[P]] = {
    import slice.slc

    def msj[V] = Option(SetMapSideJoin[Position[S], V]())

    val (t1, t2) = tuner match {
      case Binary(f, s) => (f, s)
      case t => (t, t)
    }

    val keep = data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(t1)
      .map { case p => (p, ()) }
      .tunedJoin(
        t1,
        that
          .map { case c => slice.selected(c.position) }
          .tunedDistinct(t1)
          .map { case p => (p, ()) },
        msj
      )
      .map { case (p, _) => (p, ()) }
      .forceToDisk // TODO: Should this be configurable?

    (data ++ that)
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(t2, keep, msj)
      .map { case (_, (c, _)) => c }
  }

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
  ): Context.U[Cell[Q]] = data.map { case Cell(p, c) => Cell(p.melt(dim, into, merge), c) }

  def reshape[
    D <: Nat,
    V <: Value[_],
    W <: Value[_],
    Q <: HList,
    T <: Tuner
  ](
    dim: D,
    coordinate: V,
    locate: Locate.FromCellAndOptionalValue[P, Q],
    tuner: T = Default()
  )(implicit
    ev1: Position.IndexConstraints[P, D, W],
    ev2: Position.RemoveConstraints[P, D, Q],
    ev3: Position.GreaterThanConstraints[Q, P],
    ev4: Position.ListConstraints[Q],
    ev5: FwMatrix.ReshapeTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val msj = Option(MapMapSideJoin[Position[Q], Cell[P], Value[_]]())

    val keys = data
      .collect[(Position[Q], Value[_])] { case c if (c.position(dim) equ coordinate) =>
        (c.position.remove(dim), c.content.value)
      }

    data
      .collect { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
      .tunedLeftJoin(tuner, keys, msj)
      .flatMap { case (_, (c, v)) => locate(c, v).map { case p => Cell(p, c.content) } }
  }

  def squash[
    D <: Nat,
    Q <: HList,
    T <: Tuner,
    V <: Value[_]
  ](
    dim: D,
    squasher: Squasher[P, D],
    tuner: T = Default()
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: Position.RemoveConstraints[P, D, Q],
    ev3: Position.ListConstraints[Q],
    ev4: FwMatrix.SquashTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    data
      .flatMap { case c => squasher.prepare(c, dim).map { case t => (c.position.remove(dim), t) } }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMap { case (p, t) => squasher.present(t).map { case c => Cell(p, c) } }
  }

  def squashWithValue[
    D <: Nat,
    Q <: HList,
    W,
    T <: Tuner,
    V <: Value[_]
  ](
    dim: D,
    value: Context.E[W],
    squasher: SquasherWithValue[P, D] { type V >: W },
    tuner: T = Default()
  )(implicit
    ev1: Position.IndexConstraints[P, D, V],
    ev2: Position.RemoveConstraints[P, D, Q],
    ev3: Position.ListConstraints[Q],
    ev4: FwMatrix.SquashTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => squasher.prepareWithValue(c, dim, v).map { case t => (c.position.remove(dim), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.flatMap { case v => squasher.presentWithValue(t, v).map { case c => Cell(p, c) } }
      }
  }
}

private object Util {
  def pivot[
    P <: HList,
    S <: HList,
    R <: HList
  ](
    data: Context.U[Cell[P]],
    slice: Slice[P, S, R],
    tuner: Tuner
  ): (
    Context.U[(Position[S], List[(Position[R], Option[Cell[P]])])],
    Context.E[List[Position[R]]]
  ) = {
    import slice.{ slc, rlc }

    def setSemigroup = new Semigroup[Set[Position[R]]] {
      def plus(l: Set[Position[R]], r: Set[Position[R]]) = l ++ r
    }
    def mapSemigroup = new Semigroup[Map[Position[R], Cell[P]]] {
      def plus(l: Map[Position[R], Cell[P]], r: Map[Position[R], Cell[P]]) = l ++ r
    }

    val columns = data
      .map { case c => Set(slice.remainder(c.position)) }
      .sum(setSemigroup)
      .map { case s => s.toList.sorted }

    val pivoted = data
      .map { case c => (slice.selected(c.position), Map(slice.remainder(c.position) -> c)) }
      .tuneReducers(tuner)
      .sum(mapSemigroup)
      .flatMapWithValue(columns) { case ((key, map), opt) =>
        opt.map { case cols => (key, cols.map { case c => (c, map.get(c)) }) }
      }

    (pivoted, columns)
  }
}

