// Copyright 2017 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.distance._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.environment._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.partition._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._
import commbank.grimlock.library.window._

import scala.io.Source

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2, _3 }

object Shared {
  def test1[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SetTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, InMemory[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .saveAsText(ctx, s"./tmp.${tool}/dat1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .set(
        Cell(
          Position("iid:1548763", "fid:Y", DateValue(DateCodec().decode("2014-04-26").get, DateCodec())),
          Content(ContinuousSchema[Long](), 1234)
        ),
        Default()
      )
      .slice(Over(_0), InMemory())(true, "iid:1548763")
      .saveAsText(ctx, s"./tmp.${tool}/dat2.out", (c) => List(c.toString), Default())
      .toUnit

    ctx
      .loadText(
        path + "/smallInputfile.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, "|")
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/dat3.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test2[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, InMemory[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    (
      data.names(Over(_0), Default()).map(_.toString) ++
      data.names(Over(_1), Default()).map(_.toString) ++
      data.names(Over(_2), Default()).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/nm0.out", Default())
      .toUnit

    data
      .names(Over(_1), Default())
      .slice(false, "fid:M")
      .saveAsText(ctx, s"./tmp.${tool}/nm2.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .names(Over(_1), Default())
      .slice(true, """.*[BCD]$""".r)
      .saveAsText(ctx, s"./tmp.${tool}/nm5.out", (p) => List(p.toString), Default())
      .toUnit
  }

  def test3[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.TypesTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    (
      data.types(Over(_0), Default())(false).map(_.toString) ++
      data.types(Over(_1), Default())(false).map(_.toString) ++
      data.types(Over(_2), Default())(false).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/typ1.out", Default())
      .toUnit

    (
      data.types(Over(_0), Default())(true).map(_.toString) ++
      data.types(Over(_1), Default())(true).map(_.toString) ++
      data.types(Over(_2), Default())(true).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/typ2.out", Default())
      .toUnit
  }

  def test4[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, "fid:B")
      .saveAsText(ctx, s"./tmp.${tool}/scl0.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_0), Default())(true, "iid:0221707")
      .saveAsText(ctx, s"./tmp.${tool}/scl1.out", (c) => List(c.toString), Default())
      .toUnit

    val rem = List(
      "fid:B",
      "fid:D",
      "fid:F",
      "fid:H",
      "fid:J",
      "fid:L",
      "fid:N",
      "fid:P",
      "fid:R",
      "fid:T",
      "fid:V",
      "fid:X",
      "fid:Z"
    )

    data
      .slice(Over(_1), Default())(false, data.names(Over(_1), Default()).slice(false, rem))
      .saveAsText(ctx, s"./tmp.${tool}/scl2.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test5[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_0), Default())(true, "iid:0221707")
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/sqs1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/sqs2.out", (c) => List(c.toString), Default())
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/sqs3.out")
      .toUnit

    data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/sqs4.out")
      .toUnit
  }

  def test6[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .which(c => c.content.schema.classification.isOfType(NumericType))
      .saveAsText(ctx, s"./tmp.${tool}/whc1.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .which(c => !c.content.value.isInstanceOf[StringValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc2.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .get(
        data.which(c => (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")),
        Default()
      )
      .saveAsText(ctx, s"./tmp.${tool}/whc3.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .which(c => c.content.value.isInstanceOf[LongValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc4.out", (p) => List(p.toString), Default())
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(Along(_0), Default())(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .whichByPosition(
        Over(_1),
        Default()
      )(
        List(("count", c => c.content.value leq 2), ("min", c => c.content.value equ 107))
      )
      .saveAsText(ctx, s"./tmp.${tool}/whc5.out", (p) => List(p.toString), Default())
      .toUnit
  }

  def test7[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val pos = List(
      Position("iid:1548763", "fid:Y", DateValue(DateCodec().decode("2014-04-26").get, DateCodec())),
      Position("iid:1303823", "fid:A", DateValue(DateCodec().decode("2014-05-05").get, DateCodec()))
    )

    data
      .get(pos.head, Default())
      .saveAsText(ctx, s"./tmp.${tool}/get1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .get(
        List(
          Position("iid:1548763", "fid:Y", DateValue(DateCodec().decode("2014-04-26").get, DateCodec())),
          Position("iid:1303823", "fid:A", DateValue(DateCodec().decode("2014-05-05").get, DateCodec()))
        ),
        Default()
      )
      .saveAsText(ctx, s"./tmp.${tool}/get2.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test8[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev6: Matrix.UniqueTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.content._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, "fid:B")
      .squash(_2, PreservingMaximumPosition(), Default())
      .unique(Default())
      .saveAsText(ctx, s"./tmp.${tool}/uniq.out", (c) => List(c.toString), Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .uniqueByPosition(Over(_1), Default())
      .saveAsText(ctx, s"./tmp.${tool}/uni2.out", IndexedContents.toShortString(false, "|"), Default())
      .toUnit

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/test.csv")
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .permute(_1, _0)
      .saveAsText(ctx, s"./tmp.${tool}/trs1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/data.txt", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test9[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    implicit val c = ctx

    case class StringPartitioner[
      P <: HList,
      D <: Nat,
      V <: Value[_]
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints[P, D, V]
    ) extends Partitioner[P, String] {
      def assign(cell: Cell[P]): TraversableOnce[String] = List(cell.position(dim) match {
        case StringValue("fid:A", _) => "training"
        case StringValue("fid:B", _) => "testing"
      }, "scoring")
    }

    val prt1 = data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .split(StringPartitioner(_1))

    prt1
      .saveAsText(ctx, s"./tmp.${tool}/prt1.out", (p) => List(p.toString), Default())
      .toUnit

    case class IntTuplePartitioner[
      P <: HList,
      D <: Nat,
      V <: Value[_]
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints[P, D, V]
    ) extends Partitioner[P, (Int, Int, Int)] {
      def assign(cell: Cell[P]): TraversableOnce[(Int, Int, Int)] = List(cell.position(dim) match {
        case StringValue("fid:A", _) => (1, 0, 0)
        case StringValue("fid:B", _) => (0, 1, 0)
      }, (0, 0, 1))
    }

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .split(IntTuplePartitioner(_1))
      .saveAsText(ctx, s"./tmp.${tool}/prt2.out", (p) => List(p.toString), Default())
      .toUnit

    prt1
      .get("training")
      .saveAsText(ctx, s"./tmp.${tool}/train.out", (c) => List(c.toString), Default())
      .toUnit

    prt1
      .get("testing")
      .saveAsText(ctx, s"./tmp.${tool}/test.out", (c) => List(c.toString), Default())
      .toUnit

    prt1
      .get("scoring")
      .saveAsText(ctx, s"./tmp.${tool}/score.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test10[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .summarise(Over(_1), Default())(Mean(false, true, true).andThenRelocate(_.position.append("mean").toOption))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg1.csv")
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(Along(_1), Default())(Counts().andThenRelocate(_.position.append("count").toOption))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg2.csv")
      .toUnit

    data
      .slice(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
        Skewness().andThenRelocate(_.position.append("skewness").toOption),
        Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg3.csv")
      .toUnit
  }

  def test11[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")))
      .saveAsText(ctx, s"./tmp.${tool}/trn2.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .transform(Binarise(Locate.RenameDimensionWithContent(_1)))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn3.out")
      .toUnit
  }

  def test12[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val sliced = data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))

    sliced
      .squash(_2, PreservingMaximumPosition(), Default())
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll1.out")
      .toUnit

    sliced
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsText(ctx, s"./tmp.${tool}/fll3.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test13[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHeterogeneousTuner[C#U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev3: Matrix.JoinTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev5: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev6: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev7: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev8: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val inds = sliced
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")))
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())

    sliced
      .join(Over(_0), Default())(inds)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll2.out")
      .toUnit

    sliced
      .fillHeterogeneous(Over(_1), Default())(data.summarise(Over(_1), Default())(Mean(false, true, true)))
      .join(Over(_0), Default())(inds)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll4.out")
      .toUnit
  }

  def test14[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.ChangeTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .change(
        Over(_1),
        Default()
      )(
        "fid:A",
        Content.decoder(LongCodec, NominalSchema[Long]()),
        Cell.toShortString(true, "|")
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/chg1.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test15[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.JoinTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .slice(Over(_1), Default())(true, List("fid:A", "fid:C", "fid:E", "fid:G"))
      .slice(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .summarise(Along(_2), Default())(Sums().andThenRelocate(_.position.append("sum").toOption))
      .melt(_2, _1, Value.concatenate[StringValue, StringValue]("."))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/rsh1.out")
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val inds = data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn1.csv")

    data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .join(Over(_0), Default())(inds)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/jn1.csv")
      .toUnit
  }

  def test16[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    case class HashSample[
      P <: HList,
      V <: Value[_]
    ](
    )(implicit
      ev: Position.IndexConstraints[P, _0, V]
    ) extends Sampler[P] {
      def select(cell: Cell[P]): Boolean = (cell.position(_0).toString.hashCode % 25) == 0
    }

    data
      .subset(HashSample())
      .saveAsText(ctx, s"./tmp.${tool}/smp1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test17[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val stats = sliced
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .compact(Over(_0), Default())

    def extractor[
      P <: HList,
      V <: Value[_]
    ](implicit
      ev: Position.IndexConstraints[P, _1, V]
    ) = ExtractWithDimensionAndKey[P, _1, V, StringValue, Content](_1, "max.abs").andThenPresent(_.value.asDouble)

    sliced
      .transformWithValue(stats, Normalise(extractor))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn6.csv")
      .toUnit

    case class Sample500[P <: HList]() extends Sampler[P] {
      def select(cell: Cell[P]): Boolean = cell.content.value gtr 500
    }

    sliced
      .subset(Sample500())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt1.csv")
      .toUnit

    case class RemoveGreaterThanMean[
      P <: HList,
      D <: Nat,
      K <: Value[_]
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints[P, D, K]
    ) extends SamplerWithValue[P] {
      type V = Map[Position[K :: HNil], Map[Position[StringValue :: HNil], Content]]

      def selectWithValue(cell: Cell[P], ext: V): Boolean =
        if (cell.content.schema.classification.isOfType(NumericType))
          cell.content.value leq ext(Position(cell.position(dim)))(Position("mean")).value
        else
          true
    }

    sliced
      .subsetWithValue(stats, RemoveGreaterThanMean(_1))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt2.csv")
      .toUnit
  }

  def test18[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val stats = sliced
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )

    val rem = stats
      .whichByPosition(
        Over(_1),
        Default()
      )(
        ("count", (c: Cell[StringValue :: StringValue :: HNil]) => c.content.value leq 2)
      )
      .names(Over(_0), Default())

    sliced
      .slice(Over(_1), Default())(false, rem)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt3.csv")
      .toUnit
  }

  def test19[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[C#U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev3: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SliceTuner[C#U, Default[NoParameters]],
    ev6: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev7: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    implicit val c = ctx

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val raw = data
      .slice(Over(_0), Default())(true, ids)
      .slice(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    case class CustomPartition[
      P <: HList,
      D <: Nat,
      V <: Value[_]
    ](
      dim: D,
      left: String,
      right: String
    )(implicit
      ev: Position.IndexConstraints[P, D, V]
    ) extends Partitioner[P, String] {
      val bhs = BinaryHashSplit(dim, 7, left, right, base = 10)

      def assign(cell: Cell[P]): TraversableOnce[String] =
        if (cell.position(dim).toShortString == "iid:0364354")
          List(right)
        else
          bhs.assign(cell)
    }

    val parts = raw
      .split(CustomPartition(_0, "train", "test"))

    val stats = parts
      .get("train")
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )

    val rem = stats
      .which(c => (c.position(_1) equ "count") && (c.content.value leq 2))
      .names(Over(_0), Default())

    def extractor[
      P <: HList,
      V <: Value[_]
    ](implicit
      ev: Position.IndexConstraints[P, _1, V]
    ) = ExtractWithDimensionAndKey[P, _1, V, StringValue, Content](_1, "max.abs").andThenPresent(_.value.asDouble)

    def cb(
      key: String,
      pipe: C#U[Cell[StringValue :: StringValue :: HNil]]
    ): C#U[Cell[StringValue :: StringValue :: HNil]] = pipe
      .slice(Over(_1), Default())(false, rem)
      .transformWithValue(
        stats.compact(Over(_0), Default()),
        Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
        Binarise(Locate.RenameDimensionWithContent(_1)),
        Normalise(extractor)
      )
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/pln_" + key + ".csv")

    parts
      .forEach(List("train", "test"), cb)
      .toUnit
  }

  def test20[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (dictionary, _) = Dictionary.load(Source.fromFile(path + "/dict.txt"))

    ctx
      .loadText(
        path + "/ivoryInputfile1.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, dictionary, _1, "|")
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/ivr1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test21[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[StringValue :: StringValue :: DateValue :: HNil]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.ShapeTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SizeTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    data
      .shape(Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz0.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .size(_0, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .size(_1, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .size(_2, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz3.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test22[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx
      .loadText(path + "/numericInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    case class Diff[
      P <: HList,
      S <: HList,
      R <: HList,
      Q <: HList
    ](
    )(implicit
      ev1: Position.ListConstraints[R],
      ev2: Position.AppendConstraints[S, StringValue, Q]
    ) extends Window[P, S, R, Q] {
      type I = Option[Double]
      type T = (Option[Double], Position[R])
      type O = (Double, Position[R], Position[R])

      def prepare(cell: Cell[P]): I = cell.content.value.asDouble

      def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

      def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
        case (Some(c), Some(l)) => List((c - l, rem,  t._2))
        case _ => List()
      })

      def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = List(
        Cell(
          pos.append(out._2.toShortString("") + "-" + out._3.toShortString("")),
          Content(ContinuousSchema[Double](), out._1)
        )
      )
    }

    data
      .slide(Over(_0), Default())(true, Diff())
      .saveAsText(ctx, s"./tmp.${tool}/dif1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .slide(Over(_1), Default())(true, Diff())
      .permute(_1, _0)
      .saveAsText(ctx, s"./tmp.${tool}/dif2.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test23[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairwiseTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx
      .loadText(path + "/somePairwise.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    case class DiffSquared[
      P <: HList,
      V0 <: Value[_],
      V1 <: Value[_],
      X <: HList,
      Q <: HList
    ](
    )(implicit
      ev1: Position.IndexConstraints[P, _0, V0],
      ev2: Position.IndexConstraints[P, _1, V1],
      ev3: Position.RemoveConstraints[P, _1, X],
      ev4: Position.AppendConstraints[X, StringValue, Q]
    ) extends Operator[P, Q] {
      def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
        val xc = left.position(_1).toShortString
        val yc = right.position(_1).toShortString

        if (left.position(_0) == right.position(_0))
          List(
            Cell(
              right.position.remove(_1).append("(" + xc + "-" + yc + ")^2"),
              Content(
                ContinuousSchema[Double](),
                math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2)
              )
            )
          )
        else
          List()
      }
    }

    data
      .pairwise(Over(_1), Default())(Upper, DiffSquared())
      .saveAsText(ctx, s"./tmp.${tool}/pws1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test24[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.CorrelationTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.mathsisfun.com/data/correlation.html for data

    val pkey = KeyDecoder(_0, StringCodec) // "day"
    val columns = Set(
      ColumnDecoder(_1, "temperature", Content.decoder(DoubleCodec, ContinuousSchema[Double]())),
      ColumnDecoder(_2, "sales", Content.decoder(LongCodec, DiscreteSchema[Long]()))
    )

    val (data, _) = ctx.loadText(path + "/somePairwise2.txt", Cell.tableParser(pkey, columns, "|"))

    def locate[P <: Nat](implicit ev: Position.ListConstraints[P])  = (l: Position[P], r: Position[P]) => Option(
      Position(s"(${l.toShortString("|")}*${r.toShortString("|")})")
    )

    data
      .correlation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/pws2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    val columns2 = columns + ColumnDecoder(_3, "neg.sales", Content.decoder(LongCodec, DiscreteSchema[Long]()))

    val (data2, _) = ctx.loadText(path + "/somePairwise3.txt", Cell.tableParser(pkey, columns2, "|"))

    data2
      .correlation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/pws3.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test25[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.MutualInformationTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.eecs.harvard.edu/cs286r/courses/fall10/papers/Chapter2.pdf example 2.2.1 for data

    def locate[P <: HList](implicit ev: Position.ListConstraints[P]) = (l: Position[P], r: Position[P]) => Option(
      Position(s"${r.toShortString("|")},${l.toShortString("|")}")
    )

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .mutualInformation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/mi.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .mutualInformation(Along(_0), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/im.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test26[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairwiseTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (left, _) = ctx
      .loadText(path + "/algebraInputfile1.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
    val (right, _) = ctx
      .loadText(path + "/algebraInputfile2.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    left
      .pairwiseBetween(Over(_0), Default())(
        All,
        right,
        Times(Locate.PrependPairwiseSelectedStringToRemainder(Over(_0), "(%1$s*%2$s)"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/alg.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test27[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // http://www.statisticshowto.com/moving-average/

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/sma1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_0), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/sma2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, CenteredMovingAverage(2, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/tma.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/wma1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_0), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/wma2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers

    ctx
      .loadText(path + "/cumMovAvgInputfile.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
      .data
      .slide(Along(_0), Default())(true, CumulativeMovingAverage(Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/cma.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php

    ctx
      .loadText(path + "/expMovAvgInputfile.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
      .data
      .slide(Along(_0), Default())(true, ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/ema.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test28[
    C <: Context[C]
  ](
    ctx: C,
    rules: CutRules[C#E],
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List
      .range(0, 16)
      .flatMap { case i =>
        List(
          ("iid:" + i, "fid:A", Content(ContinuousSchema[Long](), i)),
          ("iid:" + i, "fid:B", Content(NominalSchema[String](), i.toString))
        )
      }

    val stats = data
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
        Skewness().andThenRelocate(_.position.append("skewness").toOption)
      )
      .compact(Over(_0), Default())

    def extractor(implicit
      ev1: Position.IndexConstraints[StringValue :: StringValue :: HNil, _1, StringValue],
      ev2: Position.ListConstraints[StringValue :: StringValue :: HNil]
    ) = ExtractWithDimension[StringValue :: StringValue :: HNil, _1, StringValue, List[Double]](_1)

    data
      .transformWithValue(rules.fixed(stats, "min", "max", 4), Cut(extractor))
      .saveAsText(ctx, s"./tmp.${tool}/cut1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.squareRootChoice(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.square"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.sturgesFormula(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.sturges"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut3.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.riceRule(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.rice"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut4.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.doanesFormula(stats, "count", "min", "max", "skewness"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.doane"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut5.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.scott"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut6.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        rules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, "%s.break"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut7.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test29[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    val schema = DiscreteSchema[Long]()
    val data = List(
      ("iid:A", Content(schema, 0)),
      ("iid:B", Content(schema, 1)),
      ("iid:C", Content(schema, 2)),
      ("iid:D", Content(schema, 3)),
      ("iid:E", Content(schema, 4)),
      ("iid:F", Content(schema, 5)),
      ("iid:G", Content(schema, 6)),
      ("iid:H", Content(schema, 7))
    )

    data
      .stream(
        "Rscript double.R",
        List("double.R"),
        Cell.toShortString(true, "|"),
        Cell.shortStringParser(StringCodec :: LongCodec :: HNil, "#"),
        Reducers(1)
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/strm.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test30[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    val (data, errors) = ctx
      .loadText(
        path + "/badInputfile.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, "|")
      )

    data
      .saveAsText(ctx, s"./tmp.${tool}/yok.out", (c) => List(c.toString), Default())
      .toUnit

    errors
      .saveAsText(ctx, s"./tmp.${tool}/nok.out", Default())
      .toUnit
  }

  def test31[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Matrix.SaveAsIVTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    List(
      ("a", Content(ContinuousSchema[Double](), 3.14)),
      ("b", Content(DiscreteSchema[Long](), 42)),
      ("c", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv1.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv2.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv3.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv4.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv5.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv6.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv7.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv8.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", "i", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", "j", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", "k", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv9.out", tuner = Default())
      .toUnit
  }

  def test32[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(DiscreteSchema[Long](), 123)),
      ("b", "three", Content(ContinuousSchema[Double](), 9.42)),
      ("c", "two", Content(NominalSchema[String](), "bar")),
      ("c", "three", Content(ContinuousSchema[Double](), 12.56))
    )

    val labels = List(
      Cell(Position("a"), Content(DiscreteSchema[Long](), 1)),
      Cell(Position("b"), Content(DiscreteSchema[Long](), 2))
    )

    val importance = List(
      Cell(Position("a"), Content(ContinuousSchema[Double](), 0.5)),
      Cell(Position("b"), Content(ContinuousSchema[Double](), 0.75))
    )

    data
      .saveAsVW(Over(_0), Default())(ctx, s"./tmp.${tool}/vw0.out", tag = false)
      .toUnit

    data
      .saveAsVW(Over(_0), Default())(ctx, s"./tmp.${tool}/vw1.out", tag = true)
      .toUnit

    data
      .saveAsVWWithLabels(Over(_0), Default())(ctx, s"./tmp.${tool}/vw2.out", labels, tag = false)
      .toUnit

    data
      .saveAsVWWithImportance(Over(_0), Default())(ctx, s"./tmp.${tool}/vw3.out", importance, tag = true)
      .toUnit

    data
      .saveAsVWWithLabelsAndImportance(
        Over(_0),
        Default()
      )(
        ctx,
        s"./tmp.${tool}/vw4.out",
        labels,
        importance,
        tag = false
      )
      .toUnit
  }

  def test33[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Redistribute]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(NominalSchema[String](), "bar")),
      ("c", "one", Content(ContinuousSchema[Double](), 12.56)),
      ("c", "three", Content(DiscreteSchema[Long](), 123))
    )

    def writer[P <: HList](values: List[Option[Cell[P]]]) = List(
      values.map(_.map(_.content.value.toShortString).getOrElse("")).mkString("|")
    )

    val (_, errors) = data
      .streamByPosition(
        Over(_0)
      )(
        "sh ./parrot.sh",
        List("parrot.sh"),
        writer,
        Cell.shortStringParser(StringCodec :: HNil, "|"),
        5
      )

    errors
      .saveAsText(ctx, s"./tmp.${tool}/sbp.out", Redistribute(1))
      .toUnit
  }
}

