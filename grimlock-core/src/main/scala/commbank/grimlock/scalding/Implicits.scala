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

package commbank.grimlock.scalding.environment.implicits

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.implicits.{
  CellImplicits => FwCellImplicits,
  ContentImplicits => FwContentImplicits,
  EnvironmentImplicits => FwEnvironmentImplicits,
  Implicits => FwImplicits,
  MatrixImplicits => FwMatrixImplicits,
  NativeOperations => FwNativeOperations,
  PartitionImplicits => FwPartitionImplicits,
  PositionImplicits => FwPositionImplicits,
  ValueOperations => FwValueOperations
}
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.{
  Matrix,
  Matrix1D,
  Matrix2D,
  Matrix3D,
  Matrix4D,
  Matrix5D,
  Matrix6D,
  Matrix7D,
  Matrix8D,
  Matrix9D,
  MultiDimensionMatrix,
  SaveStringsAsText
}
import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import com.twitter.scalding.typed.IterablePipe

import scala.reflect.ClassTag

import shapeless.{ =:!=, Nat }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.GT

/** Implements all implicits. */
case class Implicits() extends FwImplicits[Context] {
  val cell = CellImplicits()
  val content = ContentImplicits()
  val environment = EnvironmentImplicits()
  val matrix = MatrixImplicits()
  val partition = PartitionImplicits()
  val position = PositionImplicits()
}

/** Implements all cell implicits. */
case class CellImplicits() extends FwCellImplicits[Context] {
  implicit def cellToU[P <: Nat](c: Cell[P])(implicit ctx: Context): Context.U[Cell[P]] = IterablePipe(List(c))

  implicit def listCellToU[P <: Nat](l: List[Cell[P]])(implicit ctx: Context): Context.U[Cell[P]] = IterablePipe(l)
}

/** Implements all content implicits. */
case class ContentImplicits() extends FwContentImplicits[Context] {
  implicit def toContents(data: Context.U[Content[_]]): Contents = Contents(data)

  implicit def toIndexed[
    P <: Nat
  ](
    data: Context.U[(Position[P], Content[_])]
  ): IndexedContents[P] = IndexedContents(data)
}

/** Implements all environment implicits for `context`. */
case class EnvironmentImplicits() extends FwEnvironmentImplicits[Context]  {
  implicit def saveStringsAsText(data: Context.U[String]): SaveStringsAsText = SaveStringsAsText(data)

  implicit def nativeFunctions[X](data: Context.U[X]): NativeOperations[X] = NativeOperations(data)

  implicit def valueFunctions[X](value: Context.E[X]): ValueOperations[X] = ValueOperations(value)

  implicit def eToU[X : ClassTag](value: Context.E[X])(implicit ctx: Context): Context.U[X] = value.toTypedPipe
}

/** Implements all matrix implicits. */
case class MatrixImplicits() extends FwMatrixImplicits[Context] {
  implicit def toMatrix[P <: Nat](data: Context.U[Cell[P]]): Matrix[P] = Matrix(data)

  implicit def toMatrix1D(data: Context.U[Cell[_1]]): Matrix1D = Matrix1D(data)

  implicit def toMatrix2D(data: Context.U[Cell[_2]]): Matrix2D = Matrix2D(data)

  implicit def toMatrix3D(data: Context.U[Cell[_3]]): Matrix3D = Matrix3D(data)

  implicit def toMatrix4D(data: Context.U[Cell[_4]]): Matrix4D = Matrix4D(data)

  implicit def toMatrix5D(data: Context.U[Cell[_5]]): Matrix5D = Matrix5D(data)

  implicit def toMatrix6D(data: Context.U[Cell[_6]]): Matrix6D = Matrix6D(data)

  implicit def toMatrix7D(data: Context.U[Cell[_7]]): Matrix7D = Matrix7D(data)

  implicit def toMatrix8D(data: Context.U[Cell[_8]]): Matrix8D = Matrix8D(data)

  implicit def toMatrix9D(data: Context.U[Cell[_9]]): Matrix9D = Matrix9D(data)

  implicit def toMultiDimensionMatrix[
    P <: Nat
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(data)

  implicit def listToMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = Matrix(IterablePipe(data))

  implicit def listToMatrix1D(data: List[Cell[_1]])(implicit ctx: Context): Matrix1D = Matrix1D(IterablePipe(data))

  implicit def listToMatrix2D(data: List[Cell[_2]])(implicit ctx: Context): Matrix2D = Matrix2D(IterablePipe(data))

  implicit def listToMatrix3D(data: List[Cell[_3]])(implicit ctx: Context): Matrix3D = Matrix3D(IterablePipe(data))

  implicit def listToMatrix4D(data: List[Cell[_4]])(implicit ctx: Context): Matrix4D = Matrix4D(IterablePipe(data))

  implicit def listToMatrix5D(data: List[Cell[_5]])(implicit ctx: Context): Matrix5D = Matrix5D(IterablePipe(data))

  implicit def listToMatrix6D(data: List[Cell[_6]])(implicit ctx: Context): Matrix6D = Matrix6D(IterablePipe(data))

  implicit def listToMatrix7D(data: List[Cell[_7]])(implicit ctx: Context): Matrix7D = Matrix7D(IterablePipe(data))

  implicit def listToMatrix8D(data: List[Cell[_8]])(implicit ctx: Context): Matrix8D = Matrix8D(IterablePipe(data))

  implicit def listToMatrix9D(data: List[Cell[_9]])(implicit ctx: Context): Matrix9D = Matrix9D(IterablePipe(data))

  implicit def listToMultiDimensionMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context,
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(IterablePipe(data))

  implicit def tuple1ToMatrix[
    V1
  ](
    list: List[(V1, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_1] = Matrix(IterablePipe(list.map { case (v, c) => Cell(Position(v), c) }))

  implicit def tuple1ToMatrix1D[
    V1
  ](
    list: List[(V1, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix1D = Matrix1D(IterablePipe(list.map { case (v, c) => Cell(Position(v), c) }))

  implicit def tuple2ToMatrix[
    V1,
    V2
  ](
    list: List[(V1, V2, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_2] = Matrix(IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) }))

  implicit def tuple2ToMatrix2D[
    V1,
    V2
  ](
    list: List[(V1, V2, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix2D = Matrix2D(IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) }))

  implicit def tuple2ToMultiDimensionMatrix[
    V1,
    V2
  ](
    list: List[(V1, V2, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_2] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, c) => Cell(Position(v1, v2), c) })
  )

  implicit def tuple3ToMatrix[
    V1,
    V2,
    V3
  ](
    list: List[(V1, V2, V3, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_3] = Matrix(IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) }))

  implicit def tuple3ToMatrix3D[
    V1,
    V2,
    V3
  ](
    list: List[(V1, V2, V3, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix3D = Matrix3D(IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) }))

  implicit def tuple3ToMultiDimensionMatrix[
    V1,
    V2,
    V3
  ](
    list: List[(V1, V2, V3, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_3] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, c) => Cell(Position(v1, v2, v3), c) })
  )

  implicit def tuple4ToMatrix[
    V1,
    V2,
    V3,
    V4
  ](
    list: List[(V1, V2, V3, V4, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_4] = Matrix(IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) }))

  implicit def tuple4ToMatrix4D[
    V1,
    V2,
    V3,
    V4
  ](
    list: List[(V1, V2, V3, V4, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix4D = Matrix4D(IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) }))

  implicit def tuple4ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4
  ](
    list: List[(V1, V2, V3, V4, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_4] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, c) => Cell(Position(v1, v2, v3, v4), c) })
  )

  implicit def tuple5ToMatrix[
    V1,
    V2,
    V3,
    V4,
    V5
  ](
    list: List[(V1, V2, V3, V4, V5, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_5] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple5ToMatrix5D[
    V1,
    V2,
    V3,
    V4,
    V5
  ](
    list: List[(V1, V2, V3, V4, V5, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix5D = Matrix5D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple5ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4,
    V5
  ](
    list: List[(V1, V2, V3, V4, V5, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_5] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, c) => Cell(Position(v1, v2, v3, v4, v5), c) })
  )

  implicit def tuple6ToMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_6] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple6ToMatrix6D[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix6D = Matrix6D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple6ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_6] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, c) => Cell(Position(v1, v2, v3, v4, v5, v6), c) })
  )

  implicit def tuple7ToMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_7] = Matrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple7ToMatrix7D[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix7D = Matrix7D(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple7ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_7] = MultiDimensionMatrix(
    IterablePipe(list.map { case (v1, v2, v3, v4, v5, v6, v7, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7), c) })
  )

  implicit def tuple8ToMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_8] = Matrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple8ToMatrix8D[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix8D = Matrix8D(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple8ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_8] = MultiDimensionMatrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8), c) }
    )
  )

  implicit def tuple9ToMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8,
    V9
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix[_9] = Matrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )

  implicit def tuple9ToMatrix9D[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8,
    V9
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content[_])]
  )(implicit
    ctx: Context
  ): Matrix9D = Matrix9D(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )

  implicit def tuple9ToMultiDimensionMatrix[
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
    V7,
    V8,
    V9
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content[_])]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_9] = MultiDimensionMatrix(
    IterablePipe(
      list.map { case (v1, v2, v3, v4, v5, v6, v7, v8, v9, c) => Cell(Position(v1, v2, v3, v4, v5, v6, v7, v8, v9), c) }
    )
  )
}

/** Implements all partition implicits. */
case class PartitionImplicits() extends FwPartitionImplicits[Context] {
  implicit def toPartitions[
    P <: Nat,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  ): Partitions[P, I] = Partitions(data)
}

/** Implements all position implicits. */
case class PositionImplicits() extends FwPositionImplicits[Context] {
  implicit def valueToU[
    V
  ](
    v: V
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = IterablePipe(List(Position(v)))

  implicit def listValueToU[
    V
  ](
    l: List[V]
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = IterablePipe(l.map { case v => Position(v) })

  implicit def positionToU[
    P <: Nat
  ](
    p: Position[P]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = IterablePipe(List(p))

  implicit def listPositionToU[
    P <: Nat
  ](
    l: List[Position[P]]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = IterablePipe(l)

  implicit def toPositions[P <: Nat](data: Context.U[Position[P]]): Positions[P] = Positions(data)
}

/** Implements all native operations. */
case class NativeOperations[X](data: Context.U[X]) extends FwNativeOperations[X, Context.U] {
  def ++(other: Context.U[X]): Context.U[X] = data ++ other

  def filter(f: (X) => Boolean): Context.U[X] = data.filter(f)

  def flatMap[Y : ClassTag](f: (X) => TraversableOnce[Y]): Context.U[Y] = data.flatMap(f)

  def map[Y : ClassTag](f: (X) => Y): Context.U[Y] = data.map(f)
}

/** Implements all value operations. */
case class ValueOperations[X](value: Context.E[X]) extends FwValueOperations[X, Context.E] {
  def cross[Y](that: Context.E[Y])(implicit ev: Y =:!= Nothing): Context.E[(X, Y)] = value
    .leftCross(that)
    .map {
      case (x, Some(y)) => (x, y)
      case (x, None) => throw new Exception("Empty ValuePipe not supported")
    }

  def map[Y](f: (X) => Y): Context.E[Y] = value.map(f)
}

