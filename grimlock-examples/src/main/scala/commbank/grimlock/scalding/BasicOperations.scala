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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

class BasicOperations(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
  val (data, _) = ctx.loadText(
    s"${path}/exampleInput.txt",
    Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|") _
  )

  // Get the number of rows.
  data
    .size(_0)
    .saveAsText(ctx, s"./demo.${output}/row_size.out", Cell.toShortString(true, "|") _)
    .toUnit

  // Get all dimensions of the matrix.
  data
    .shape()
    .saveAsText(ctx, s"./demo.${output}/matrix_shape.out", Cell.toShortString(true, "|") _)
    .toUnit

  // Get the column names.
  data
    .names(Over(_1))
    .saveAsText(ctx, s"./demo.${output}/column_names.out", Position.toShortString("|") _)
    .toUnit

  // Get the type of variables of each column.
  data
    .types(Over(_1))(true)
    .saveAsText(ctx, s"./demo.${output}/column_types.txt", Cell.toShortString(true, "|") _)
    .toUnit

  // Transpose the matrix.
  data
    .permute(_1, _0)
    .saveAsText(ctx, s"./demo.${output}/transposed.out", Cell.toShortString(true, "|") _)
    .toUnit

  // Construct a simple query
  def simpleQuery[P <: HList](cell: Cell[P]) = (cell.content.value gtr 995) || (cell.content.value equ "F")

  // Find all co-ordinates that match the above simple query.
  val coords = data
    .which(simpleQuery)
    .saveAsText(ctx, s"./demo.${output}/query.txt", Position.toShortString("|") _)

  // Get the data for the above coordinates.
  data
    .get(coords)
    .saveAsText(ctx, s"./demo.${output}/values.txt", Cell.toShortString(true, "|") _)
    .toUnit

  // Keep columns A and B, and remove row 0221707
  data
    .slice(Over(_1))(true, List("fid:A", "fid:B"))
    .slice(Over(_0))(false, "iid:0221707")
    .saveAsText(ctx, s"./demo.${output}/sliced.txt", Cell.toShortString(true, "|") _)
    .toUnit
}

