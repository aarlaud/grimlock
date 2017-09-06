// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.encoding.{ Codec, EncodeString, StringCodec, Value }
import commbank.grimlock.framework.position.Position

import com.twitter.scrooge.ThriftStruct

import java.util.regex.Pattern

import org.apache.hadoop.io.Writable

import play.api.libs.json.{ JsError, JsObject, Json, JsResult, JsSuccess, JsValue, Reads, Writes }

import scala.util.Try

import shapeless.{ ::, HList, LUBConstraint, Nat, Sized }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.hlist.{ Mapper, ToTraversable, Zip }
import shapeless.ops.nat.{ LTEq, ToInt }
import shapeless.syntax.sized._

/**
 * Cell in a matrix.
 *
 * @param position The position of the cell in the matri.
 * @param content  The contents of the cell.
 */
case class Cell[P <: HList](position: Position[P], content: Content[_]) {
  /**
   * Relocate this cell.
   *
   * @param relocator Function that returns the new position for this cell.
   */
  def relocate[X <: HList](relocator: (Cell[P]) => Position[X]): Cell[X] = Cell(relocator(this), content)

  /**
   * Mutate the content of this cell.
   *
   * @param mutator Function that returns the new content for this cell.
   */
  def mutate[D](mutator: (Cell[P]) => Content[D]): Cell[P] = Cell(position, mutator(this))

  /**
   * Return string representation of a cell.
   *
   * @param codecs      The codecs used to encode the coordinates.
   * @param separator   The separator to use between various fields.
   * @param descriptive Indicator if codec and schema are required or not.
   */
  def toShortString[
    L <: HList,
    Z <: HList,
    M <: HList
  ](
    codecs: L,
    separator: String,
    descriptive: Boolean = true
  )(implicit
    ev1: LUBConstraint[L, Codec[_]],
    ev2: Zip.Aux[L :: P :: HNil, Z],
    ev3: Mapper.Aux[EncodeString.type, Z, M],
    ev4: ToTraversable.Aux[M, List, Any]
  ): String = position.toShortString(separator) + separator + content.toShortString(separator, descriptive)

  /**
   * Converts the cell to a JSON string.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON(pretty: Boolean = false, descriptive: Boolean = true): String = {
    implicit val wrt = Cell.writes[P](descriptive)
    val json = Json.toJson(this)

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }
}

/** Companion object to the Cell class. */
object Cell {
  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate[P <: HList] = Cell[P] => Boolean

  /** Type for parsing a string into either a `Cell[P]` or an error message. */
  type TextParser[P <: HList] = (String) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing a key value tuple into either a `Cell[P]` or an error message. */
  type SequenceParser[K <: Writable, V <: Writable, P <: HList] = (K, V) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing Parquet data. */
  type ParquetParser[T <: ThriftStruct, P <: HList] = (T) => TraversableOnce[Either[String, Cell[P]]]

  /**
   * Parse a line into a `Cell[_1]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1D(
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromParts(separator))

  /**
   * Parse a line into a `Cell[_1]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1DWithDictionary(
    dict: Map[String, Content.Parser],
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromDictionary[_1](dict))

  /**
   * Parse a line into a `Cell[_1]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromSchema(schema))

  /**
   * Parse a line into a `List[Cell[_2]]` with column definitions.
   *
   * @param columns   `List[(String, Content.Parser)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   */
  def parseTable(
    columns: List[(String, Content.Parser)],
    pkeyIndex: Int = 0,
    separator: String = "\u0001"
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = line => {
    val parts = line.trim.split(Pattern.quote(separator), columns.length)

    if (parts.length == columns.length) {
      val pkey = parts(pkeyIndex)

      columns.zipWithIndex.flatMap { case ((name, decoder), idx) =>
        if (idx != pkeyIndex)
          decoder(parts(idx))
            .map(con => Right(Cell(Position(pkey, name), con)))
            .orElse(Option(Left("Unable to decode: '" + line + "'")))
        else
          None
      }
    } else
      List(Left("Unable to split: '" + line + "'"))
  }

  /**
   * Parse JSON into a `Cell[_1]`.
   *
   * @param first The codec for decoding the first dimension.
   */
  def parse1DJSON(
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_1]` with a dictionary.
   *
   * @param dict  The dictionary describing the features in the data.
   * @param first The codec for decoding the first dimension.
   */
  def parse1DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromDictionary[_1](dict))

  /**
   * Parse JSON into a `Cell[_1]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   */
  def parse1DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromSchema(schema))

  /**
   * Return function that returns a string representation of a cell.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields (only used if verbose is `false`).
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString[
    P <: Nat
  ](
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  ): (Cell[P]) => TraversableOnce[String] = (t: Cell[P]) =>
    List(if (verbose) t.toString else t.toShortString(separator, descriptive))

  /**
   * Return function that returns a JSON representation of a cell.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON[P <: Nat ](pretty: Boolean = false, descriptive: Boolean = true): (Cell[P]) => TraversableOnce[String] =
    (t: Cell[P]) => List(t.toJSON(pretty, descriptive))

  /**
   * Return a `Reads` for parsing a JSON cell.
   *
   * @param codecs List of codecs for parsing the position.
   * @param parser Optional parser; in case the JSON content is not self describing.
   */
  def reads[P <: Nat : ToInt](
    codecs: Sized[List[Codec], P],
    parser: (List[Value]) => Option[Content.Parser]
  ): Reads[Cell[P]] = new Reads[Cell[P]] {
    implicit val prd = Position.reads(codecs)

    def reads(json: JsValue): JsResult[Cell[P]] = {
      val fields = json.as[JsObject].value

      if (fields.size == 2)
        (
          for {
            pos <- fields.get("position").map(_.as[Position[P]])
            con <- fields.get("content").map(_.as[Content](Content.reads(parser(pos.coordinates))))
          } yield JsSuccess(Cell(pos, con))
        ).getOrElse(JsError("Unable to parse cell"))
      else
        JsError("Incorrect number of fields")
    }
  }

  /**
   * Return a `Writes` for writing JSON cell.
   *
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def writes[P <: Nat](descriptive: Boolean): Writes[Cell[P]] = new Writes[Cell[P]] {
    implicit val wrt = Content.writes(descriptive)

    def writes(o: Cell[P]): JsValue = Json.obj("position" -> o.position, "content" -> o.content)
  }

  private def parseXD[
    Q <: Nat : ToInt
  ](
    line: String,
    separator: String,
    codecs: Sized[List[Codec], Q],
    parser: CellContentParser
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]

    val (pos, con) = line.trim.split(Pattern.quote(separator), split + parser.textParts).splitAt(split)

    if (pos.size != split || con.size != parser.textParts)
      List(Left("Unable to split: '" + line + "'"))
    else
      parser.getTextParser(pos) match {
        case Some(prs) =>
          val cell = for {
            p <- codecs.zip(pos).flatMap { case (c, p) => c.decode(p) }.sized[Q]
            c <- prs(con)
          } yield Right(Cell(Position(p), c))

          cell.orElse(Option(Left("Unable to decode: '" + line + "'")))
        case _ =>  List(Left("Missing schema for: '" + line + "'"))
      }
  }

  private def parseXDJSON[
    Q <: Nat : ToInt
  ](
    json: String,
    codecs: Sized[List[Codec], Q],
    parser: CellContentParser
  ): TraversableOnce[Either[String, Cell[Q]]] = List(
    Json.fromJson[Cell[Q]](Json.parse(json))(reads(codecs, parser.getJSONParser)) match {
      case JsSuccess(cell, _) => Right(cell)
      case _ => Left(s"Unable to decode: '" + json + "'")
    }
  )
}

private trait CellContentParser {
  val textParts: Int

  def getTextParser(pos: Array[String]): Option[(Array[String]) => Option[Content]]
  def getJSONParser(pos: List[Value]): Option[Content.Parser]
}

private case class ParserFromParts(separator: String = "") extends CellContentParser {
  val textParts = 3

  def getTextParser(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(c, s, v) => Content.fromComponents(c, s, v)
      case _ => None
    }
  )
  def getJSONParser(pos: List[Value]) = None
}

private case class ParserFromSchema(schema: Content.Parser) extends CellContentParser {
  val textParts = 1

  def getTextParser(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(v) => schema(v)
      case _ => None
    }
  )
  def getJSONParser(pos: List[Value]) = Option(schema)
}

private case class ParserFromDictionary[D <: Nat : ToInt](
  dict: Map[String, Content.Parser]
) extends CellContentParser {
  val textParts = 1

  def getTextParser(pos: Array[String]) = {
    val parser = for {
      key <- Try(pos(if (idx == 0) pos.length - 1 else idx - 1)).toOption
      prs <- dict.get(key)
    } yield prs

    parser.map(dec =>
      (con: Array[String]) => con match {
        case Array(v) => dec(v)
        case _ => None
      }
    )
  }
  def getJSONParser(pos: List[Value]) = for {
    key <- Try(pos(if (idx == 0) pos.length - 1 else idx - 1)).toOption
    parser <- dict.get(key.toShortString)
  } yield parser

  private val idx = Nat.toInt[D]
}

