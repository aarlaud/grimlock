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

package commbank.grimlock.framework.utility

/** Trait for ecaping special characters in a string. */
trait Escape {
  /** The special character to escape. */
  val special: String

  /**
   * Escape a string.
   *
   * @param str The string to escape.
    * @return The escaped string.
   */
  def escape(str: String): String
}

/**
 * Escape a string by enclosing it in quotes.
 *
 * @param special The special character to quote.
 * @param quote   The quoting character to use.
 * @param all     Indicator if all strings should be quoted.
 */
case class Quote(special: String, quote: String = "\"", all: Boolean = false) extends Escape {
  def escape(str: String): String = if (all || str.contains(special)) quote + str + quote else str
}

/**
 * Escape a string by replacing the special character.
 *
 * @param special The special character to replace.
 * @param pattern The escape pattern to use. Use `%1$``s` to substitute the special character.
 */
case class Replace(special: String, pattern: String = "\\%1$s") extends Escape {
  def escape(str: String): String = str.replaceAllLiterally(special, pattern.format(special))
}

