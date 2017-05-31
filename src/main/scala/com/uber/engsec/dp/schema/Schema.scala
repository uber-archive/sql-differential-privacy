/*
 * Copyright (c) 2017 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.engsec.dp.schema

import java.io.{BufferedReader, FileInputStream, IOException, InputStreamReader}
import java.util

import com.fasterxml.jackson.annotation.{JsonAnySetter, JsonProperty}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.mutable

/** Utilties for handling schema representations and metadata parsed from yaml config file.
  */
object Schema {

  val TABLE_YAML_FILE_PATH: String = System.getProperty("schema.config.path", "schema.yaml")

  private var databases: Databases = null
  private var printWarnings: Boolean = true
  var tables: mutable.Map[String, Table] = new mutable.HashMap[String, Table]()

  import scala.collection.JavaConverters._
  def tablesAsJava() = tables.asJava

  { parseYaml() }

  /** Don't print any warning messages about unknown schema.
    */
  def suppressWarnings() {
    printWarnings = false
  }

  def parseYaml() {
    clearTables()
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory)
    mapper.registerModule(DefaultScalaModule)

    try {
      // First try to load from the JAR file
      var in = this.getClass().getResourceAsStream("/" + TABLE_YAML_FILE_PATH)
      if (in == null) {
        // If this fails, we may not be running from the JAR. Try to load the resource from disk.
        in = new FileInputStream(TABLE_YAML_FILE_PATH)
      }
      val reader = new BufferedReader(new InputStreamReader(in))
      setDatabases(mapper.readValue(reader, classOf[Databases]))
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
        System.err.println("Error reading schema.yaml file. Exiting.")
        System.exit(-1)
      }
    }
  }

  /** Sets Schema databases
    *
    * @param  tempDatabases instance of Databases class
    */
  def setDatabases(tempDatabases: Databases) {
    databases = tempDatabases
    tables.clear()
    for (database <- databases.databases) {
      for (table <- database.tables) {
        tables.put(table.name, table)
      }
    }
  }

  /** Sets Schema databases and tables
    *
    * @param  tempDatabases instance of Databases class
    * @param  tempTables    Map<String,Table>
    */
  def setDatabasesAndTables(tempDatabases: Databases, tempTables: mutable.Map[String, Table]) {
    databases = tempDatabases
    tables.clear()
    tables = tempTables
  }

  def clearTables() {
    tables.clear()
    databases = Databases(Nil)
  }

  // Keep track of which tables we've already printed a warning about.
  var unknownTables: mutable.Set[String] = new mutable.HashSet[String]()

  def _normalizeTableName(tableName: String): String = {
    val dbPrefix = currentDb.namespace + "."

    // Strip namespace prefix if present
    if (tableName.startsWith(dbPrefix))
      tableName.substring(dbPrefix.length)
    else
      tableName
  }

  // This method returns the empty list if the tables is not known.
  def getSchemaForTable(tableName: String): List[Column] = {
    val normalizedTableName = _normalizeTableName(tableName)

    if (!tables.contains(normalizedTableName)) {
      if (printWarnings && !unknownTables.contains(normalizedTableName)) {
        System.err.println("Warning: unknown schema for table '" + normalizedTableName + "' (this message will only be printed once)")
        unknownTables.add(normalizedTableName)
      }
      return Nil
    }

    tables(normalizedTableName).columns
  }

  def currentDb: Database = {
    // If schema contains more than one database, change this code to allow disambiguation!
    if (databases.databases.size != 1)
      throw new IllegalArgumentException("More than one database specified in schema config, and currently no way to specify current db.")
    databases.databases.head
  }

  def getSchemaMapForTable(tableName: String): Map[String, Column] = {
    val normalizedTableName = _normalizeTableName(tableName)
    tables.get(normalizedTableName).map{ _.columnMap }.getOrElse(Map.empty)
  }

  def getTableProperties(tableName: String): Map[String, String] = {
    val normalizedTableName = _normalizeTableName(tableName)
    tables.get(normalizedTableName).map { _.properties }.getOrElse(Map.empty)
  }
}

/** Represents a schema component with attached properties. Examples include the PII or private status of a column,
  * and column statistics computed from the database (e.g. maximum frequencies of join keys).
  */
abstract trait SchemaConfigWithProperties {
  var properties: Map[String,String] = Map.empty
  @JsonAnySetter def set(name: String, value: String): Unit = {
    properties = properties + (name -> value)
  }
}

case class Column(name: String) extends SchemaConfigWithProperties {}

case class Database(database: String, dialect: String, namespace: String, tables: List[Table])

case class Databases(databases: List[Database])

case class Table(@JsonProperty("table") name: String, columns: List[Column]) extends SchemaConfigWithProperties {
  val columnMap: Map[String, Column] = columns.map{ col => col.name -> col }.toMap
}

case class Tables(tables: List[Table])

class CalciteTable(table: Table) extends AbstractTable {
  import scala.collection.JavaConverters._
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val colNames = table.columns.map { _.name }
    val colTypes = List.fill(table.columns.size)(typeFactory.createSqlType(SqlTypeName.ANY))
    typeFactory.createStructType(colTypes.asJava, colNames.asJava)
  }
}

class CalciteSchemaFromConfig extends AbstractSchema {
  import scala.collection.JavaConverters._
  override def getTableMap: util.Map[String, schema.Table] = Schema.tables.mapValues { table =>
    new CalciteTable(table).asInstanceOf[schema.Table]
  }.asJava
}