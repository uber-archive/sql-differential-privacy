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
import com.uber.engsec.dp.schema.Schema.stripNamespacePrefix
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory, StructKind}
import org.apache.calcite.schema.ScannableTable
import org.apache.calcite.schema.impl.{AbstractSchema, AbstractTable}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.{DataContext, schema}

import scala.collection.mutable

/** Utilties for handling schema representations and metadata parsed from yaml config file.
  */
object Schema {
  // To read schema information automatically from yaml file, set this flag
  val TABLE_YAML_FILE_PATH: String = System.getProperty("schema.config.path", "schema.yaml")

  // Set this flag to suppress warning messages about every unknown table.
  val SUPPRESS_WARNINGS = System.getProperty("schema.suppress.warnings", "false").toBoolean

  // Keep track of which tables we've already printed a warning about.
  var unknownTables: mutable.Set[String] = new mutable.HashSet[String]()

  // Cache of databases allowing easy lookup by name. Tables are stored in lookup table inside Database class.
  var databases: mutable.Map[String, Database] = new mutable.HashMap[String, Database]

  if (TABLE_YAML_FILE_PATH.nonEmpty) loadYamlFiles(TABLE_YAML_FILE_PATH.split(','))

  def loadYamlFiles(fileNames: Array[String]) = {
    // First try to load from the JAR file
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory)
    mapper.registerModule(DefaultScalaModule)
    fileNames.foreach { yamlPath =>
      try {
        var in = this.getClass().getResourceAsStream("/" + yamlPath)
        if (in == null) {
          // If this fails, we may not be running from the JAR. Try to load the resource from disk.
          in = new FileInputStream(yamlPath)
        }
        val reader = new BufferedReader(new InputStreamReader(in))

        // Read schema information for databases from yaml file and add to internal schema.
        val dbs = mapper.readValue(reader, classOf[Databases])
        addDatabases(dbs)
      } catch {
        case e: IOException => {
          e.printStackTrace()
          System.err.println(s"Error reading schema file '$yamlPath'. Exiting.")
          System.exit(-1)
        }
      }
    }
  }

  /**
    * Add the schemas for the given set of databases.
    */
  def addDatabases(dbs: Databases) = dbs.databases.foreach { db => databases.put(db.database, db) }

  def clearDatabases() = databases.clear

  def stripNamespacePrefix(database: Database, tableName: String): String = tableName.stripPrefix(s"${database.namespace}.")

  /**
    * Returns a reference to the database with the given name.
    */
  def getDatabase(database: String) = databases.getOrElse(database, throw new IllegalArgumentException(s"Unknown database '${database}'. Did you define this database in the schema?"))

  def getTableMapForDatabase(database: Database): Map[String, Table] = database.tableMap

  /**
    * Returns the list of columns for the given database tables, or Nil if the table is not found in the schema.
    */
  def getSchemaForTable(database: Database, tableName: String): List[Column] = {
    val tables = getTableMapForDatabase(database)
    val normalizedTableName = stripNamespacePrefix(database, tableName)

    if (!tables.contains(normalizedTableName)) {
      val fullyQualifiedName = s"$database.$normalizedTableName"
      if (!SUPPRESS_WARNINGS && !unknownTables.contains(fullyQualifiedName)) {
        System.err.println("Warning: unknown schema for table '" + normalizedTableName + s"' in database '${database}' (this message will only be printed once)")
        unknownTables.add(fullyQualifiedName)
      }
      return Nil
    }

    tables(normalizedTableName).columns
  }

  def getSchemaMapForTable(database: Database, tableName: String): Map[String, Column] = {
    val tables = getTableMapForDatabase(database)
    val normalizedTableName = stripNamespacePrefix(database, tableName)
    tables.get(normalizedTableName).map{ _.columnMap }.getOrElse(Map.empty)
  }

  /** Retrieves the config properties for the database table represented by the given table. Returns empty map if no
    * config is defined for the table.
    */
  def getTableProperties(database: Database, tableName: String): Map[String, String] = {
    val tables = getTableMapForDatabase(database)
    val normalizedTableName = stripNamespacePrefix(database, tableName)
    tables.get(normalizedTableName).map { _.properties }.getOrElse(Map.empty)
  }
}

/** Represents a schema component with attached properties. Examples include the PII or private status of a column,
  * and column statistics computed from the database (e.g. maximum frequencies of join keys).
  */
abstract trait SchemaConfigWithProperties {
  private var _properties : Map[String,Any] = Map.empty
  @JsonAnySetter def set(name: String, value: Any): Unit = {
    _properties = _properties + (name -> value)
  }

  def properties = _properties map {case (k, v) => (k, v.toString())}
  def get[T](propName: String) = _properties.get(propName).asInstanceOf[Option[T]]
}

case class Column(name: String, fields: List[Column]) extends SchemaConfigWithProperties {}

/** Model of a specific database, including namespace and schema information. Consulted by
  * framework during query translation and analysis.
  */
case class Database(
    database: String,
    dialect: String,
    namespace: String,
    tables: List[Table] = Nil) {
  val tableMap = tables.map{ table => table.name -> table }.toMap

  def getSchemaMapForTable(tableName: String): Map[String, Column] = {
    val normalizedTableName = stripNamespacePrefix(this, tableName)
    tableMap.get(normalizedTableName).map{ _.columnMap }.getOrElse(Map.empty)
  }

  /** Clones the schema for the database but specifies a different query dialect */
  def withDialect(_dialect: String): Database = {
    val _tableMap = tableMap
    new Database(database, _dialect, namespace, Nil) {
      override val tableMap = _tableMap
    }
  }
}

case class Databases(databases: List[Database])

case class Table(@JsonProperty("table") name: String, columns: List[Column]) extends SchemaConfigWithProperties {
  val columnMap: Map[String, Column] = columns.map{ col => col.name -> col }.toMap
}

case class Tables(tables: List[Table])

class CalciteTable(table: Table) extends AbstractTable with ScannableTable {
  import scala.collection.JavaConverters._

  override def scan(root: DataContext): Enumerable[Array[AnyRef]] = ???

  def schemaToStruct(fields: List[Column], typeFactory: RelDataTypeFactory, structKind: StructKind = StructKind.FULLY_QUALIFIED): RelDataType = {
    if (fields == null || fields.isEmpty)
      typeFactory.createSqlType(SqlTypeName.ANY)
    else {
      val colNames = fields.map{ _.name }
      val colTypes = fields.map{ field => schemaToStruct(field.fields, typeFactory, StructKind.PEEK_FIELDS) }
      typeFactory.createStructType(structKind, colTypes.asJava, colNames.asJava)
    }
  }

  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    val result = schemaToStruct(table.columns, typeFactory)
    result
  }
}

class CalciteSchemaFromConfig(database: Database) extends AbstractSchema {
  import org.apache.calcite.schema.{Schema => CalciteSchema}

  import scala.collection.JavaConverters._

  val tableMap = new util.HashMap[String, schema.Table]()
  val subSchemaMap = new mutable.HashMap[String, mutable.Set[(String, schema.Table)]] with mutable.MultiMap[String, (String, schema.Table)]

  Schema.getTableMapForDatabase(database).values.foreach { table =>
    if (table.name.contains(".")) {
      val prefixIdx = table.name.indexOf(".")
      val subschemaName = table.name.substring(0, prefixIdx)
      val tableName = table.name.substring(prefixIdx+1)
      subSchemaMap.addBinding(subschemaName, (tableName, new CalciteTable(table).asInstanceOf[schema.Table]))
    } else {
      val calciteTable = new CalciteTable(table)
      tableMap.put(table.name, calciteTable)
    }
  }

  override def getTableMap: util.Map[String, schema.Table] = tableMap

  override def getSubSchemaMap: util.Map[String, CalciteSchema] = {
    val result = new util.HashMap[String, CalciteSchema] ()

    subSchemaMap.keys.foreach { key =>
      val newSchema = new AbstractSchema() {
        override def getTableMap: util.Map[String, schema.Table] = {
          subSchemaMap(key).toMap.asJava
        }
      }

      result.put(key, newSchema)
    }

    result
  }
}