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
package com.uber.engsec.dp.sql.relational_algebra

import java.util
import java.util.Properties

import com.google.common.collect.ImmutableList
import com.uber.engsec.dp.schema.{CalciteSchemaFromConfig, Schema}
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.avatica.util.{Casing, Quoting}
import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.{HepPlanner, HepProgram}
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.CachingRelMetadataProvider
import org.apache.calcite.rel.rules.FilterJoinRule
import org.apache.calcite.rel.{RelNode, RelRoot}
import org.apache.calcite.rex.{RexBuilder, RexExecutor}
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.{SqlConformance, SqlConformanceEnum, SqlValidatorImpl}
import org.apache.calcite.sql.{SqlInsert, SqlOperatorTable}
import org.apache.calcite.sql2rel.{RelDecorrelator, SqlRexConvertletTable, SqlToRelConverter}
import org.apache.calcite.tools._

/** Transforms a SQL query to relational algebra using Calcite. */
object Transformer {
  val schema = new CalciteSchemaFromConfig

  val parserConfig = SqlParser.configBuilder
    .setQuoting(Quoting.DOUBLE_QUOTE)
    .setConformance(SqlConformanceEnum.LENIENT)
    .setUnquotedCasing(Casing.UNCHANGED)
    .setQuotedCasing(Casing.UNCHANGED)
    .setCaseSensitive(false)
    .build

  val rootSchema = Frameworks.createRootSchema(true)

  val conformanceProperties = new Properties()
  conformanceProperties.setProperty("conformance", "LENIENT")

  val config = Frameworks.newConfigBuilder
    .defaultSchema(rootSchema.add(Schema.currentDb.namespace, schema))
    .parserConfig(parserConfig)
    .build

  def create: Transformer = new Transformer(config)
}

class Transformer(val config: FrameworkConfig) {
  val operatorTable: SqlOperatorTable = config.getOperatorTable
  val programs: ImmutableList[Program] = config.getPrograms
  val traitDefs: ImmutableList[RelTraitDef[_ <: RelTrait]] = config.getTraitDefs
  val parserConfig: SqlParser.Config = config.getParserConfig
  val convertletTable: SqlRexConvertletTable = config.getConvertletTable
  val executor: RexExecutor = config.getExecutor

  val defaultSchema = config.getDefaultSchema
  var typeFactory: JavaTypeFactory = null
  var planner: RelOptPlanner = null

  var validator: SqlValidatorImpl = null
  var root: RelRoot = null

  def getEmptyTraitSet: RelTraitSet = planner.emptyTraitSet

  def convertToRelTree(sql: String): RelNode = {
    Frameworks.withPlanner(new Frameworks.PlannerAction[Void]() {
      override def apply(cluster: RelOptCluster, relOptSchema: RelOptSchema, rootSchema: SchemaPlus): Void = {
        typeFactory = cluster.getTypeFactory.asInstanceOf[JavaTypeFactory]
        planner = cluster.getPlanner
        planner.setExecutor(executor)
        null.asInstanceOf[Void]
      }
    }, this.config)

    if (this.traitDefs != null) {
      import scala.collection.JavaConverters._
      planner.clearRelTraitDefs()
      this.traitDefs.asScala.foreach { planner.addRelTraitDef(_) }
    }

    val parser = SqlParser.create(sql, parserConfig)
    val sqlNode = parser.parseStmt
    val conformance = SqlConformanceEnum.LENIENT
    val catalogReader = createCatalogReader
    this.validator = new CalciteSqlValidator(operatorTable, catalogReader, typeFactory, conformance)
    this.validator.setIdentifierExpansion(true)

    val validatedSqlNode = validator.validate(sqlNode)

    val rexBuilder = new RexBuilder(typeFactory)
    val cluster = RelOptCluster.create(planner, rexBuilder)
    val config = SqlToRelConverter
      .configBuilder
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withExpand(false)
      .build

    val sqlToRelConverter =
      new SqlToRelConverter(new ViewExpanderImpl, validator, createCatalogReader, cluster, convertletTable, config)

    root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true)
    root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
    root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
    root = root.withRel(sqlToRelConverter.trimUnusedFields(true, root.rel))

    val program = HepProgram.builder.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN).build
    val optPlanner = new HepPlanner(program)
    optPlanner.setRoot(root.rel)
    optPlanner.findBestExp
  }

  class ViewExpanderImpl extends RelOptTable.ViewExpander {
    override def expandView(rowType: RelDataType,
                            queryString: String,
                            schemaPath: util.List[String],
                            viewPath: util.List[String]): RelRoot = {
      val parser = SqlParser.create(queryString, parserConfig)
      val sqlNode = parser.parseQuery

      val conformance = SqlConformanceEnum.LENIENT
      val catalogReader = createCatalogReader.withSchemaPath(schemaPath)
      val validator = new CalciteSqlValidator(operatorTable, catalogReader, typeFactory, conformance)
      validator.setIdentifierExpansion(true)
      val validatedSqlNode = validator.validate(sqlNode)
      val rexBuilder = new RexBuilder(typeFactory)
      val cluster = RelOptCluster.create(planner, rexBuilder)
      val config = SqlToRelConverter.configBuilder.withTrimUnusedFields(false).withConvertTableAccess(false).build
      val sqlToRelConverter = new SqlToRelConverter(new ViewExpanderImpl, validator, catalogReader, cluster, convertletTable, config)
      root = sqlToRelConverter.convertQuery(validatedSqlNode, true, false)
      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true))
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel))
      root
    }
  }

  def createCatalogReader = {
    val _rootSchema = rootSchema(defaultSchema)
    new CalciteCatalogReader(CalciteSchema.from(_rootSchema), parserConfig.caseSensitive, CalciteSchema.from(defaultSchema).path(null), typeFactory)
  }

  def rootSchema(schema: SchemaPlus): SchemaPlus = {
    if (schema.getParentSchema == null)
      schema
    else
      rootSchema(schema.getParentSchema)
  }

  @throws[RelConversionException]
  def transform(ruleSetIndex: Int, requiredOutputTraits: RelTraitSet, rel: RelNode): RelNode = {
    rel.getCluster.setMetadataProvider(new CachingRelMetadataProvider(rel.getCluster.getMetadataProvider, rel.getCluster.getPlanner))
    val program = programs.get(ruleSetIndex)
    program.run(planner, rel, requiredOutputTraits, ImmutableList.of[RelOptMaterialization], ImmutableList.of[RelOptLattice])
  }
}

class CalciteSqlValidator(val opTab: SqlOperatorTable,
                          val _catalogReader: CalciteCatalogReader,
                          val _typeFactory: JavaTypeFactory,
                          val conformance: SqlConformance)
  extends SqlValidatorImpl(opTab, _catalogReader, _typeFactory, conformance) {

  override def getLogicalSourceRowType(sourceRowType: RelDataType, insert: SqlInsert): RelDataType = {
    val superType = super.getLogicalSourceRowType(sourceRowType, insert)
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(superType)
  }

  override def getLogicalTargetRowType(targetRowType: RelDataType, insert: SqlInsert): RelDataType = {
    val superType = super.getLogicalTargetRowType(targetRowType, insert)
    typeFactory.asInstanceOf[JavaTypeFactory].toSql(superType)
  }
}