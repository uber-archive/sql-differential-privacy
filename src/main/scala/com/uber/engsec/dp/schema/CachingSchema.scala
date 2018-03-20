package org.apache.calcite.jdbc

import org.apache.calcite.schema.{Schema, SchemaPlus}

object SchemaAdapter {
  def toRootSchemaPlus(schema: Schema, name: String): SchemaPlus = new CachingCalciteSchema(null, schema, name).plus()
}