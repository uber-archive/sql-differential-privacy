# Overview

This repository contains a full implementation of a differential privacy mechanism for SQL queries using elastic sensitivity,
including the SQL analysis framework used to build it.


Elastic sensitivity is an approach for efficiently approximating the local sensitivity of a query, which can be used to
enforce differential privacy for the query. The approach requires only a static analysis of the query and therefore
imposes minimal performance overhead. Importantly, it does not require any changes to the database.
Details of the approach are available in [this paper](https://arxiv.org/abs/1706.09479).

The framework for implementing elastic sensitivity is designed to perform dataflow analyses over complex SQL queries.
It provides an abstract representation of queries, plus several kinds of built-in dataflow analyses tailored to this
representation. This framework can be used to implement other types of dataflow analyses and will soon support additional differential privacy mechanisms for SQL.

## Building & Running

This framework is written in Scala and built using Maven. To build the code:

```
$ mvn package
```

## Example: Differential Privacy using Elastic Sensitivity

Elastic sensitivity can be used to determine the scale of random noise necessary to make the results of a query
differentially private. For a given output column of a query with elastic sensitivity *s*, to achieve
differential privacy for that column it suffices to *smooth* *s* according to the smooth sensitivity approach to obtain
*S*, then add random noise drawn from the Laplace distribution, scaled to *(S/epsilon)* and centered at 0, to the true
result of the query. The smoothing can be accomplished using the smooth sensitivity approach introduced by [Nissim et al](http://www.cse.psu.edu/~ads22/pubs/NRS07/NRS07-full-draft-v1.pdf).

Example code demonstrating this approach is available in `com.uber.engsec.dp.util.DPExample`.

To run this example:
```
mvn exec:java -Dexec.mainClass="com.uber.engsec.dp.util.DPExample"
```

 
## Analysis Framework

This framework can perform additional analyses on SQL queries, and can be extended with new analyses.
Each analysis in this framework extends the base class `com.uber.engsec.dp.sql.AbstractAnalysis`. 

To run an analysis on a query, call the method `com.uber.engsec.dp.sql.AbstractAnalysis.analyzeQuery`.
The parameter of this method is a string containing a SQL query, and its return value is an abstract domain representing
the results of the analysis.

The source code includes several example analyses to demonstrate features of the framework. The simplest example is `com.uber.engsec.dp.analysis.taint.TaintAnalysis`, which returns an abstract domain containing information about which output columns of the query might contain data flowing from "tainted" columns in the database. The database schema determines which columns are tainted. You can invoke this analysis as follows:

```scala
scala> (new com.uber.engsec.dp.analysis.taint.TaintAnalysis).analyzeQuery("SELECT my_col1 FROM my_table")
BooleanDomain = my_col1 -> False
```

This code includes several built-in analyses, including:

  - The elastic sensitivity analysis, available in `com.uber.engsec.dp.analysis.differential_privacy.ElasticSensitivityAnalysis`, returns an abstract domain (`com.uber.engsec.dp.analysis.differential_privacy.SensitivityDomain`) that maps each output column of the query to its elastic sensitivity.
  - `com.uber.engsec.dp.analysis.columns_used.ColumnsUsedAnalysis` lists the original database columns
  from which the results of each output column are computed.
  - `com.uber.engsec.dp.analysis.histogram.HistogramAnalysis` lists the aggregation-ness of each
  output column of the query (i.e. whether or not the output is an aggregation, and if so, which type).
  - `com.uber.engsec.dp.analysis.join.JoinKeysUsed` lists the original database columns used as equijoin
  keys for each output column of the query.

## Writing New Analyses

New analyses can be implemented by extending one of the abstract analysis classes and implementing *transfer functions*
which describe how to update the analysis state for relevant query constructs. Analyses are written to update a
specific type of *abstract domain* which represents the current state of the analysis. Each abstract domain type
implements the trait `com.uber.engsec.dp.dataflow.AbstractDomain`.

The simplest way to implement a new analysis is to use `com.uber.engsec.dp.dataflow.dp.column.AbstractColumnAnalysis`,
which automatically tracks analysis state for each column of the query independently. Most of the example analyses are
of this type.

New analyses can be invoked in the same way as the built-in example analyses.

## Reporting Security Bugs

Please report security bugs through [HackerOne](https://hackerone.com/uber).

## License

This project is released under the MIT License.

## Contact Information

This project is developed and maintained by [Noah Johnson](mailto:noahj@berkeley.edu) and [Joe Near](mailto:jnear@berkeley.edu).
