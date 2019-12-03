# Overview

(This project is deprecated and not maintained.)

This repository contains a query analysis and rewriting framework to enforce differential privacy for general-purpose
SQL queries. The rewriting engine can automatically transform an input query into an *intrinsically private query* which
embeds a differential privacy mechanism in the query directly; the transformed query enforces differential privacy on
its results and can be executed on any standard SQL database. This approach supports many state-of-the-art
differential privacy mechanisms; the code currently includes rewriters based on [Elastic Sensitivity](https://arxiv.org/abs/1706.09479) and
[Sample and Aggregate](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.296.2379&rep=rep1&type=pdf), and more will be added soon.

The rewriting framework is built on a robust dataflow analyses engine for SQL queries. This framework
provides an abstract representation of queries, plus several kinds of built-in dataflow analyses tailored to this
representation. This framework can be used to implement other types of dataflow analyses, as described below.

## Building & Running

This framework is written in Scala and built using Maven. The code has been tested on Mac OS X and Linux. To build the code:

```
$ mvn package
```

## Example: Query Rewriting

The file `examples/QueryRewritingExample.scala` contains sample code for query rewriting and demonstrates the supported
mechanisms using a few simple queries. To run this example:
```
mvn exec:java -Dexec.mainClass="examples.QueryRewritingExample"
```

This example code can be easily modified, e.g., to test different queries or change parameter values.

## Background: Elastic Sensitivity

Elastic sensitivity is an approach for efficiently approximating the local sensitivity of a query, which can be used to
enforce differential privacy for the query. The approach requires only a static analysis of the query and therefore
imposes minimal performance overhead. Importantly, it does not require any changes to the database.
Details of the approach are available in [this paper](https://arxiv.org/abs/1706.09479).

Elastic sensitivity can be used to determine the scale of random noise necessary to make the results of a query
differentially private. For a given output column of a query with elastic sensitivity *s*, to achieve
differential privacy for that column it suffices to *smooth* *s* according to the smooth sensitivity approach to obtain
*S*, then add random noise drawn from the Laplace distribution, scaled to *(S/epsilon)* and centered at 0, to the true
result of the query. The smoothing can be accomplished using the smooth sensitivity approach introduced by [Nissim et al](http://www.cse.psu.edu/~ads22/pubs/NRS07/NRS07-full-draft-v1.pdf).

The file `examples.ElasticSensitivityExample` contains code demonstrating this approach directly (i.e., applying noise manually rather than generating an intrinsically private query).

To run this example:
```
mvn exec:java -Dexec.mainClass="examples.ElasticSensitivityExample"
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
