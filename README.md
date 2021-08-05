# DataFusion MongoDB Connector

This is a connector that allows reading MongoDB data into Apache Arrow DataFusion via Apache Arrow.

The library works by writing a logical plan into an aggregation framework query, thereby passing down projection and filter predicates.

The library still needs a lot of work before being production ready, but I am making its code public in case someone finds some use for it.
