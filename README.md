# sc2cc - Super Columns to Composite Columns!

Tired of your worn out old Super Columns in Cassandra? Make 'em new again.<br/>
With one run of `sc2cc`, your columns will be as Composite as ever.

# Configuration

Configuration is broken into two parts

* Cassandra Configuration - the connection, cluster, keyspace, and column family
* Transformer Tuning - the number of work threads, task size, and queue size 

All properties are set in `src/main/resources/sc2cc.properties`. The properties that are not
commented are required. The values of the commented properties represent the default used if
the property is omitted.

Be sure to create the new column family schema. For example, if you have a super column family
with comparator and sub_comparator of 'UTF8Type', then create a new non-super column family with
comparator of 'CompositeType(UTF8Type,UTF8Type)'. The easiest way to do this is using `cassandra-cli`.

# Usage

To begin the transformation, just run

    mvn -e exec:java -Dexec.mainClass="com.brighttag.sc2cc.Main"

If you need to run this on another machine, your best option is to setup the configuration, then
build a single jar file containing the configuration, classes, and dependencies.

    mvn package

The resulting jar file is located at `target/cassandra-sc2cc-1.0-SNAPSHOT-jar-with-dependencies.jar`.

Now you can scp this file to the other machine(s) and run

    java -jar cassandra-sc2cc-1.0-SNAPSHOT-jar-with-dependencies.jar

That's it!