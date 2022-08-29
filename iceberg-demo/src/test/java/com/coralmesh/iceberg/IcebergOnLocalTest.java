package com.coralmesh.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.coralmesh.iceberg.TestHelper.*;

@Testcontainers
public class IcebergOnLocalTest {

  JdbcCatalog catalog = new JdbcCatalog();
  Namespace nyc = Namespace.of("nyc");
  TableIdentifier name = TableIdentifier.of(nyc, "logs");

  Schema schema =
      new Schema(
          Types.NestedField.required(1, "level", Types.StringType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "message", Types.StringType.get()),
          Types.NestedField.optional(
              4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get())));

  PartitionSpec spec =
      PartitionSpec.builderFor(schema).hour("event_time").identity("level").build();

  String JDBC_CATALOG_URI;
  String CATALOG_NAME = "demo_local";
  String SPARK_SQL_CATALOG = "spark.sql.catalog." + CATALOG_NAME;

  @TempDir Path tempPath;

  @Container
  private PostgreSQLContainer postgresqlContainer =
      postgresqlContainer(POSTGRESQL_IMAGE, JDBC_CATALOG_USER, JDBC_CATALOG_PASSWORD, CATALOG_NAME);

  @BeforeEach
  public void beforeEach() {
    JDBC_CATALOG_URI =
        "jdbc:postgresql://"
            + postgresqlContainer.getHost()
            + ":"
            + postgresqlContainer.getMappedPort(5432)
            + "/"
            + CATALOG_NAME;
    postgresqlContainer.start();

    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    properties.put(CatalogProperties.URI, JDBC_CATALOG_URI);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "user", JDBC_CATALOG_USER);
    properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", JDBC_CATALOG_PASSWORD);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, tempPath + "/iceberg/warehouse");
    properties.put(CatalogProperties.FILE_IO_IMPL, HadoopFileIO.class.getName());

    Configuration conf = new Configuration();
    catalog.setConf(conf);
    catalog.initialize(CATALOG_NAME, properties);

    catalog.createTable(name, schema, spec);
  }

  @AfterEach
  void afterEach() {
    catalog.dropTable(name, true);
    postgresqlContainer.stop();
  }

  @Test
  public void readAndWriteByOriginTest() throws IOException {
    writeDataByLocalDataWriter();
    Table table = catalog.loadTable(name);
    CloseableIterable<Record> result = IcebergGenerics.read(table).build();

    for (Record r : result) {
      System.out.println(r);
    }
  }

  @Test
  public void readAndWriteBySparkSQLTest() {
    writeLocalDataBySparkSQL();
    Table table = catalog.loadTable(name);
    CloseableIterable<Record> result = IcebergGenerics.read(table).build();

    int resultCount = 0;
    for (Record r : result) {
      System.out.println(r);
      resultCount += 1;
    }
    Assertions.assertEquals(3, resultCount);
  }

  private void writeDataByLocalDataWriter() throws IOException {
    Table table = catalog.loadTable(name);
    // write data
    OutputFile file = Files.localOutput(tempPath + "_" + UUID.randomUUID());
    DataWriter<Record> dataWriter =
        Parquet.writeData(file)
            .metricsConfig(MetricsConfig.forTable(table))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(spec)
            .withPartition(new PartitionKey(spec, schema))
            .build();

    GenericRecord genericRecord = GenericRecord.create(schema);
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    builder.add(
        genericRecord.copy(
            ImmutableMap.of("level", "info", "event_time", OffsetDateTime.now(), "message", "1")));
    builder.add(
        genericRecord.copy(
            ImmutableMap.of("level", "debug", "event_time", OffsetDateTime.now(), "message", "2")));
    builder.add(
        genericRecord.copy(
            ImmutableMap.of("level", "error", "event_time", OffsetDateTime.now(), "message", "3")));

    List<Record> overflowRecords = builder.build();
    try {
      for (Record record : overflowRecords) {
        dataWriter.write(record);
      }
    } finally {
      dataWriter.close();
    }

    DataFile dataFile = dataWriter.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
  }

  private void writeLocalDataBySparkSQL() {
    SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Java API Demo")
            .config(
                "spark.sql.extensions",
                org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions.class.getName())
            .config(SPARK_SQL_CATALOG, org.apache.iceberg.spark.SparkCatalog.class.getName())
            .config(
                SPARK_SQL_CATALOG + ".catalog-impl",
                org.apache.iceberg.jdbc.JdbcCatalog.class.getName())
            .config(SPARK_SQL_CATALOG + ".uri", JDBC_CATALOG_URI)
            .config(SPARK_SQL_CATALOG + ".jdbc.user", JDBC_CATALOG_USER)
            .config(SPARK_SQL_CATALOG + ".jdbc.password", JDBC_CATALOG_PASSWORD)
            .config(
                SPARK_SQL_CATALOG + ".io-impl",
                org.apache.iceberg.hadoop.HadoopFileIO.class.getName())
            .config(SPARK_SQL_CATALOG + ".warehouse", tempPath + "/iceberg/warehouse")
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
            .config("spark.eventLog.enabled", "true")
            //            .config("spark.eventLog.dir", tempPath + "/iceberg/spark-events")
            .config("spark.eventLog.dir", tempPath.toString())
            //            .config("spark.history.fs.logDirectory", tempPath +
            // "/iceberg/spark-events")
            .config("spark.history.fs.logDirectory", tempPath.toString())
            .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    String query =
        "INSERT INTO demo_local.nyc.logs "
            + "VALUES "
            + "('info', timestamp 'today', 'Just letting you know!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('warning', timestamp 'today', 'You probably should not do this!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('error', timestamp 'today', 'This was a fatal application error!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3'))";

    spark.sql(query).show();

    String selectSQLStr = "SELECT * FROM demo_local.nyc.logs";
    Dataset<Row> ds = spark.sql(selectSQLStr);
    Assertions.assertEquals(3, ds.count());
  }
}
