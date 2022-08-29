package com.coralmesh.iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import java.nio.file.Path;

import static com.coralmesh.iceberg.TestHelper.*;

// TODO
public class IcebergOnS3Test {

  String CATALOG_NAME = "demo_local_s3";
  String SPARK_SQL_CATALOG = "spark.sql.catalog." + CATALOG_NAME;
  String DEMO_LOCAL_S3_NYC_LOGS_PATH = CATALOG_NAME + ".nyc.logs";

  @TempDir Path tempPath;

  SparkSession spark;

  @Container
  private GenericContainer minioContainer =
      minioContainer(MINIO_IMAGE, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD);

  @Container
  private PostgreSQLContainer postgresqlContainer =
      postgresqlContainer(POSTGRESQL_IMAGE, JDBC_CATALOG_USER, JDBC_CATALOG_PASSWORD, CATALOG_NAME);

  @BeforeEach
  public void beforeEach() throws Exception {
    minioContainer.start();
    postgresqlContainer.start();

    String minioEndPoint =
        "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(9000);

    createBucket(minioEndPoint, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_WAREHOUSE_BUCKET);

    System.setProperty("aws.accessKeyId", MINIO_ROOT_USER);
    System.setProperty("aws.secretAccessKey", MINIO_ROOT_PASSWORD);
    System.setProperty("aws.region", MINIO_DEFAULT_REGION);

    spark =
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
            .config(
                SPARK_SQL_CATALOG + ".uri",
                "jdbc:postgresql://"
                    + postgresqlContainer.getHost()
                    + ":"
                    + postgresqlContainer.getMappedPort(5432)
                    + "/"
                    + CATALOG_NAME)
            .config(SPARK_SQL_CATALOG + ".jdbc.user", JDBC_CATALOG_USER)
            .config(SPARK_SQL_CATALOG + ".jdbc.password", JDBC_CATALOG_PASSWORD)
            .config(
                SPARK_SQL_CATALOG + ".io-impl", org.apache.iceberg.aws.s3.S3FileIO.class.getName())
            .config(SPARK_SQL_CATALOG + ".warehouse", "s3://" + MINIO_WAREHOUSE_BUCKET)
            .config(SPARK_SQL_CATALOG + ".s3.endpoint", minioEndPoint)
            .config(SPARK_SQL_CATALOG + ".s3.delete-enabled", true)
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
            .config("spark.eventLog.enabled", "true")
            //            .config("spark.eventLog.dir", tempPath + "/iceberg/spark-events")
            .config("spark.eventLog.dir", tempPath.toString())
            //            .config("spark.history.fs.logDirectory", tempPath +
            // "/iceberg/spark-events")
            .config("spark.history.fs.logDirectory", tempPath.toString())
            .getOrCreate();
    spark
        .sql(
            "CREATE TABLE "
                + DEMO_LOCAL_S3_NYC_LOGS_PATH
                + " (level string, event_time timestamp,message string,call_stack array<string>) USING iceberg"
                + " PARTITIONED BY (hours(event_time), identity(level))")
        .show();
  }

  @AfterEach
  void afterEach() {
    spark.sql("DROP table " + DEMO_LOCAL_S3_NYC_LOGS_PATH).show();
    postgresqlContainer.stop();
    minioContainer.stop();
  }

  @Test
  public void writeTest() {
    writeS3DataBySparkSQL();
  }

  private void writeS3DataBySparkSQL() {

    spark.sparkContext().setLogLevel("ERROR");

    String query =
        "INSERT INTO "
            + DEMO_LOCAL_S3_NYC_LOGS_PATH
            + " VALUES "
            + "('info', timestamp 'today', 'Just letting you know!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('warning', timestamp 'today', 'You probably should not do this!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('error', timestamp 'today', 'This was a fatal application error!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3'))";

    spark.sql(query).show();

    String selectSQLStr = "SELECT * FROM " + DEMO_LOCAL_S3_NYC_LOGS_PATH;
    Dataset<Row> ds = spark.sql(selectSQLStr);
    ds.show();
    Assertions.assertEquals(3, ds.count());
  }
}
