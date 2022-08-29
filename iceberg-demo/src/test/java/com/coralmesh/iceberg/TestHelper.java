package com.coralmesh.iceberg;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class TestHelper {
  public static final String JDBC_CATALOG_USER = "admin";
  public static final String JDBC_CATALOG_PASSWORD = "password";
  public static final String POSTGRESQL_IMAGE = "postgres:12.0";
  public static final String MINIO_IMAGE = "minio/minio:latest";
  public static final String MINIO_ROOT_USER = "minioadmin";
  public static final String MINIO_ROOT_PASSWORD = "minioadmin";
  public static final String MINIO_DEFAULT_REGION = "us-east-1";
  public static final String MINIO_WAREHOUSE_BUCKET = "warehouse";

  public static GenericContainer minioContainer(String image, String user, String password) {
    return new GenericContainer(image)
        .withEnv("MINIO_ROOT_USER", user)
        .withEnv("MINIO_ROOT_PASSWORD", password)
        .withExposedPorts(9000, 9001)
        .withCommand("server /data --console-address :9001");
  }

  public static PostgreSQLContainer postgresqlContainer(
      String image, String user, String password, String database) {
    return new PostgreSQLContainer(DockerImageName.parse(image))
        .withDatabaseName(database)
        .withUsername(user)
        .withPassword(password);
  }

  public static void createBucket(
      String endpoint, String accessKey, String SecretKey, String bucket) throws Exception {
    MinioClient minioClient =
        MinioClient.builder().endpoint(endpoint).credentials(accessKey, SecretKey).build();

    boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
    if (!found) {
      minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
    } else {
      System.out.println("Bucket already exists:" + bucket);
    }
  }
}
