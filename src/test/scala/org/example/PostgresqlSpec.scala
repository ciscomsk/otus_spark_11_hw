package org.example

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

class PostgresqlSpec extends AnyFlatSpec with TestContainerForAll {

  override val containerDef: PostgreSQLContainer.Def = PostgreSQLContainer.Def()

  val testTableName: String = "users"
  val partitionSize = 10

  "PostgreSQL data source" should "read table" in withContainers { postgresServer =>
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate

    val resDF: DataFrame = spark
      .read
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .option("partitionColumn", "user_id")
      .option("partitionSize", partitionSize)
      .load()

    assert(resDF.rdd.getNumPartitions == 5)

    resDF.show

    spark.stop
  }

  "PostgreSQL data source" should "write table" in withContainers { postgresServer =>
    val spark: SparkSession = SparkSession
      .builder
      .master("local[*]")
      .appName("PostgresWriterJob")
      .getOrCreate

    import spark.implicits._

    val df = (60 to 70).map(_.toLong).toDF("user_id")

    df
      .write
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .option("partitionColumn", "user_id")
      .option("partitionSize", partitionSize)
      .mode(SaveMode.Append)
      .save

    spark.stop
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case c: PostgreSQLContainer => {
        val conn: Connection = connection(c)
        val stmt1: Statement = conn.createStatement
        stmt1.execute(Queries.createTableQuery)
        val stmt2: Statement = conn.createStatement
        stmt2.execute(Queries.insertDataQuery)
        conn.close()
      }
    }
  }

  def connection(c: PostgreSQLContainer): Connection = {
    Class.forName(c.driverClassName)
    val properties: Properties = new Properties()
    properties.put("user", c.username)
    properties.put("password", c.password)
    DriverManager.getConnection(c.jdbcUrl, properties)
  }

  object Queries {
    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"
    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")
    lazy val insertDataQuery = s"INSERT INTO $testTableName VALUES $testValues;"
  }

}
