package org.example.datasource.postgres

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.{DriverManager, ResultSet}
import java.util
import scala.collection.JavaConverters._

/*
Доработайте код в файле src/main/scala/org/example/datasource/postgres/PostgresDatasource.scala так,
чтобы тест в файле src/test/scala/org/example/PostgresqlSpec.scala при выполнении читал таблицу users не в одну партицию,
а в несколько (размер одной партиции должен задаваться через метод .option("partitionSize", "10")).

Рекомендация: Архитектуру решения можно обсудить в общей группе в Slack.
 */

class DefaultSource extends TableProvider { //

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = PostgresTable.schema

  override def getTable(
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String]
  ): Table = new PostgresTable(properties.get("tableName")) // TODO: Error handling
}

class PostgresTable(val name: String) extends SupportsRead with SupportsWrite {
  override def schema(): StructType = PostgresTable.schema

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new PostgresScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new PostgresWriteBuilder(info.options)
}

object PostgresTable {
  val schema: StructType = new StructType().add("user_id", LongType)
}

case class ConnectionProperties(url: String, user: String, password: String, tableName: String, partitionSize: String, tableSize: String )

/** Read */

class PostgresScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build(): Scan = new PostgresScan(ConnectionProperties(
    options.get("url"),
    options.get("user"),
    options.get("password"),
    options.get("tableName"),
    options.get("partitionSize"),
    options.get("tableSize"),
  ))
}



case class PostgresPartition(start: Long, stop: Long) extends InputPartition

class PostgresScan(connectionProperties: ConnectionProperties) extends Scan with Batch {
  val partitionSize = connectionProperties.partitionSize.toInt
  val tableSize = connectionProperties.tableSize.toInt

  val partitionsQty: Int = Math.ceil(tableSize / partitionSize).toInt

  val partitions: Seq[PostgresPartition] = for (i <- 1 to partitionsQty)
    yield PostgresPartition((i - 1) * partitionSize, i * partitionSize)

  override def readSchema(): StructType = PostgresTable.schema
  override def toBatch: Batch = this
  override def planInputPartitions(): Array[InputPartition] = partitions.toArray
  override def createReaderFactory(): PartitionReaderFactory = new PostgresPartitionReaderFactory(connectionProperties)
}

class PostgresPartitionReaderFactory(connectionProperties: ConnectionProperties) extends PartitionReaderFactory {
  /**
   * Returns a row-based partition reader to read data from the given {@link InputPartition}.
   *
   * Implementations probably need to cast the input partition to the concrete
   * {@link InputPartition} class defined for the data source.
   */
  override def createReader(partition: InputPartition) = new PostgresPartitionReader(
    connectionProperties,
    partition.asInstanceOf[PostgresPartition].start,
    partition.asInstanceOf[PostgresPartition].stop)
}

class PostgresPartitionReader(connectionProperties: ConnectionProperties , offset: Long, limit: Long) extends PartitionReader[InternalRow] { // для чтения одной парртиции

  private val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  private val statement = connection.createStatement()
  private val resultSet = statement.executeQuery(
    s"select * from ${connectionProperties.tableName} offset ${offset} limit ${limit}"
  )

  override def next(): Boolean = resultSet.next()

  override def get(): InternalRow = InternalRow(resultSet.getLong(1))

  override def close(): Unit = connection.close()
}

/** Write */

class PostgresWriteBuilder(options: CaseInsensitiveStringMap) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new PostgresBatchWrite(ConnectionProperties(
    options.get("url"), options.get("user"), options.get("password"), options.get("tableName") , options.get("partitionSize"), options.get("tableSize")
  ))
}

class PostgresBatchWrite(connectionProperties: ConnectionProperties) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new PostgresDataWriterFactory(connectionProperties)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class PostgresDataWriterFactory(connectionProperties: ConnectionProperties) extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] =
    new PostgresWriter(connectionProperties)
}

object WriteSucceeded extends WriterCommitMessage

class PostgresWriter(connectionProperties: ConnectionProperties) extends DataWriter[InternalRow] {

  val connection = DriverManager.getConnection(
    connectionProperties.url,
    connectionProperties.user,
    connectionProperties.password
  )
  val statement = "insert into users (user_id) values (?)"
  val preparedStatement = connection.prepareStatement(statement)

  override def write(record: InternalRow): Unit = {
    val value = record.getLong(0)
    preparedStatement.setLong(1, value)
    preparedStatement.executeUpdate()
  }

  override def commit(): WriterCommitMessage = WriteSucceeded
  override def abort(): Unit = {}
  override def close(): Unit = connection.close()
}
