package com.appsflyer.spark.utils

import com.appsflyer.spark.bigquery.BigQueryServiceFactory
import com.appsflyer.spark.bigquery.streaming.BigQueryStreamWriter
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model.{Table, TableReference, TimePartitioning}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.control.NonFatal

object BigQueryPartitionUtils {

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryStreamWriter])

  val bqService = BigQueryServiceFactory.getService

  def createBigQueryPartitionedTable(targetTable: TableReference,
                                     partitionExpirationMs: Long = null): Any = {

    val datasetId = targetTable.getDatasetId
    val projectId: String = targetTable.getProjectId
    val tableName = targetTable.getTableId

    try {
      logger.info("Creating Time Partitioned Table")
      val table = new Table()
      table.setTableReference(targetTable)

      val timePartitioning = new TimePartitioning()
      timePartitioning.setType("DAY")
      timePartitioning.setExpirationMs(partitionExpirationMs)
      table.setTimePartitioning(timePartitioning)

      val request = bqService.tables().insert(projectId, datasetId, table)
      request.execute()
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 409 =>
        logger.info(s"$projectId:$datasetId.$tableName already exists")
      case NonFatal(e) => throw e
    }
  }
}
