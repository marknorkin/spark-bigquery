package com.appsflyer.spark.bigquery

import org.apache.spark.sql.streaming.DataStreamWriter

/**
  * Spark streaming support for BigQuery
  */
package object streaming {

  /**
    * Enhanced version of DataStreamWriter with BigQuery support.
    *
    * Sample usage:
    *
    * <code>df.writeStream.bigQueryTable("project-id:dataset-id.table-id")</code>
    */
  implicit class BigQueryDataFrameWriter(writer: DataStreamWriter[String]) extends Serializable {

    /**
      * Insert data into BigQuery table using streaming API
      *
      * @param fullyQualifiedOutputTableId output-table id of the form
      *                                    [optional projectId]:[datasetId].[tableId]
      * @param batchSize                   number of rows to write to BigQuery at once
      *                                    (default: 500)
      * @param isPartitionedByDay          Partition table by day
      *                                    (default: false)
      * @param partitionExpirationMs       Number of milliseconds for which to keep the storage for a partition,
      *                                    or <code>null</code> to disable expiration at all.
      *                                    (default: null)
      */
    def bigQueryTable(fullyQualifiedOutputTableId: String,
                      batchSize: Int = 500,
                      isPartitionedByDay: Boolean = false,
                      partitionExpirationMs: Long = null): Unit = {

      val bigQueryWriter = new BigQueryStreamWriter(fullyQualifiedOutputTableId, batchSize, isPartitionedByDay,
        partitionExpirationMs)

      writer.foreach(bigQueryWriter).start
    }
  }

}
