package at.ac.ait

import com.datastax.spark.connector.rdd.ValidRDDType
import com.datastax.spark.connector.rdd.reader.RowReaderFactory
import org.apache.spark.sql.{ Dataset, Encoder, SparkSession, SaveMode }
import scala.reflect.ClassTag

import org.apache.spark.sql.functions.{ count, collect_set, concat_ws, explode, hex, lower, max }

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }

class RansomwareDataset(spark: SparkSession) {

  import spark.implicits._
  import com.datastax.spark.connector._

  val hdfs = "ENTER HDFS HOST"
  val hdfsPath = hdfs + "ENTER HDFS PATH"

  def ff(number: Long) = {
    val formatter = java.text.NumberFormat.getNumberInstance
    formatter.format(number)
  }

  private val fs = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfs)
    FileSystem.get(conf)
  }

  def writeAsString(hdfsPath: String, content: String) {
    val path: Path = new Path(hdfsPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val os = fs.create(path)
    os.write(content.getBytes)
    os.close()
  }

  def printColumns[T](ds: Dataset[T]) = {
    ds.columns.reduce((s1, s2) => s1 + "," + s2)
  }

  def store[T](dataset: Dataset[T], filename: String) = {
    dataset
      .write
      .mode(SaveMode.Overwrite)
      .option("header", false)
      .csv(hdfsPath + filename)

    writeAsString(
      hdfsPath + filename.split("\\.").head + "_headers." + filename.split("\\.").last,
      printColumns(dataset))

  }

  // Datset Loading

  def loadSeedAddresses(filename: String) = {
    spark.read
      .option("header", true)
      .csv(hdfsPath + filename)
      .select($"address", $"family")
      .distinct
      .as[FocusAddress]
  }

  def load[T <: Product: ClassTag: RowReaderFactory: ValidRDDType: Encoder](keyspace: String, tableName: String, columns: ColumnRef*) = {
    val table = spark.sparkContext.cassandraTable[T](keyspace, tableName)
    if (columns.isEmpty)
      table.toDS().as[T]
    else
      table.select(columns: _*).toDS().as[T]
  }

  // Dataset Computation

  def computeBlockchainStatistics(
    addresses:           Dataset[Address],
    clusters:            Dataset[Cluster],
    transactions:        Dataset[Transaction],
    addrOutRelations:    Dataset[AddressOutgoingRelations],
    clusterOutRelations: Dataset[ClusterRelations]) = {

    Seq(
      BlockchainStatistics(
        transactions.select(max($"height")).first().getInt(0),
        transactions.count,
        addresses.count,
        clusters.count,
        addrOutRelations.count,
        clusterOutRelations.count)).toDS
  }

  def computeExpandedAddresses(
    focusAddresses:  Dataset[FocusAddress],
    addressClusters: Dataset[AddressCluster]) = {

    val focusAddressesWithClusters = focusAddresses
      .join(addressClusters, Seq("address"), "left_outer")
      .select($"address", $"family", $"cluster")

    val expandedAddresses = focusAddressesWithClusters
      .select($"cluster", $"family")
      .distinct
      .join(addressClusters, Seq("cluster"), "left_outer")
      .select($"address", $"family", $"cluster")

    focusAddressesWithClusters
      .union(expandedAddresses)
      .distinct
      .as[ExpandedAddress]
  }

  def computeAddressStatistics(
    expandedAddresses: Dataset[ExpandedAddress],
    addresses:         Dataset[Address]) = {

    expandedAddresses
      .join(addresses, "address")
      .select($"address", $"family", $"cluster",
        $"noIncomingTxs", $"noOutgoingTxs",
        $"firstTx.timestamp" as "firstTx",
        $"lastTx.timestamp" as "lastTx",
        $"totalReceived.satoshi" as "totalReceivedSATOSHI",
        $"totalReceived.usd" as "totalReceivedUSD")
      .as[AddressStatistics]
  }

  def computeIncomingTransactions(
    expandedAddresses: Dataset[ExpandedAddress],
    transactions:      Dataset[Transaction]) = {

    transactions.withColumn("output", explode($"outputs"))
      .select(
        $"output.address" as "address",
        lower(hex($"txHash")) as "txHash",
        $"timestamp",
        $"output.value" as "valueSATOSHI")
      .join(expandedAddresses.select($"address", $"family", $"cluster"), "address")
      .as[RansomwareTransaction]
  }

  def computeOutgoingTransactions(
    expandedAddresses: Dataset[ExpandedAddress],
    transactions:      Dataset[Transaction]) = {

    transactions.withColumn("input", explode($"inputs"))
      .select(
        $"input.address" as "address",
        lower(hex($"txHash")) as "txHash",
        $"timestamp",
        $"input.value" as "valueSATOSHI")
      .join(expandedAddresses.select($"address", $"family", $"cluster"), "address")
      .as[RansomwareTransaction]
  }

  def computeOutgoingRelations(
    expandedAddresses: Dataset[ExpandedAddress],
    outgoingRelations: Dataset[AddressOutgoingRelations],
    addressClusters: Dataset[AddressCluster]) = {

    expandedAddresses.select($"address", $"family", $"cluster" as "srcCluster")
      .join(
        outgoingRelations
          .select(
            $"srcAddress" as "address",
            $"dstAddress",
            $"estimatedValue.USD" as "estimatedValueUSD",
            $"estimatedValue.SATOSHI" as "estimatedValueSATOSHI",
            $"noTransactions"),
        "address")
      .join(
          addressClusters.select($"address" as "dstAddress", $"cluster" as "dstCluster"),
          Seq("dstAddress"),
          "left_outer")
      .as[RansomwareOutgoingRelation]
  }

  def computeClusterTags(
    addressClusters:   Dataset[AddressCluster],
    tags:              Dataset[Tag]) = {

    tags.join(addressClusters, "address")
      .filter($"cluster".isNotNull)
      .select($"cluster", $"tag", $"actorCategory")
      .groupBy($"cluster")
      .agg(
        count($"tag") as "tagCount",
        concat_ws("|", collect_set($"tag")) as "tags",
        concat_ws("|", collect_set($"actorCategory")) as "categories")
      .as[ClusterTags]
  }

  def computeClusterStatistics(
    expandedAddresses: Dataset[ExpandedAddress],
    clusters:          Dataset[Cluster]) = {

    expandedAddresses
      .select($"cluster").distinct
      .join(clusters, "cluster")
      .select(
        $"cluster", $"noAddresses",
        $"noIncomingTxs", $"noOutgoingTxs",
        $"firstTx.timestamp" as "firstTx",
        $"lastTx.timestamp" as "lastTx",
        $"totalReceived.satoshi" as "totalReceivedSATOSHI",
        $"totalReceived.usd" as "totalReceivedUSD")
      .as[ClusterStatistics]

  }

  def extract() {

    // load pre-computed dataset
    val tags = load[Tag]("graphsense_raw", "tag").persist
    val transactions = load[Transaction]("graphsense_transformed", "transaction",
      "tx_hash", "height", "timestamp", "inputs", "outputs").persist
    val addressClusters = load[AddressCluster]("graphsense_transformed", "address_cluster",
      "address", "cluster").persist
    val addresses = load[Address]("graphsense_transformed", "address",
      "address", "no_incoming_txs", "no_outgoing_txs", "first_tx", "last_tx", "total_received", "total_spent").persist
    val clusters = load[Cluster]("graphsense_transformed", "cluster").persist
    val addrOutRelations = load[AddressOutgoingRelations]("graphsense_transformed", "address_outgoing_relations",
      "src_address", "dst_address", "no_transactions", "estimated_value").persist
    val clusterInRelations = load[ClusterRelations]("graphsense_transformed", "cluster_incoming_relations",
      "src_cluster", "dst_cluster", "no_transactions")

//    val blockchainStatistics = computeBlockchainStatistics(addresses, clusters, transactions, addrOutRelations, clusterInRelations)
//    blockchainStatistics.show
//    store(blockchainStatistics, "blockchain_stats.csv")

    // load collector addresses
    val seedAddresses = loadSeedAddresses("seed_addresses.csv").persist
    println(s"Loaded ${seedAddresses.count} seed addresses.")

    // filter out families and tags
    val filteredSeedAddresses = seedAddresses.filter(!$"family".isin("Unknown", "Ransomware"))
    val filteredTags = tags.filter($"source".isin("walletexplorer.com", "blockchain.info"))
    
    // compute tags for clusters
    val clusterTags = computeClusterTags(addressClusters, filteredTags).persist
    
    // expand seed addresses using multiple-input heuristics
    val expandedAddresses = computeExpandedAddresses(filteredSeedAddresses, addressClusters).persist

    val expandedAddressesStatistics = computeAddressStatistics(expandedAddresses, addresses)
    store(expandedAddressesStatistics, "expanded_addresses_stats.csv")

    val incomingTransactions = computeIncomingTransactions(expandedAddresses, transactions)
    store(incomingTransactions, "expanded_txs_incoming.csv")

    val outgoingTransactions = computeOutgoingTransactions(expandedAddresses, transactions)
    store(outgoingTransactions, "expanded_txs_outgoing.csv")

    val outgoingRelations = computeOutgoingRelations(expandedAddresses, addrOutRelations, addressClusters).persist
    store(outgoingRelations, "expanded_rel_outgoing.csv")

    val outgoingRelationsTags = outgoingRelations
      .select($"dstCluster" as "cluster").distinct
      .join(clusterTags, "cluster")
    store(outgoingRelationsTags, "expanded_rel_outgoing_cluster_tags.csv")

    val expandedAddressesClusterTags = expandedAddresses
      .select($"cluster").distinct
      .join(clusterTags, "cluster")
    store(expandedAddressesClusterTags, "expanded_addresses_cluster_tags.csv")
      
    val expandedAddressesClusterStatistics = computeClusterStatistics(expandedAddresses, clusters)
    store(expandedAddressesClusterStatistics, "expanded_addresses_cluster_stats.csv")

    // load collector addresses
    val collectorAddresses = loadSeedAddresses("collector_addresses.csv").persist
    println(s"Loaded ${collectorAddresses.count} collector addresses.")

    val collectorAddressesWithCluster = collectorAddresses
      .join(addressClusters, Seq("address"), "left_outer")
      .select($"address", $"family", $"cluster")
      .as[ExpandedAddress]
      .persist
    store(collectorAddressesWithCluster, "collector_addresses_w_cluster.csv")

    val collectorAddressesStatistics = computeAddressStatistics(collectorAddressesWithCluster, addresses)
    store(collectorAddressesStatistics, "collector_addresses_stats.csv")
    
    val collectorClusterTags = collectorAddressesWithCluster
      .select($"cluster").distinct
      .join(clusterTags, "cluster")
    store(collectorClusterTags, "collector_cluster_tags.csv")

    val collectorClusterStatistics = computeClusterStatistics(collectorAddressesWithCluster, clusters)
    store(collectorClusterStatistics, "collector_cluster_stats.csv")
    
    val collectorOutgoingRelations = computeOutgoingRelations(collectorAddressesWithCluster, addrOutRelations, addressClusters).persist
    store(collectorOutgoingRelations, "collector_rel_outgoing.csv")

    val collectorOutgoingRelationsTags = collectorOutgoingRelations
      .select($"dstCluster" as "cluster").distinct
      .join(clusterTags, "cluster")
    store(collectorOutgoingRelationsTags, "collector_rel_outgoing_cluster_tags.csv")
    
    ()
  }

}