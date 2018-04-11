package at.ac.ait

// SPECIFIC CLASSES

case class BlockchainStatistics(
  blocks:       Int,
  transactions: Long,
  addresses:    Long,
  clusters:     Long,
  addressRel:   Long,
  clusterRel:   Long)

case class FocusAddress(
  address: String,
  family:  String)

case class ExpandedAddress(
  address: String,
  family:  String,
  cluster: Long)

case class AddressStatistics(
  address:              String,
  family:               String,
  noIncomingTxs:        Int,
  noOutgoingTxs:        Int,
  firstTx:              Int,
  lastTx:               Int,
  totalReceivedSATOSHI: Long,
  totalReceivedUSD:     Double,
  cluster:              Long)

case class ClusterStatistics(
  cluster:              Long,
  noAddresses:          Int,
  noIncomingTxs:        Int,
  noOutgoingTxs:        Int,
  firstTx:              Int,
  lastTx:               Int,
  totalReceivedSATOSHI: Long,
  totalReceivedUSD:     Double)

case class RansomwareTransaction(
  address:      String,
  family:       String,
  cluster:      Long,
  txHash:       String,
  timestamp:    Int,
  valueSATOSHI: Long)

case class RansomwareOutgoingRelation(
  address:               String,
  family:                String,
  srcCluster:            Long,
  dstAddress:            String,
  estimatedValueUSD:     Double,
  estimatedValueSATOSHI: Long,
  noTransactions:        Int,
  dstCluster:            Long)

case class ClusterTags(
  cluster:    Long,
  tags:       String,
  tagCount:   Long,
  categories: String)

// GRAPHSENSE MODEL CLASSES

case class AddressCluster(
  address: String,
  cluster: Long)

case class Tag(
  address:       String,
  tag:           String,
  tagUri:        String,
  description:   String,
  actorCategory: String,
  source:        String,
  sourceUri:     String,
  timestamp:     Int)

case class TxIdTime(
  txHash:    Array[Byte],
  height:    Int,
  timestamp: Int)

case class Value(
  satoshi: Long,
  eur:     Double,
  usd:     Double)

case class Address(
  address:       String,
  noIncomingTxs: Int,
  noOutgoingTxs: Int,
  firstTx:       TxIdTime,
  lastTx:        TxIdTime,
  totalReceived: Value,
  totalSpent:    Value)

case class Cluster(
  cluster:       Long,
  noAddresses:   Int,
  noIncomingTxs: Int,
  noOutgoingTxs: Int,
  firstTx:       TxIdTime,
  lastTx:        TxIdTime,
  totalReceived: Value,
  totalSpent:    Value)

case class TxInputOutput(
  address: Option[String],
  value:   Option[Long])

case class Transaction(
  //  txPrefix: String,
  txHash:    Array[Byte],
  height:    Int,
  timestamp: Int,
  //  coinbase: Boolean,
  //  totalInput: Long,
  //  totalOutput: Long,
  inputs:  Seq[TxInputOutput],
  outputs: Seq[TxInputOutput])

case class AddressOutgoingRelations(
  srcAddress:     String,
  dstAddress:     String,
  noTransactions: Int,
  estimatedValue: Value)

case class ClusterRelations(
  srcCluster:     Long,
  dstCluster:     Long,
  noTransactions: Int)
  
  