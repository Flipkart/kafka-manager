package kafka.manager

import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.manager.ActorModel._
import kafka.manager.utils.FiniteQueue
import org.joda.time.DateTime

import scala.collection.immutable.Queue

/**
 * Created by vinay.varma on 9/9/15.
 */

class TopicMonitorActor() extends BaseQueryCommandActor {

  val soTimeOut = 5000
  val bufferSize = 50
  val clientId = "simple-offset-monitor"


  private[this] var topicOffsetCountMap: Map[String, Seq[PartitionOffsetCount]] = Map.empty
  private[this] var tpOffsetCountMap: Map[TopicPartition, OffsetCount] = Map.empty


  def updateProducerStats(brokerList: BrokerList, topicIdentities: Map[String, TopicIdentity]): Unit = {
    val brokers = for (bi <- brokerList.list) yield bi.id -> bi
    val brokerMap: Map[Int, BrokerIdentity] = brokers.toMap
    val topicPartitionByBroker = topicIdentities.values.flatMap { ti =>
      ti.partitionsIdentity.values.map(tpi => (tpi.leader, ti.topic, tpi.partNum))
    }.groupBy(_._1)
    for (tpb <- topicPartitionByBroker) {
      aroundSimpleConsumer(brokerMap(tpb._1))(updateCurrentOffsetFor(_)(tpb._2))
    }

  }

  def aroundSimpleConsumer(bi: BrokerIdentity)(calc: SimpleConsumer => Unit): Unit = {
    val consumer = new SimpleConsumer(bi.host, bi.port, soTimeOut, bufferSize, clientId)
    calc(consumer)
    consumer.close()
  }

  def updateCurrentOffsetFor(consumer: SimpleConsumer)(tps: Iterable[(Int, String, Int)]): Unit = {
    val now = DateTime.now()
    for (tp <- tps) {
      val response = consumer.earliestOrLatestOffset(TopicAndPartition(tp._2, tp._3), -1, 0)
      val topicOffsetCount = PartitionOffsetCount(tp._3, now, response)
      topicOffsetCountMap += (tp._2 -> topicOffsetCountMap.get(tp._2).map(count => count.+:(topicOffsetCount)).getOrElse(Seq(topicOffsetCount)))
      tpOffsetCountMap += (TopicPartition(tp._2,tp._3)->OffsetCount(now,response))
    }
  }

  override def processQueryRequest(request: QueryRequest): Unit = {

    request match {
      case tmr: TMUpdateRequest =>
        updateProducerStats(tmr.brokerList, tmr.topicIdentities)
      case TMGetAll=>
        sender() ! TMGetAllResponse(tpOffsetCountMap)
      case any: Any => log.warning("tma : processQueryResponse : Received unknown message: {}", any)
    }

  }

  override def processCommandRequest(request: CommandRequest): Unit = ???

  override def processActorResponse(response: ActorResponse): Unit = ???

  implicit def queue2finitequeue[A](q: Queue[A]): FiniteQueue[A] = new FiniteQueue[A](q)
}

//
//object TopicMonitorActor extends App {
//  new TopicMonitorActor().getCurrentOffsetFor("localhost", 9092)
//}
