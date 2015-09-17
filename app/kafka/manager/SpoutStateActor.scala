package kafka.manager

import akka.actor.ActorPath
import akka.pattern._
import akka.util.Timeout
import kafka.manager.ActorModel._
import org.apache.curator.framework.recipes.cache.TreeCache
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.Try

/**
 * Created by vinay.varma on 9/16/15.
 */
class SpoutStateActor(spoutStateConfig: SpoutStateConfig, topicMonitorActor: ActorPath) extends BaseQueryCommandActor with CuratorAwareActor with BaseZkPath {

  val spoutTreeCache = new TreeCache(curator, spoutStateConfig.zkRoot)

  @throws[Exception](classOf[Exception]) override
  def preStart(): Unit = {
    spoutTreeCache.start()
  }

  private[this] implicit val apiTimeout: Timeout = FiniteDuration(
    4000,
    MILLISECONDS
  )
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case re: SCGetCluster =>
        val s=sender()
        val spouts = spoutTreeCache.getCurrentChildren(spoutStateConfig.zkRoot).keySet()
        val spoutPartitionMap = spouts.map(s => zkPath(s)).flatMap(r => spoutTreeCache.getCurrentChildren(r).keySet().map(p => (r, p)))
        val futureTopicsMap = context.actorSelection(topicMonitorActor)
          .ask(TMGetAll)
          .mapTo[TMGetAllResponse]

        val consumerOffsets = spoutPartitionMap
          .map(sp => zkPathFrom(sp._1, sp._2))
          .map(r => spoutTreeCache.getCurrentData(r).getData)
          .map(OffsetResponse.deserialize).map(or => or.get.getConsumerOffset)

        val f=futureTopicsMap.map(tmgar => {
          val tpMap = tmgar.topicPartionCountMap
          val res = consumerOffsets.map(co => {
            tpMap.get(TopicPartition(co.topic, co.partition)).fold(ConsumerLatency(co.name, co.topic, co.offset, 0L, co.partition))(oc => ConsumerLatency(co.name, co.topic, co.offset, oc.count, co.partition))
          })
           SSGetAllResponse(res.toSet)
        }) pipeTo sender()

      case _ =>
        println("ssa unkown query request")
    }

  }

  override def processCommandRequest(request: CommandRequest): Unit = ???

  override def processActorResponse(response: ActorResponse): Unit = ???

  override protected def curatorConfig: CuratorConfig = spoutStateConfig.curatorConfig

  override protected def baseZkPath: String = spoutStateConfig.zkRoot
}

case class ConsumerLatency(consumerName: String, topic: String, consumerOffset: Long, producerOffset: Long, partition: Int)

case class SpoutStateConfig(name: String, curatorConfig: CuratorConfig, zkRoot: String)

case class Topology(id: String, name: String)

case class Broker(host: String, port: Int)

case class OffsetResponse(topology: Topology, offset: Long, partition: Int, broker: Broker, topic: String) {
  def getConsumerOffset: SpoutConsumerOffset = SpoutConsumerOffset(topic, offset, partition, topology.name)
}

case class SpoutConsumerOffset(topic: String, offset: Long, partition: Int, name: String)

object OffsetResponse {

  import scalaz.syntax.applicative._
  import scalaz.{Failure, Success}
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.scalaz.JsonScalaz._
  import scala.language.reflectiveCalls

  implicit val formats = Serialization.formats(FullTypeHints(List(classOf[ClusterConfig])))

  implicit def topologyJSONR: JSONR[Topology] = Topology.applyJSON(
    field[String]("id"), field[String]("name"))

  implicit def brokerJSONR: JSONR[Broker] = Broker.applyJSON(
    field[String]("host"), field[Int]("port"))


  def deserialize(ba: Array[Byte]): Try[OffsetResponse] = {
    Try {
      val json = parse(kafka.manager.utils.deserializeString(ba))
      val result = (field[Topology]("topology")(json) |@| field[Long]("offset")(json) |@| field[Int]("partition")(json) |@| field[Broker]("broker")(json) |@| field[String]("topic")(json)) {
        (topology: Topology, offset: Long, partition: Int, broker: Broker, topic: String) =>
          OffsetResponse.apply(topology, offset, partition, broker, topic)
      }

      result match {
        case Failure(nel) =>
          throw new IllegalArgumentException(nel.toString())
        case Success(offsetResponse) =>
          offsetResponse
      }

    }
  }

}



