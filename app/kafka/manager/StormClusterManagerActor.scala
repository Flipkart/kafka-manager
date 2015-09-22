package kafka.manager

import java.nio.charset.StandardCharsets

import akka.actor.{Props, ActorPath}
import kafka.manager.ActorModel._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCache, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.zookeeper.CreateMode

import scala.util.{Success, Failure, Try}
import scala.collection.JavaConverters._

/**
 * Created by vinay.varma on 9/14/15.
 */
class StormClusterManagerActor(val cConfig: CuratorConfig, val bZkPath: String, topicMonitorActor: ActorPath) extends BaseQueryCommandActor with CuratorAwareActor with BaseZkPath {

  private[this] val consumerZkPath = zkPath("consumers")
  private[this] val consumersPathCache = new PathChildrenCache(curator, consumerZkPath, true)

  private[this] val consumersTreeCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      log.info(s"Got event : ${event.getType} path=${Option(event.getData).map(_.getPath)}")
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED | PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          self ! SCUpdateState
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
        case _ =>
      }

    }
  }

  private[this] var clusterConfigMap: Map[String, StormClusterConfig] = Map.empty
  private[this] var lastUpdateMillis: Long = 0L

  private[this] var spoutStateMap: Map[String, ActorPath] = Map.empty
  private[this] var spoutStateConfigMap: Map[String, SpoutStateConfig] = Map.empty

  @throws[Exception](classOf[Exception]) override
  def preStart(): Unit = {
    log.info("Started actor %s".format(self.path))
    log.info("Starting topology tree cache...")
    consumersPathCache.start()
    log.info("Adding topology tree cache listener...")
    consumersPathCache.getListenable.addListener(consumersTreeCacheListener)
  }

  override protected def curatorConfig: CuratorConfig = cConfig

  override def processActorResponse(response: ActorResponse): Unit = {
    response match {

      case any: Any => log.warning("scma : processActorResponse : Received unknown message: {}", any)
    }
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case request: SCGetCluster =>
        spoutStateMap.get(request.cluserName).fold[Unit] {
          sender ! ActorErrorResponse(s"Unknown cluster : ${request.cluserName}")
        } {
          ssPath: ActorPath =>
            context.actorSelection(ssPath).forward(request)
        }
      case SCGetAllClusters =>
        sender() ! SCGetAllClustersResponse(spoutStateMap.keySet.toList)
      case _ =>
        println("scma unknown query request")
    }

  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case SCAddCluster(stormClusterConfig) =>
        val data = StormClusterConfig.serialize(stormClusterConfig)
        val zkPath = zkPathFrom(consumerZkPath, stormClusterConfig.clusterName)
        sender() ! SCCommandResponse(
          Try {
            require(consumersPathCache.getCurrentData(zkPath) == null,
              s"Cluster already exists : ${stormClusterConfig.clusterName}")
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath, data)
          }
        )

      case SCUpdateState =>
        updateState()
      case _ =>
        println("Got unkown call to Storm cluster actor")
    }
  }

  override protected def baseZkPath: String = bZkPath

  private[this] def updateState(): Unit = {
    log.info("Updating internal state...")
    val result = Try {
      consumersPathCache.getCurrentData.asScala.foreach { data =>
        StormClusterConfig.deserialize(data.getData) match {
          case Failure(t) =>
            log.error("Failed to deserialize cluster config", t)
          case Success(newConfig) =>
            clusterConfigMap.get(newConfig.name).fold(addCluster(newConfig))(updateCluster(_, newConfig))
        }
      }
    }
    result match {
      case Failure(t) =>
        log.error("Failed to update internal state ... ", t)
      case _ =>
    }
    lastUpdateMillis = System.currentTimeMillis()
  }

  private[this] def addCluster(config: StormClusterConfig): Try[Boolean] = {
    Try {
      val spoutStateConfig = SpoutStateConfig(config.name, config.curatorConfig, config.stormZkRootNode)
      val props = Props(classOf[SpoutStateActor], spoutStateConfig, topicMonitorActor)
      val newSpoutStateActor = context.actorOf(props.withDispatcher("pinned-dispatcher"), config.name).path
      spoutStateConfigMap += (config.clusterName -> spoutStateConfig)
      spoutStateMap += (config.clusterName -> newSpoutStateActor)
      true
    }
  }

  private[this] def updateCluster(currentConfig: StormClusterConfig, newConfig: StormClusterConfig): Try[Boolean] = {
    Try {
      println("scm Empty call to update cluster")
      true
    }
  }
}


case class StormClusterConfig(name: String, clusterName: String, curatorConfig: CuratorConfig, stormZkRootNode: String)


object StormClusterConfig {

  import scalaz.{Failure, Success}
  import scalaz.syntax.applicative._
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization
  import org.json4s.scalaz.JsonScalaz._


  import scala.language.reflectiveCalls

  implicit val formats = Serialization.formats(FullTypeHints(List(classOf[ClusterConfig])))

  implicit def curatorConfigJSONW: JSONW[CuratorConfig] = new JSONW[CuratorConfig] {
    def write(a: CuratorConfig) =
      makeObj(("zkConnect" -> toJSON(a.zkConnect))
        :: ("zkMaxRetry" -> toJSON(a.zkMaxRetry))
        :: ("baseSleepTimeMs" -> toJSON(a.baseSleepTimeMs))
        :: ("maxSleepTimeMs" -> toJSON(a.maxSleepTimeMs))
        :: Nil)
  }

  implicit def curatorConfigJSONR: JSONR[CuratorConfig] = CuratorConfig.applyJSON(
    field[String]("zkConnect"), field[Int]("zkMaxRetry"), field[Int]("baseSleepTimeMs"), field[Int]("maxSleepTimeMs"))

  def serialize(stormClusterConfig: StormClusterConfig): Array[Byte] = {
    val json = makeObj(("name" -> toJSON(stormClusterConfig.name))
      :: ("curatorConfig" -> toJSON(stormClusterConfig.curatorConfig))
      :: ("clusterName" -> toJSON(stormClusterConfig.clusterName))
      :: ("stormZkRootNode" -> toJSON(stormClusterConfig.stormZkRootNode))
      :: Nil)
    compact(render(json)).getBytes(StandardCharsets.UTF_8)
  }

  def deserialize(ba: Array[Byte]): Try[StormClusterConfig] = {
    Try {
      val json = parse(kafka.manager.utils.deserializeString(ba))

      val result = (field[String]("name")(json) |@| field[CuratorConfig]("curatorConfig")(json) |@| field[String]("clusterName")(json) |@| field[String]("stormZkRootNode")(json)) {
        (name: String, curatorConfig: CuratorConfig, clusterName: String, stormZkRootNode: String) =>
          StormClusterConfig.apply(name, clusterName, curatorConfig, stormZkRootNode)
      }

      result match {
        case Failure(nel) =>
          throw new IllegalArgumentException(nel.toString())
        case Success(clusterConfig) =>
          clusterConfig
      }

    }
  }
}


