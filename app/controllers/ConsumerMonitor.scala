package controllers

import controllers.Cluster._
import models.FollowLink
import models.form.AddStormCluster
import play.api.data.Form
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.mvc.{Action, Controller}

import scala.concurrent.Future

/**
 * Created by vinay.varma on 9/15/15.
 */
object ConsumerMonitor extends Controller {

    import play.api.libs.concurrent.Execution.Implicits.defaultContext

  val addStormClusterForm = Form(
    mapping(
      "name" -> nonEmptyText.verifying(maxLength(250), validateName),
      "cluster" -> nonEmptyText.verifying(maxLength(250), validateName),
      "zkHosts" -> nonEmptyText.verifying(validateZkHosts),
      "zkRoot" -> nonEmptyText
    )(AddStormCluster.apply)(AddStormCluster.unapply)
  )
  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager

  def handleAddStormCluster() = Action.async { implicit request =>
    addStormClusterForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest),
      addStormCluster => {
        kafkaManager.addStormCluster(addStormCluster.name, addStormCluster.cluster, addStormCluster.zkConnect, addStormCluster.zkRoot).map(errOrSuccess=>
          Ok(
            views.html.common.resultOfCommand(
              views.html.navigation.defaultMenu(),
              models.navigation.BreadCrumbs.withView("Add Cluster"),
              errOrSuccess,
              "Add Cluster",
              FollowLink("Go to cluster view.",routes.ConsumerMonitor.getAllOffsets(addStormCluster.name).toString()),
              FollowLink("Try again.",routes.Cluster.addCluster().toString())
            )
          )
        )
      })
  }


  def addStormCluster=Action.async{
    Future.successful(Ok(views.html.consumer.addConsumerCluster(addStormClusterForm)))
  }

  def getAllClusters = Action.async {
    val resp = kafkaManager.getAllConsumers
    resp.map { errorOrSuccess =>
      if (errorOrSuccess.isDefined)
        Ok(views.html.consumer.consumerList(errorOrSuccess.get))
      else
        BadRequest
    }
  }

  def getAllOffsets(c: String) = Action.async {
    val response = kafkaManager.getAllOffsets(c)
    response.map { errorOrSuccess =>
      if (errorOrSuccess.isDefined)
        Ok(views.html.consumer.consumerOffsetView(c,errorOrSuccess.get))
      else
        BadRequest
    }
  }



}
