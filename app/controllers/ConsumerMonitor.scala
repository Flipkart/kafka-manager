package controllers

import controllers.Cluster.{validateName, _}
import kafka.manager.ActorModel.SSGetAllResponse
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

  def addStormCluster() = Action.async { implicit request =>
    addStormClusterForm.bindFromRequest.fold(
      formWithErrors => Future.successful(BadRequest),
      addStormCluster => {
        kafkaManager.addStormCluster(addStormCluster.name, addStormCluster.cluster, addStormCluster.zkConnect, addStormCluster.zkRoot)
        Future.successful(Ok)
      })
  }

  def getAllOffsets(c: String) = Action.async {
    val response = kafkaManager.getAllOffsets(c)
    response.map { errorOrSuccess =>
      if (errorOrSuccess.isDefined)
        Ok(SSGetAllResponse.serialize(errorOrSuccess.get))
      else
        Ok
    }
  }



}
