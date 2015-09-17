package models.form

/**
 * Created by vinay.varma on 9/15/15.
 */
sealed trait ConsumerMonitorOperation

case class AddStormCluster(name:String,cluster:String,zkConnect:String,zkRoot:String)
