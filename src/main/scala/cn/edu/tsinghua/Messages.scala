package cn.edu.tsinghua

import akka.actor.ActorRef

object Messages {
  case class IdentityRequest(worker: ActorRef)
	case object IdentityReply

	case object TaskRequest
  case class TaskReply(fileAndLocation: FileAndLocation, master: ActorRef)
  case class TaskExhuasted(master: ActorRef)
  case class TaskResult(fileAndLocation: FileAndLocation)
}
