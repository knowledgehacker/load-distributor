package cn.edu.tsinghua

import scala.collection.mutable.MutableList

import akka.actor.ActorRef

object Messages {
  case class IdentityRequest(worker: ActorRef)
	case object IdentityReply

	case class RoundRequest(host: String)
  case class RoundReply(tasks: MutableList[Task], master: ActorRef)
  case class RoundResult(host: String)
  case class RoundEnd(master: ActorRef)
}
