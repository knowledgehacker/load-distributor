package cn.edu.tsinghua

import scala.collection.mutable.MutableList

import akka.actor.ActorRef

object Messages {
  case object IdentityRequest
	case object IdentityReply

	case class RoundRequest(host: String)
  case class RoundReply(tasks: MutableList[Task])
  case class RoundResult(host: String)
  case object RoundEnd
}
