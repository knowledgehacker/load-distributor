package cn.edu.tsinghua

object Messages {
  case object IdentityRequest
	case object IdentityReply

	case class RoundRequest(host: String)
  case class RoundReply(tasks: List[Task])
  case class RoundResult(host: String)
  case object RoundEnd
}
