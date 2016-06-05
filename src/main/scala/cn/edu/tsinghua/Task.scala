package cn.edu.tsinghua

case class Task(timestamp: Long, file: String) {
  override def equals(o: Any) = o match {
    case that: Task => timestamp == that.timestamp && file.equals(that.file)
    case _ => false
  }
}

case class TaskResult(task: Task)
