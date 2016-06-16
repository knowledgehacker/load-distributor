package cn.edu.tsinghua

case class Task(time: String, file: String) {
  override def equals(o: Any) = o match {
    case that: Task => time.equals(that.time) && file.equals(that.file)
    case _ => false
  }
}

case class TaskResult(id: Int, task: Task)
