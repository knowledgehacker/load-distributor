package cn.edu.tsinghua.worker

import akka.actor.Actor
import akka.actor.ActorLogging

import cn.edu.tsinghua.{Task, TaskResult}

class Workee(id: Int) extends Actor with ActorLogging {

  override def postStop(): Unit = {
    println(s"workee $self stopping")
  }

  def receive = {
    case task: Task =>
      if (!isDuplicated(task)) {
        processTask(task)
        sender() ! TaskResult(id, task)
      }
  }

  def processTask(task: Task) = {
    log.info(s"process file ${task.file} starts.")

    // test
    context stop self

    log.info(s"process file ${task.file} finishes.")
  }

  private def isDuplicated(task: Task): Boolean = {
    false
  }
}
