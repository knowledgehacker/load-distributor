package cn.edu.tsinghua.worker

import akka.actor.Actor
import akka.actor.ActorLogging

import cn.edu.tsinghua.{Task, TaskResult}

class Workee extends Actor with ActorLogging {

  // TODO: handle the failures except restart, such as stop

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    // TODO: recover work done by handling message "message"
  }

  override def postRestart(reason: Throwable) = {
    // TODO: reprocess the message which causes the restart
  }

  def receive = {
    case task: Task =>
      processTask(task)
      sender() ! TaskResult(task)
  }

  def processTask(task: Task) = {
    // TODO: do actual work
    log.info(s"process file ${task.file}.")
  }

}
