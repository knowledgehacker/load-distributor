package cn.edu.tsinghua.master

import scala.collection.mutable.{MutableList, Queue}
import akka.actor.{Actor, ActorLogging, Terminated}
import cn.edu.tsinghua.worker.{WorkerRegisterReply, WorkerRegisterRequest, WorkerUnregister}
import cn.edu.tsinghua.{Messages, Task, Work}

final class Works(private var time: String = null, val workQueue: Queue[Work] = Queue[Work]()) {
  def getTime: String = time

  def setTime(time: String) = this.time = time

  def add(work: Work) = {
    workQueue.enqueue(work)
  }

  def remove(): Work = {
    workQueue.dequeue()
  }

  def isEmpty(): Boolean = {
    workQueue.isEmpty
  }
}

class Master(discoverHostname: String, discoverPort: Int) extends Actor with ActorLogging {
  import Messages._
  import context._

  val works: Works = new Works

  def receive = {
    case time: String =>
      works.setTime(time)

    case work: Work =>
      works.add(work)

    case WorkerRegisterRequest =>
      val worker = sender()
      println(s"actor with path ${worker.path} registers")
      worker ! WorkerRegisterReply
      watch(worker)

    case WorkerUnregister =>
      val worker = sender()
      unwatch(worker)

    case RoundRequest(host) =>
      if(works.isEmpty) {
        sender() ! RoundEnd
      }
      else {
        val tasks = MutableList[Task]()
        val work = works.remove
        work.files foreach {file => tasks += Task(works.getTime, file)}
        sender() ! RoundReply(tasks.toList)
      }

    case RoundResult(host) =>
      if(works.isEmpty) {
        println("done")
      }

    case Terminated(worker) =>
      log.info("Worker $worker terminated, stops watching it")
      unwatch(worker)
  }
}