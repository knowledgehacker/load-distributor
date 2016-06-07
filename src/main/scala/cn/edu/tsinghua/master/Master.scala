package cn.edu.tsinghua.master

import scala.collection.mutable.{Queue, MutableList}

import akka.actor.{Actor, ActorRef, ActorLogging, Terminated}

import cn.edu.tsinghua.{Work, Task, Messages}

case object MasterInit
case class MasterRegisterRequest(master: ActorRef)
case object MasterRegisterReply
case class MasterUnregister(master: ActorRef)

class Master(discoverHostname: String, discoverPort: Int) extends Actor with ActorLogging {
  import Messages._
  import context._

  var time: String = null
  var works = Queue[Work]()

  val discover = system.actorSelection(s"akka.tcp://discoverSys@$discoverHostname:$discoverPort/user/discover")
  println(s"discover: $discover")

  override def preStart = {
    self ! MasterInit
  }

  override def postRestart(reason: Throwable) = {
    /*
     * The default implementation of postRestart(on new instance) calls preStart hook, which is not what we expect.
     * So we override postRestart here to ensure it does not call preStart hook.
     */
  }

  // TODO: when I kill actor "Master" with Ctrl+C, this hook is not called. How to handle this???
  override def postStop = {
    println(s"$self unregistering")
    discover ! MasterUnregister(self) // we need to unregister when the actor is terminated
  }

  override def preRestart(reason: Throwable, message: Option[Any]) = {
    context.children foreach { child ⇒
      context.unwatch(child)
      context.stop(child)
    }

    /*
     * The default implementation of preRestart(on old instance) calls postStop hook, which is not what we expect.
     * So we override preRestart here to ensure it does not call postStop hook.
     */
  }

  def receive = {
    case MasterInit =>
      println(s"$self registering")
      discover ! MasterRegisterRequest(self) // register here instead of in preStart to ensure register after the actor started

    case MasterRegisterReply =>
      println(s"$self registered")

    case tm: String =>
      time = tm

    case work: Work =>
      works.enqueue(work)

    case IdentityRequest =>
      val worker = sender()
      println(s"actor with path ${worker.path} identifies itself")
      worker ! IdentityReply
      watch(worker)

    case RoundRequest(host) =>
      if(works.isEmpty) {
        sender() ! RoundEnd
      }
      else {
        val tasks = MutableList[Task]()
        val work = works.dequeue
        work.files foreach {file => tasks += Task(time, file)}
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