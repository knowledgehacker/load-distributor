package cn.edu.tsinghua.master

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Terminated}
import cn.edu.tsinghua.{FileAndLocation, Job, Messages}

case object MasterInit
case class MasterRegister(master: ActorRef)
case class MasterUnregister(master: ActorRef)

class Master(discover: ActorSelection) extends Actor with ActorLogging {
  import Messages._
  import context._

  var job: Job = null

  override def preStart = {
    job = Job.get("log/file.txt") // TODO: remove hard code here

    self ! MasterInit // we need to register when the actor is created and started
  }

  override def postRestart(reason: Throwable) = {
    /*
     * The default implementation of postRestart(on new instance) calls preStart hook, which is not what we expect.
     * So we override postRestart here to ensure it does not call preStart hook.
     */
  }

  override def postStop = {
    discover ! MasterUnregister(self) // we need to unregister when the actor is terminated
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
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
      discover ! MasterRegister(self) // register here instead of in preStart to ensure register after the actor started

    case IdentityRequest(worker) =>
      println(s"actor with path ${worker.path} identifies itself")
      worker ! IdentityReply
      watch(worker)

    case TaskRequest =>
      if(job.isEmpty) {
        sender() ! TaskExhuasted(self)
      }
      else {
        val fileAndLocations = job.fileAndLocations

        val fileAndLocation = fileAndLocations.head
        println(s"next task - $fileAndLocation")
        sender() ! TaskReply(fileAndLocation, self)

        job.fileAndLocations = fileAndLocations.tail
      }

    case TaskResult(fileAndLocation: FileAndLocation) =>
      if(job.isEmpty) {
        /*
        println("getJob")
        job = Job.get("log/file.txt") // TODO: remove hard code here
        */

        println("done")
      }

    case Terminated(worker) =>
      log.info("Worker $worker terminated, stops watching it")
      unwatch(worker)
  }
}