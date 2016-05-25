package cn.edu.tsinghua.worker

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.ActorSelection
import akka.actor.ReceiveTimeout
import akka.actor.ActorLogging

import cn.edu.tsinghua.{FileAndLocation, Messages}

case object WorkerInit
case class WorkerRegister(worker: ActorRef)
case class WorkerUnregister(worker: ActorRef)

object Worker {

  def run(discoverHostname: String, discoverPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    println(s"hostname: ${config.getString("akka.remote.netty.tcp.hostname")}")
    println(s"port: ${config.getString("akka.remote.netty.tcp.port")}")

    // TODO: can we create an ActorSystem for all workers in Supervisor and send it to the worker here, instead of create an ActorSystem for each worker?
    val system: ActorSystem = ActorSystem("workerSys", config)
    println(s"system: $system")
    val discover: ActorSelection = system.actorSelection(s"akka.tcp://discoverSys@$discoverHostname:$discoverPort/user/discover")
    println(s"discover: $discover")
    val worker = system.actorOf(Props(classOf[Worker], discover), "worker")
  }
}

class Worker(discover: ActorSelection) extends Actor with ActorLogging {
  import Messages._
  import context._

  private var master: ActorRef = null

  override def preStart = {
    self ! WorkerInit
  }

  override def postStop = {
    discover ! WorkerUnregister(self)
  }

  val receiveTimeout = 1 second

  def receive = init
  def init: Receive = {
    case WorkerInit =>
      println(s"$self registering")
      discover ! WorkerRegister(self)
      setReceiveTimeout(receiveTimeout)

    case ReceiveTimeout =>
      println(s"$self retries registering")
      discover ! WorkerRegister(self)
      setReceiveTimeout(receiveTimeout)

    case m: ActorRef =>
      setReceiveTimeout(Duration.Undefined)

      println(s"$self registered - master: $m")
      master = m
      watch(master)

      master ! IdentityRequest(self)
      become(working)
  }

  def working: Receive = {

    case m: ActorRef =>
      println("master changes from $master to $m")
      master = m

    case IdentityReply =>
      master ! TaskRequest

    case TaskReply(fileAndLocation: FileAndLocation, master) =>
      processTask(fileAndLocation)
      master ! TaskResult(fileAndLocation)
      master ! TaskRequest

    case TaskExhuasted(master) =>
      /*
      // keep asking for new task periodically(every  1s)
      println("no more task, sleep 1s.")
      Thread.sleep(1000L)
      master ! TaskRequest
      */

    case Terminated(master) =>
      log.error("Master terminated, stops watching it.")
      unwatch(master)
  }

  def processTask(fileAndLocation: FileAndLocation) = {
    // TODO: do actual work
    log.info(s"File ${fileAndLocation.file} downloaded from server ${fileAndLocation.location}.")
  }
}
