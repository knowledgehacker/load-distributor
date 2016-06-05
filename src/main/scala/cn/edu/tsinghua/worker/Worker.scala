package cn.edu.tsinghua.worker

import scala.collection.mutable.Set
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ReceiveTimeout
import akka.actor.Terminated

import akka.routing.ActorRefRoutee
import akka.routing.Router
import akka.routing.RoundRobinRoutingLogic

import cn.edu.tsinghua.{Messages, Task, TaskResult}

case object LookupMaster
case class MasterLookup(worker: ActorRef)
case class WorkerRegisterRequest(worker: ActorRef)
case object WorkRegisterReply
case class WorkerUnregister(worker: ActorRef)

object Worker {

  def run(discoverHostname: String, discoverPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    val hostname = config.getString("akka.remote.netty.tcp.hostname")
    println(s"hostname: $hostname")
    val port = config.getString("akka.remote.netty.tcp.port")
    println(s"port: $port")

    val system: ActorSystem = ActorSystem("workerSys", config)
    val discover: ActorSelection = system.actorSelection(s"akka.tcp://discoverSys@$discoverHostname:$discoverPort/user/discover")
    println(s"discover: $discover")
    val worker = system.actorOf(Props(classOf[Worker], discover, hostname), "worker")
  }
}

class Worker(discover: ActorSelection, hostname: String) extends Actor with ActorLogging {
  import Messages._
  import context._

  private var master: ActorRef = null

  // TODO: do we need to persist "tasks" here to handle restart???
  private val tasks: Set[Task] = Set[Task]()

  override def preStart = {
    self ! LookupMaster // looks up master upon start or restart
  }

  override def postStop = {
    discover ! WorkerUnregister(self)
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

  var poolRouter = {
    val routees = Vector.fill(2) {
      val r = context.actorOf(Props[Workee])
      println(s"routee: $r")
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  val receiveTimeout = 1 second

  def receive = init
  def init: Receive = {
    case LookupMaster =>
      println(s"$self looking up master")
      discover ! MasterLookup(self)
      setReceiveTimeout(receiveTimeout)

    case ReceiveTimeout =>
      println(s"$self retries looking up master")
      discover ! MasterLookup(self)
      setReceiveTimeout(receiveTimeout)

    case m: ActorRef =>
      setReceiveTimeout(Duration.Undefined)

      println(s"master: $m")
      master = m
      watch(master)

      println(s"$self registering")
      discover ! WorkerRegisterRequest(self)

    case WorkRegisterReply =>
      println(s"$self registered")
      master ! IdentityRequest(self)
      become(working)
  }

  def working: Receive = {
    case m: ActorRef =>
      println("master changes from $master to $m")
      master = m
      watch(master)

    case IdentityReply =>
      // TODO: do not request tasks of next round immediately after restart, instead should wait for the works of current round to finish

      // TODO: check what will happen if master does not exist if RoundRequest is sent???
      master ! RoundRequest(hostname)
      setReceiveTimeout(receiveTimeout)

    case RoundReply(taskList, master) =>
      setReceiveTimeout(Duration.Undefined)
      tasks ++= taskList
      tasks foreach {task => poolRouter.route(task, self)}

    case TaskResult(task) =>
      println(s"task $task done")
      tasks -= task
      if (tasks.isEmpty) {
        println("all tasks for this round done")
        master ! RoundResult(hostname)
      }

      // next round
      master ! RoundRequest(hostname)
      setReceiveTimeout(receiveTimeout)

    case ReceiveTimeout =>
      println(s"$self retries round request")
      master ! RoundRequest(hostname)
      setReceiveTimeout(receiveTimeout)

    case RoundEnd(master) =>
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
}
