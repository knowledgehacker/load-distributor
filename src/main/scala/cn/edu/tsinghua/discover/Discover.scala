package cn.edu.tsinghua.discover

import com.typesafe.config.ConfigFactory

import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.ActorLogging

import cn.edu.tsinghua.master.{MasterRegister, MasterUnregister}
import cn.edu.tsinghua.worker.{WorkerRegister, WorkerUnregister}

object Discover {

  def run(discoverHostname: String, discoverPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    println(s"hostname: ${config.getString("akka.remote.netty.tcp.hostname")}")
    println(s"port: ${config.getString("akka.remote.netty.tcp.port")}")

    val system: ActorSystem = ActorSystem("discoverSys", config)
    println(s"system: $system")
    val discover = system.actorOf(Props[Discover], "discover")
  }
}

class Discover extends Actor with ActorLogging {

  // TODO: we need to persist these states and recover them upon discover fails and restarts
  private var oldMaster: Option[ActorRef] = None
  private var master: Option[ActorRef] = None
  private var workers = Set[ActorRef]()

  def receive = {
    case masterReg: MasterRegister =>
      // master is always None when discover actor receives message "masterReg"
      master = Some(masterReg.master)
      if (!oldMaster.isDefined) {
        println(s"master $master registers")
      }
      else {
        println(s"master changes from $oldMaster to $master")
        workers.map(worker => worker ! master)
      }
      oldMaster = master

    case workerReg: WorkerRegister =>
      if (master.isDefined) {
        val worker = workerReg.worker
        println(s"worker $worker registers")
        worker ! master.get

        workers += worker
      }

    case masterUnreg: MasterUnregister =>
      println(s"master ${masterUnreg.master} unregisters")
      master = None

    case workerUnreg: WorkerUnregister =>
      val worker = workerUnreg.worker
      println(s"worker $worker unregisters")
      workers -= worker

    case invalidMessage: Any =>
      println(s"invalid message - $invalidMessage")
  }
}
