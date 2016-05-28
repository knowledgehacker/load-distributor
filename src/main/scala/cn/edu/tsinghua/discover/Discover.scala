package cn.edu.tsinghua.discover

import com.typesafe.config.ConfigFactory

import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.persistence.PersistentActor

import cn.edu.tsinghua.master.{MasterRegisterRequest, MasterRegisterReply, MasterUnregister}
import cn.edu.tsinghua.worker.{MasterLookup, WorkerRegisterRequest, WorkRegisterReply, WorkerUnregister}

object Discover {
  case class MasterState(oldMaster: Option[ActorRef], master: Option[ActorRef])

  def run(discoverHostname: String, discoverPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    println(s"hostname: ${config.getString("akka.remote.netty.tcp.hostname")}")
    println(s"port: ${config.getString("akka.remote.netty.tcp.port")}")

    val system: ActorSystem = ActorSystem("discoverSys", config)
    val discover = system.actorOf(Props[Discover], "discover")
  }
}

class Discover extends PersistentActor with ActorLogging {
  override def persistenceId = "discover-persistence-id"

  private var oldMaster: Option[ActorRef] = None
  private var master: Option[ActorRef] = None
  private var workers = Set[ActorRef]()

  override def receiveRecover = {
    case masterState: Discover.MasterState =>
      updateMaster(masterState)

    case workerRR: WorkerRegisterRequest =>
      registerWorker(workerRR)

    case workerUnreg: WorkerUnregister =>
      unregisterWorker(workerUnreg)
  }

  override def receiveCommand = {
    case masterRR: MasterRegisterRequest =>
      val masterState = Discover.MasterState(oldMaster, Some(masterRR.master))
      persist(masterState) { masterState =>
        updateMaster(masterState)
      }

    case masterUnreg: MasterUnregister =>
      println(s"master ${masterUnreg.master} unregisters")
      val masterState = Discover.MasterState(oldMaster, None)
      persist(masterState) { masterState =>
        updateMaster(masterState)
      }

    case masterLookup: MasterLookup =>
      if (master.isDefined) {
        masterLookup.worker ! master.get
      }

    case workerReg: WorkerRegisterRequest =>
      persist(workerReg) { workerReg =>
        registerWorker(workerReg)
      }

    case workerUnreg: WorkerUnregister =>
      persist(workerUnreg) { workerUnreg =>
        unregisterWorker(workerUnreg)
      }

    case invalidMessage: Any =>
      println(s"invalid message - $invalidMessage")
  }

  private def updateMaster(masterState: Discover.MasterState) {
    // master is always None when discover actor receives message "masterReg"
    master = masterState.master
    if (!oldMaster.isDefined) {
      println(s"master ${master.get} registers")
    }
    else {
      println(s"master changes from $oldMaster to $master")
      workers.map(worker => worker ! master)
    }
    oldMaster = master

    master.get ! MasterRegisterReply
  }

  private def registerWorker(workerRR: WorkerRegisterRequest) {
    val worker = workerRR.worker
    println(s"worker $worker registers")
    workers += worker

    worker ! WorkRegisterReply
  }

  private def unregisterWorker(workerUnreg: WorkerUnregister) {
    val worker = workerUnreg.worker
    println(s"worker $worker unregisters")
    workers -= worker
  }
}
