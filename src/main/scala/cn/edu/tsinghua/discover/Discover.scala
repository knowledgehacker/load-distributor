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

  private var master: Option[ActorRef] = None
  private var workers = Set[ActorRef]()

  override def receiveRecover = {
    case masterRR: MasterRegisterRequest =>
      master = Some(masterRR.master)
      println(s"recover - master: $master")

    case masterUnreg: MasterUnregister =>
      master = None
      println(s"recover - master: $master")

    case workerRR: WorkerRegisterRequest =>
      workers += workerRR.worker
      println(s"recover - workers: $workers")

    case workerUnreg: WorkerUnregister =>
      workers -= workerUnreg.worker
      println(s"recover - workers: $workers")
  }

  override def receiveCommand = {
    case masterRR: MasterRegisterRequest =>
      persist(masterRR) { masterRR =>
        registerMaster(masterRR)
      }

    case masterUnreg: MasterUnregister =>
      println(s"master ${masterUnreg.master} unregisters")
      persist(masterUnreg) { masterUnreg =>
        unregisterMaster(masterUnreg)
      }

    case MasterLookup =>
      if (master.isDefined) {
        sender() ! master.get
      }

    case workerRR: WorkerRegisterRequest =>
      persist(workerRR) { workerRR =>
        registerWorker(workerRR)
      }

    case workerUnreg: WorkerUnregister =>
      persist(workerUnreg) { workerUnreg =>
        unregisterWorker(workerUnreg)
      }

    case invalidMessage: Any =>
      println(s"invalid message - $invalidMessage")
  }

  private def registerMaster(masterRR: MasterRegisterRequest) {
    /*
     * When the master stops, Discover receives "MasterUnregister", then field "master" is set to None.
     * Thus no matter whether it is the first time for the master to register, field "master" changes from None to Some.
     */
    master = Some(masterRR.master)
    println(s"master ${master.get} registers")
    workers map {worker => worker ! master.get}

    master.get ! MasterRegisterReply
  }

  private def unregisterMaster(masterUnreg: MasterUnregister): Unit = {
    println(s"master ${masterUnreg.master} unregisters")
    master = None
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
