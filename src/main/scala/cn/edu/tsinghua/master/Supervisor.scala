package cn.edu.tsinghua.master

import java.util.concurrent.TimeUnit.SECONDS
import scala.concurrent.duration.Duration

import com.typesafe.config.ConfigFactory

import akka.actor.Props
import akka.actor.ActorSystem
import akka.actor.ActorSelection
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Restart

case object Launch

object Supervisor {

  def run(discoverHostname: String, discoverPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    println(s"hostname: ${config.getString("akka.remote.netty.tcp.hostname")}")
    println(s"port: ${config.getString("akka.remote.netty.tcp.port")}")

    val system: ActorSystem = ActorSystem("supervisorSys", config)
    println(s"system: $system")
    val supervisor = system.actorOf(Props(classOf[Supervisor], discoverHostname, discoverPort), "supervisor")
    supervisor ! Launch
  }
}

class Supervisor(discoverHostname: String, discoverPort: Int) extends Actor with ActorLogging {
import context._

  override def supervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 5,
    withinTimeRange = Duration.create(60, SECONDS)) {
    case _: Exception => Restart
  }

  def receive = {
    case Launch =>
      val discover: ActorSelection = system.actorSelection(s"akka.tcp://discoverSys@$discoverHostname:$discoverPort/user/discover")
      println(s"discover: $discover")
      val master = system.actorOf(Props(classOf[Master], discover), "master")
  }
}
