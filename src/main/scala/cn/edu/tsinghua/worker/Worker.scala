package cn.edu.tsinghua.worker

import java.util.concurrent.TimeUnit.{SECONDS => _, _}

import scala.collection.mutable.Set
import scala.concurrent.duration._
import com.typesafe.config.ConfigFactory
import akka.actor.{ActorLogging, ActorRef, ActorSelection, ActorSystem, OneForOneStrategy, Props, ReceiveTimeout, Terminated}
import akka.actor.SupervisorStrategy.{Stop}
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import cn.edu.tsinghua.{Messages, Task, TaskResult}

case class WorkerRegisterRequest(worker: ActorRef)
case object WorkerRegisterReply
case class WorkerUnregister(worker: ActorRef)

object Worker {

  def run(masterHostname: String, masterPort: Int, configFileName: String, args: Array[String]) = {
    val config = ConfigFactory.load(configFileName)
    val hostname = config.getString("akka.remote.netty.tcp.hostname")
    println(s"hostname: $hostname")
    val port = config.getString("akka.remote.netty.tcp.port")
    println(s"port: $port")

    val system: ActorSystem = ActorSystem("workerSys", config)
    val master: ActorSelection = system.actorSelection(s"akka.tcp://supervisorSys@$masterHostname:$masterPort/user/supervisor/master")
    println(s"master: $master")
    val worker = system.actorOf(Props(classOf[Worker], master, hostname), "worker")
  }
}

sealed trait Event
case class TaskAssignmentModified(id: Int, task: Task) extends Event

case class TaskAssignment(workeeNum: Int, private var taskAssignment: Array[Set[Task]], private var taskNum: Int) {

  for (i <- 0 until workeeNum) {
    taskAssignment(i) = Set[Task]()
  }

  def add(id: Int, task: Task) = {
    taskAssignment(id).add(task)
    taskNum += 1
  }

  def get(id: Int): Set[Task] = taskAssignment(id)

  def modify(event: TaskAssignmentModified) = {
    taskAssignment(event.id).remove(event.task)
    taskNum -= 1
  }

  def isEmpty(): Boolean = {
    taskNum == 0
  }
}

class Worker(private var master: ActorSelection, private val hostname: String) extends PersistentActor with ActorLogging {
  import Messages._
  import context._

  override def persistenceId = "worker"

  private val workeeNum: Int = 2

  private val workees: Array[ActorRef] = new Array[ActorRef](workeeNum)
  for (i <- 0 until workeeNum) {
    workees(i) = createAndWatchWorkee(i)
  }

  private var taskAssignment: TaskAssignment = TaskAssignment(workeeNum, new Array[Set[Task]](workeeNum), 0)

  override def supervisorStrategy = OneForOneStrategy(
    maxNrOfRetries = 5,
    withinTimeRange = Duration.create(60, SECONDS)) {
    /*
     * TODO: Resume for some exceptions, and just Stop for other exceptions.
     * Restart will retain messages not processed in the mailbox, which is not what we expect.
     */
    case _: Exception => Stop
  }

  override def postStop(): Unit = {
    println(s"worker $self unregistering")
    master ! WorkerUnregister
  }

  val receiveTimeout = 1 second

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
      println(s"worker $self registering")
      master ! WorkerRegisterRequest

    case SnapshotOffer(_, snapshot: TaskAssignment) => {
      println("SnapshotOffer")
      taskAssignment = snapshot
    }

    case event: TaskAssignmentModified => taskAssignment.modify(event)
  }

  override def receiveCommand = {
    case masterActor: ActorRef =>
      println(s"master changes from $master to $masterActor")
      master = context.actorSelection(masterActor.path)

    case WorkerRegisterReply =>
      println(s"worker $self registered")
      if (taskAssignment.isEmpty) {
        println(s"receiveRecover - task assignment is empty")
        master ! RoundRequest(hostname)
        setReceiveTimeout(receiveTimeout)
      }

    case RoundReply(taskList) =>
      setReceiveTimeout(Duration.Undefined)
      for (i <- 0 until taskList.size) {
        taskAssignment.add(i % workeeNum, taskList(i))
      }
      saveSnapshot(taskAssignment)

      for (i <- 0 until workeeNum) {
        val workee = workees(i)
        taskAssignment.get(i) foreach { task => workee ! task}
      }

    case TaskResult(id, task) =>
      println(s"task $task done")
      persist(TaskAssignmentModified(id, task)) { event =>
        taskAssignment.modify(event)

        if (taskAssignment.isEmpty) {
          println("all tasks for this round done")
          master ! RoundResult(hostname)

          // next round
          master ! RoundRequest(hostname)
          setReceiveTimeout(receiveTimeout)
        }
      }

    case ReceiveTimeout =>
      println(s"$self retries round request")
      master ! RoundRequest(hostname)
      setReceiveTimeout(receiveTimeout)

    case RoundEnd =>
      println("round end")

    case Terminated(workee) =>
      unwatch(workee)

      for (i <- 0 until workeeNum) {
        if (workees(i) == workee) {
          val newWorkee = createAndWatchWorkee(i)
          log.warning(s"Workee $workee terminated, replace it $newWorkee.")
          taskAssignment.get(i) foreach { task => newWorkee ! task}

          workees(i) = newWorkee
        }
      }
  }

  private def createAndWatchWorkee(id: Int): ActorRef = {
    val workee = context.actorOf(Props(classOf[Workee], id))
    println(s"workee: $workee")
    context watch workee

    workee
  }
}
