package cn.edu.tsinghua

import cn.edu.tsinghua.master.Supervisor
import cn.edu.tsinghua.worker.Worker

object Main {

  def main(args: Array[String]) {
    val masterHostname: String = System.getProperty("master.hostname")
    val masterPort: String = System.getProperty("master.port")
    if (masterHostname == null || masterPort == null) {
      System.err.println("Master host name or port is not specified, exit.")
      System.exit(-1)
    }

    val configFileNameWithExtension = System.getProperty("config.resource")
    val configFileName = configFileNameWithExtension.substring(1, configFileNameWithExtension.indexOf('.'))
    if (configFileName.compareTo("master") == 0) {
      Supervisor.run(masterHostname, Integer.parseInt(masterPort), configFileName, args)
    }
    else if (configFileName.compareTo("worker") == 0) {
      Worker.run(masterHostname, Integer.parseInt(masterPort), configFileName, args)
    }
    else {

    }
  }
}
