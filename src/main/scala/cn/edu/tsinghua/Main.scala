package cn.edu.tsinghua

import cn.edu.tsinghua.discover.Discover
import cn.edu.tsinghua.master.Supervisor
import cn.edu.tsinghua.worker.Worker

object Main {

  def main(args: Array[String]) {
    val discoverHostname: String = System.getProperty("discover.hostname")
    val discoverPort: String = System.getProperty("discover.port")
    if (discoverHostname == null || discoverPort == null) {
      System.err.println("Discover host name or port is not specified, exit.")
      System.exit(-1)
    }

    val configFileNameWithExtension = System.getProperty("config.resource")
    val configFileName = configFileNameWithExtension.substring(1, configFileNameWithExtension.indexOf('.'))
    if (configFileName.compareTo("discover") == 0) {
      Discover.run(discoverHostname, Integer.parseInt(discoverPort), configFileName, args)
    }
    else if (configFileName.compareTo("master") == 0) {
      Supervisor.run(discoverHostname, Integer.parseInt(discoverPort), configFileName, args)
    }
    else if (configFileName.compareTo("worker") == 0) {
      Worker.run(discoverHostname, Integer.parseInt(discoverPort), configFileName, args)
    }
    else {

    }
  }
}
