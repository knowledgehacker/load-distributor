package cn.edu.tsinghua

import java.io.FileReader
import java.io.BufferedReader

case class FileAndLocation(file: String, location: String) {
  override def toString(): String = s"file: $file, location: $location"
}

object Job {

  def get(fileListLocation: String): Job = {
    var fileAndLocations = List[FileAndLocation]()

    val bufferedReader = new BufferedReader(new FileReader(fileListLocation))
    var line: String = bufferedReader.readLine
    while (line != null) {
      fileAndLocations = fileAndLocations :+ FileAndLocation(line, "local") // TODO: change location from "local" to real server
      line = bufferedReader.readLine
    }

    new Job(-1L, fileAndLocations) // TODO: change timestamp from "-1L" to real timestamp
  }
}

class Job(val timestamp: Long, var fileAndLocations: List[FileAndLocation]) {

  def isEmpty(): Boolean = fileAndLocations.isEmpty
}