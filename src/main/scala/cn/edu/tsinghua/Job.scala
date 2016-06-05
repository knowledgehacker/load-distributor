package cn.edu.tsinghua

import java.io.FileReader
import java.io.BufferedReader

import scala.collection.mutable.MutableList
import scala.collection.mutable.Map

object Job {

  def get(fileListLocation: String): Job = {
    val filesByLocation = Map[String, MutableList[String]]()

    val bufferedReader = new BufferedReader(new FileReader(fileListLocation))
    var line: String = bufferedReader.readLine
    while (line != null) {
      val fileLocation: Array[String] = line.split("\t")
      val file = fileLocation(0)
      val location = fileLocation(1)
      println(s"line: $line, file: $file, location: $location")

      filesByLocation.get(location) match {
        case Some(files: MutableList[String]) => files += file
        case None => filesByLocation += (location -> MutableList(file))
      }

      line = bufferedReader.readLine
    }

    /*
    filesByLocation foreach { case(location: String, files: MutableList[String]) =>
      print(s"location: $location => ")
      files foreach {file =>
      print(s"$file, ")}
      println()
    }
    */

    new Job(-1L, filesByLocation) // TODO: change timestamp from "-1L" to real timestamp
  }
}

class Job(val timestamp: Long, val filesByLocation: Map[String, MutableList[String]]) {
  val iterator = filesByLocation.iterator

  def getFiles(location: String): MutableList[String] = {
    //filesByLocation(location)
    iterator.next()._2
  }

  def isEmpty(): Boolean = !iterator.hasNext
}