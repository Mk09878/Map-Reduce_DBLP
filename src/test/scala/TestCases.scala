import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.FunSuite

class TestCases extends FunSuite{

  val conf: Config = ConfigFactory.load("Config_File")

  def isNYears(ss: List[Int]): Boolean ={
    var temp = -1
    var counter = 0
    ss.foreach(x => {
      if(temp == -1){
        temp = x
        counter += 1
      }
      else if(counter == conf.getInt("Years")) return true
      else{
        if(x - temp == 1){
          counter += 1
        }
        else{
          counter = 0
        }
        temp = x
      }
    })
    false
  }

  def top100(): Int ={
    var counter = 0
    while (counter < 100){
      counter += 1
    }
    counter
  }

  def topTen(author_list: List[String]): List[String] ={
    val topTen = author_list.groupBy(identity).toList.sortBy(-_._2.size).take(10)
    val topTenAuthors = topTen.map { case (k, v) => s"$k" }
    topTenAuthors
  }

  test("Value is extracted correctly from config file"){
    val toTest = 10
    assert(conf.getInt("Years") == toTest)
  }

  test("Check if algorithm to check consecutive years is working"){
    assert(isNYears(List(2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010)))
  }

  test("Check if the loop stops at 100 while producing top 100 authors"){
    assert(top100() == 100)
  }

  test("Check if top ten authors are calcualted correctly"){
    val result: List[String] = topTen(List("a", "a", "a", "b", "b", "a", "c", "b"))
    assert(result.head.equals("a"))
    assert(result(1).equals("b"))
    assert(result(2).equals("c"))
  }

  test("getValues method in Mappers class is fetching the values correctly"){

    val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val input = "<inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Mark Grechanik</author>\n<author>B. M. Mainul Hossain</author>\n<author>Ugo Buy</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + input + "</dblp>"

    val authors = new JobMappers().getValues(xmlString,"author")
    assert(authors.nonEmpty)
    assert(authors.length == 3)
  }




}
