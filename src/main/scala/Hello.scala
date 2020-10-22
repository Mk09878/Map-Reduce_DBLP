import scala.collection.mutable
import scala.util.control.Breaks._

object Hello extends App {
  /*println("Hello World")
  val weather =
    <rss>
      <channel>
        <title>Yahoo! Weather - Boulder, CO</title>
        <item>
          <title>Conditions for Boulder, CO at 2:54 pm MST</title>
          <forecast day="Thu" date="10 Nov 2011" low="37" high="58" text="Partly Cloudy"
                    code="29" />
        </item>
      </channel>
    </rss>

  val forecast = weather \ "channel" \ "item" \ "forecast"

  var a = Vector("a", "b", "c")
  println(a.foldLeft("")((a, b) => a + b + " | "))

  var b =  new mutable.HashSet[Int]()
  b.add(1)
  b.add(2)
  b.add(3)
  println(b.toString())
  val strarr = b.toString().split(",")
  strarr.foreach(strar => println(strar))*/

  var c = List(1, 2, 3)
  println(c.mkString("|"))
  val strarr1 = c.mkString("|").split("|")
  strarr1.foreach(strar => println(strar))

  for(a <- 1 to 10){
    if(a > 5)
      break
    println(a)
  }
}
