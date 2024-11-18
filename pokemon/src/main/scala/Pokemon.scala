import scala.io.Source


object Pokemon {
  def main(args: Array[String]): Unit = {
    val lines = Source.fromFile("../../data/pokemon.csv").getLines.toList
    println(lines.head)
  }
}


def getHeaderContent(l: List[String]): (Array[String], List[Array[String]]) = {
    val header = l.head
    val content = l.tail
    (header.split(","), content.map(_.split(",")))
}


def getIndex(header: Array[String]): Map[String, Int] = {
    header.zipWithIndex.toMap
}


def getColumn(content: List[Array[String]], index: Int) = {
    content.map(x => x(index))
}


def getMeanValue(content: List[Array[String]], index: Int) : Double = {
    val column = getColumn(content, index)
    column.map(_.toDouble).reduce(_ + _) / column.length
}


def getHistogram(content: List[Array[String]], index: Int, nBins: Int) = {
    val column = content.map(x => x(index).toDouble)
    val start = column.min
    val end = column.max
    column.groupBy(x => ((x - start) * nBins / (end - start)).toInt * (end - start) / nBins + start).view.mapValues(_.length).toList.sortBy(_._1)
}


// Bird Pokemon with highest attack
println(content.filter(x => x(index.getOrElse("classification", -1)) == "Bird Pokémon")
      .maxBy(x => x(index.getOrElse("attack", -1))).array(index.getOrElse("name", -1)))



