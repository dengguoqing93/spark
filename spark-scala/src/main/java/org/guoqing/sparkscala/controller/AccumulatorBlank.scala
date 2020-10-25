import org.guoqing.sparkscala.config.InitSpark

object AccumulatorBlank extends App {
  val sc = InitSpark.getSparkConf()
  val file = sc.textFile("file.txt")
  val blankLines = sc.longAccumulator
  val callSigns = file.flatMap(line => {
    if (line == "") {
      blankLines add 1
    }
    line.split(" ")
  })


  callSigns.saveAsTextFile("output.txt")
  println("Blank lines: " + blankLines.value)
}