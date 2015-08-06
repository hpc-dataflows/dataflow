import org.apache.spark.SparkContext
import scopt._

class SimpleMap {

  import java.io.File
  case class Config(src: String = "", dst: String = "",
                    blocks: Int = 0, blockSize: Int = 0, nparts : Int = 1,
                    size: Int = 1, nodes: 1)

  def nanoTime[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0, result)
  }

  def parseCommandLine(args: Array[String]) = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("scopt", "3.x")
      opt[String]('s', "src") action { (x, c) =>
        c.copy(src = x)
      } text ("s/src is a String property")
      opt[String]('d', "dst") action { (x, c) =>
        c.copy(dst = x)
      } text ("d/dst is a String property")
      opt[Int]('b', "blocks") action { (x, c) =>
        c.copy(blocks = x)
      } text ("b/blocks is a String property")
      opt[Int]('s', "block_size") action { (x, c) =>
        c.copy(blockSize = x)
      } text ("s/block_size is a String property")
      opt[Int]('n', "nodes") action { (x, c) =>
        c.copy(nodes = x)
      } text ("p/nparts is a String property")
      opt[Int]('p', "nparts") action { (x, c) =>
        c.copy(nparts = x)
      } text ("p/nparts is a String property")
      opt[Int]('z', "size") action { (x, c) =>
        c.copy(size = x)
      } text ("p/nparts is a String property")
    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      return config
    } getOrElse {
      // arguments are bad, usage message will have been displayed
    }
  }

  def main(args: Array[String]) {\
    parseCommandLine(args)
  }ß
}
