import java.nio.file.Paths

import org.apache.spark.SparkContext
import java.io._

import scala.util.Try

object SimpleMap {

  case class Config(src: Option[String] = None, dst: Option[String] = None,
                    blocks: Int = 0, blockSize: Int = 0, nparts : Int = 1,
                    size: Int = 1, nodes: Int = 1)

  def nanoTime[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0, result)
  }

  def parseCommandLine(args: Array[String]) : Option[Config] = {
    val parser = new scopt.OptionParser[Config]("scopt") {
      head("SimpleMap", "0.1.x")
      opt[String]('s', "src") action { (x, c) =>
        c.copy(src = Some(x))
      } text ("s/src is a String property")
      opt[String]('d', "dst") action { (x, c) =>
        c.copy(dst = Some(x))
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
      help("help") text("prints this usage text")

    }
    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      config
    }
  }

  def createResultsDir(dst: String, resultsDirName: String): Boolean = {
    val outdir = new File(dst, resultsDirName)
    Try{ outdir.mkdirs() } getOrElse(false)
  }

  def generate(x : Int, blockCount : Int) = {
    val seed = System.nanoTime() / (x+1)
    //np.random.seed(seed)
    print(s"($x) generating $blockCount vectors...")
    val a = -1000
    val b = 1000
    val r = new scala.util.Random(seed)
    val arr = Array.fill(blockCount, 3)(r.nextDouble)
    (x,arr)
  }

  def main(args: Array[String]) {
    val config = parseCommandLine(args).getOrElse(Config())
    val sc = new SparkContext()

    createResultsDir(config.dst.getOrElse("."), "/results")

    if (config.blocks > 0 && config.blockSize > 0) {
      val rdd = sc.parallelize(0 to config.blocks, config.nodes * 12 * config.nparts)
      val gen_block_count = (config.blockSize * 1E6 / 24).toInt // 24 bytes per vector
      println(s"generating ${config.blocks} of $gen_block_count vectors each...")
      //outfile.write("generating data...\n")
      //outfile.write("partition_multiplier: "+str(args.nparts)+"\n")
      //outfile.write("gen_num_blocks: "+str(gen_num_blocks)+"\n")
      //outfile.write("gen_block_size: "+str(gen_block_size)+"\n")
      //outfile.write("total_data_size: "+str(gen_num_blocks*gen_block_size)+"\n")
      val A = rdd.map(x => generate(x, gen_block_count))
    }
    println(config)
  }
}
