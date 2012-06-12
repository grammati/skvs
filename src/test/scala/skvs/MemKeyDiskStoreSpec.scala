import org.specs2.mutable._
import MemKeyDiskStore._
import java.io._


class StringStore(storeLocation: String) extends MemKeyDiskStore(storeLocation) {
  // For testing convenience
  def put(key: String, value: String): Unit = put(key.getBytes("utf-8"), value.getBytes("utf-8"))
  def get(key: String): Option[String] = get(key.getBytes("utf-8")) match {
    case Some(v) => Some(new String(v, "utf-8"))
    case None => None
  }
  def traverse[A](start: String, end: String)(reader: Reader[A]): A = {
    traverse(start.getBytes("utf-8"), end.getBytes("utf-8"))(reader)
  }
}

object TestStoreSource {
  def apply(name: String): DiskStore[Array[Byte]] = {
    new StringStore("./tmp/" + name)
  }
}

class MemKeyDiskStoreSpec extends Specification {

  def store(name:String) = {
    new StringStore("./tmp/" + name)
  }

  // Return a Reader that collects values in a List
  def listCollector[T] = {
    var lst = List[T]()
    def fn(kv: Option[(T,T)]): Reader[List[T]] = kv match {
      case None => Done(lst)
      case Some((k,v)) => {
        lst ::= v
        More(fn)
      }
    }
    More(fn)
  }

  def stringCollector = {
    var lst = List[String]()
    def fn(kv: Option[(Array[Byte],Array[Byte])]): Reader[List[String]] = kv match {
      case None => Done(lst)
      case Some((k,v)) => {
        lst ::= new String(v, "utf-8")
        More(fn)
      }
    }
    More(fn)
  }


  sequential                            // <-- this doesn't work!!

  "An disk store" should {

    "work properly" in {

      var ds: StringStore = null
    
      // OK, I cannot figure out how to get SBT to execute my tests in order
      // Yeah, I know, they should be independent, but... the thing I'm testing is stateful!

	  Runtime.getRuntime().exec("rm -rf ./tmp/test1")
	  
      // be creatable
      ds = store("test1")

      // return None from get when empty
      ds.get("Hello") must_== None

      // be traversable when empty
      ds.traverse("a", "zzzz")(stringCollector) must have size(0)

      // behave if flushed when empty
      ds.flush()
      ds mustNotEqual null

      // allow puts
      ds.put("cc", "cccc")

      // get an unflushed value
      ds.get("cc") must_== Some("cccc")

      // traverse an unflushed value
      ds.traverse("a", "zzz")(stringCollector) must_== List("cccc")

      // flush without error
      ds.flush

      // get a flushed value
      ds.get("cc") must_== Some("cccc")

      // allow put after flush
      ds.put("ee", "eeee")

      // get both flushed and unflushed values
      ds.get("cc") must_== Some("cccc")
      ds.get("ee") must_== Some("eeee")

      // traverse both flushed and unflushed values
      ds.traverse("a", "zzzz")(stringCollector) must_== List("eeee", "cccc")

      // traverse with inclusive endpoints
      ds.traverse("cc", "ee")(stringCollector) must_== List("eeee","cccc")

      // traverse with various in-and-out-of-range endpoints
      ds.traverse("dd", "ee")(stringCollector) must_== List("eeee")
      ds.traverse("cc", "dd")(stringCollector) must_== List("cccc")
      ds.traverse("ee", "ff")(stringCollector) must_== List("eeee")
      ds.traverse("aa", "cc")(stringCollector) must_== List("cccc")

      // properly handle keys that are an initial subsequence of another
      ds.get("c") must_== None
      ds.get("ccc") must_== None

      // Survive being flushed and re-incarnated
      ds.flush
      ds = null
      ds = store("test1")

      ds.get("cc") must_== Some("cccc")
      ds.get("ee") must_== Some("eeee")
      ds.traverse("a", "zzzz")(stringCollector) must_== List("eeee", "cccc")

    }

    "Not store duplicate values" in {

	  Runtime.getRuntime().exec("rm -rf ./tmp/duptest")
      Thread.sleep(500) // WTF??? doesn't work without this :(
      val ds2 = store("duptest")

      val v = "abcdefghij"
      0 to 1000 foreach { n =>
        ds2.put(n.toString, v)
      }
      ds2.flush

      val vf = new File("./tmp/duptest/values")
      vf.length() must_== 18 // 8 bytes record-overhead, 10-byte value
    }

  }
}
