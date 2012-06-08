import org.specs2.mutable._
import MemKeyDiskStore._


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
    Runtime.getRuntime().exec("rm -rf ./tmp/" + name)
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


  sequential

  "An disk store" should {

    var ds: StringStore = null
    
    "be creatable" in {
      ds = store("test1")
    }

    "return None from get when empty" in {
      ds.get("Hello") must_== None
    }

    "be traversable when empty" in {
      ds.traverse("a", "zzzz")(stringCollector) must have size(0)
    }

    "behave if flushed when empty" in {
      ds.flush()
      ds mustNotEqual null
    }

    "allow puts" in {
      ds.put("cc", "cccc")
    }

    "get an unflushed value" in {
      ds.get("cc") must_== Some("cccc")
    }

    "traverse an unflushed value" in {
      ds.traverse("a", "zzz")(stringCollector) must_== List("cccc")
    }

    "flush without error" in {
      ds.flush
    }

    "get a flushed value" in {
      ds.get("cc") must_== Some("cccc")
    }

    "allow put after flush" in {
      ds.put("ee", "eeee")
    }

    "get both flushed and unflushed values" in {
      ds.get("cc") must_== Some("cccc")
      ds.get("ee") must_== Some("eeee")
    }

    "traverse both flushed and unflushed values" in {
      val res = ds.traverse("a", "zzzz")(listCollector[Array[Byte]])
      res must have size(2)
    }

  }
}
