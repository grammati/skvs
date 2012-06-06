import org.specs2.mutable._
import MemKeyDiskStore._


class StringStore(storeLocation: String) extends MemKeyDiskStore(storeLocation) {
  // For testing convenience
  def put(key: String, value: String): Unit = put(key.getBytes("utf-8"), value.getBytes("utf-8"))
  def get(key: String): Option[String] = get(key.getBytes("utf-8")) match {
    case Some(v) => Some(new String(v, "utf-8"))
    case None => None
  }
}

object TestStoreSource {
  def apply(name: String): DiskStore[Array[Byte]] = {
    new StringStore("./tmp/" + name)
  }
}

class MemKeyDiskStoreSpec extends Specification {

  def store(name:String) = new StringStore("./tmp/" + name)

  "An disk store" should {

    var ds: StringStore = null
    
    "be creatable" in {
      ds = store("test1")
    }

    "return None from get when empty" in {
      ds.get("Hello") must_== None
    }

    "be traversable when empty" in {
      ds.traverse(null, null)(More( (kv:Option[(Array[Byte], Array[Byte])]) =>
        kv match {
          case None => Done(42)
          case Some(_) => null
        }
      )) must_== 42;
    }

    "behave if flushed when empty" in {
      ds.flush()
      ds mustNotEqual null
    }

    "allow puts" in {
      ds.put("a", "aaaa")
      ds.get("a") must_== Some("aaaa")
    }
  }

}
