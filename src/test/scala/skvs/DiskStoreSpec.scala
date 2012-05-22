import org.specs2.mutable._
//import skvs.DummyStore
import scala.collection.immutable.SortedMap


class DummyStore extends DiskStore {

  implicit def stringToByteArray(s: String): Array[Byte] = {
    s.getBytes("utf-16")
  }
  implicit def byteArrayToString(ba: Array[Byte]): String = {
    new String(ba, "UTF-16")
  }
  def s2ba(s: String) = stringToByteArray(s)
  def ba2s(ba: Array[Byte]) = byteArrayToString(ba)

  // Use Strings for easier testing
  var data = SortedMap[String, String]()

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    data += ba2s(key) -> ba2s(value)
  }
  
  def get(key: Array[Byte]): Option[Array[Byte]] = {
    val v = data(ba2s(key))
    if (v == null) return null
    return Some(s2ba(v))
  }
  // Convenience method for testing:
  def get(key: String): Option[Array[Byte]] = get(s2ba(key))

  def flush(): Unit = {
    //
  }
  
  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]): A = {
    val from: String = start
    val to: String = end
    var rdr = reader
    rdr match {
      case More(fn) => {
        data.foreach { p =>
          val k = p._1;
           val v = p._2
           if (k >= from && k <= to) {
             rdr = fn(Some((k,v)))
             rdr match {
               case Done(result) => return result
               case More(_) => // keep looping
             }
           }
        }
        fn(None) match {
          case Done(result) => return result
          case More(_) => throw new RuntimeException("Bad Reader! No soup for you!")
        }
      }
      case Done(result) => return result
    }
  }
}


class DiskStoreSpec extends Specification {

  "An empty store" should {
    "return None from get" in {
      new DummyStore().get("Hello") must be None
    }
  }
  
}
