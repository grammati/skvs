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
    data.get(ba2s(key)) match {
      case None => return None
      case Some(v) => return Some(s2ba(v))
    }
  }

  // Convenience methods for testing - Strings are easier to deal with
  def put(key: String, value: String): Unit = put(s2ba(key), s2ba(value))
  def get(key: String): Option[String] = {
    get(s2ba(key)) match {
      case Some(ba) => Some(ba2s(ba))
      case None => None
    }
  }

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

  def store = new DummyStore()

  "An empty store" should {
    "return None from get" in {
      store.get("Hello") must_== None
    }
  }

  "A non-empty store" should {
    "return values put in" in {
      val s = store
      s.put("a", "AAA")
      s.get("a") must_== Some("AAA")
    }
  }
  
}
