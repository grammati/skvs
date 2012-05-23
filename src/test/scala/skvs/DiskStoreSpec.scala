import org.specs2.mutable._
//import skvs.DummyStore
import scala.collection.immutable.SortedMap
import scala.collection.immutable.StringOps


class DummyStore[T <: Comparable[T]] extends DiskStore[T] {

  var data = SortedMap[T,T]()

  def put(key: T, value: T): Unit = data += key -> value
  
  def get(key: T): Option[T] = data.get(key)

  def flush(): Unit = {
    // do nothing - everything is in memory
  }
  
  def traverse[A](start: T, end: T)(reader: Reader[A]): A = {
    var rdr = reader
    rdr match {
      case More(fn) => {
        data.foreach { p =>
          val k = p._1
          val v = p._2
          if (k.compareTo(start) >= 0 && k.compareTo(end) <= 0) {
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

  def store = new DummyStore[String]()

  def alphaStore = {
    val s = store
    s.put("cc", "CCC")
    s.put("aa", "AAA")
    s.put("dd", "DDD")
    s.put("bb", "BBB")
    s
  }

  def listCollector[T] = {
    var lst = List[T]()
    val fn: Option[(T,T)] => Reader[List[T]] = (kv) => kv match {
      case None => Done(lst)
      case Some((k,v)) => {
        lst ::= v
        More(fn)
      }
    }
    fn
  }

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
    "return None for other values" in {
      val s = store
      s.put("a", "AAA")
      s.get("b") must_== None
    }
  }

  "Traversing a store" should {
    "iterate values in order" in {
      val s = alphaStore
      s.traverse("aa", "dd")(More(listCollector[String])) must_== List("A", "B")
    }
  }
  
}
