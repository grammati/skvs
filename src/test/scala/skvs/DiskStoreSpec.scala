import org.specs2.mutable._
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

  def alphaStore = {
    val s = store
    s.put("cc", "CCC")
    s.put("aa", "AAA")
    s.put("ff", "FFF")
    s.put("dd", "DDD")
    s
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

  "Traversing a store" should {
    "iterate values in order" in {
      alphaStore.traverse("", "z")(listCollector[String]) must_== List("FFF", "DDD", "CCC", "AAA")
    }
    "iterate using inclusive start and end" in {
      alphaStore.traverse("cc", "dd")(listCollector[String]) must_== List("DDD", "CCC")
    }
    "return an empty list when start and end are out-of-range" in {
      alphaStore.traverse("xx", "zz")(listCollector[String]) must be empty
    }
    "return and empty list when start and end are reversed" in {
      alphaStore.traverse("ff", "aa")(listCollector[String]) must be empty
    }
    "work correctly when start and end are not in the collection" in {
      alphaStore.traverse("bb", "ee")(listCollector[String]) must_== List("DDD", "CCC")
    }
  }
  
}
