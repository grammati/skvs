import org.specs2.mutable._
import MemKeyDiskStore._


object TestStoreSource {
  def apply(name: String): DiskStore[Array[Byte]] = {
    MemKeyDiskStore("./tmp/" + name)
  }
}


class MemKeyDiskStoreSpec extends Specification {

  def store(name:String) = TestStoreSource(name)

  "An disk store" should {

    var ds: DiskStore[Array[Byte]] = null
    
    "be creatable" in {
      ds = store("test1")
    }

    "return None from get when empty" in {
      ds.get("Hello".getBytes("utf-8")) must_== None
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
  }

}
