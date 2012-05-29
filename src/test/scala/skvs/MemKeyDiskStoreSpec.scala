import org.specs2.mutable._
import skvs._

class TestStoreSource extends DiskStoreSource[Array[Byte]] {
  def apply(name: String): DiskStore[Array[Byte]] = {
    MemKeyDiskStoreSource("./tmp/" + name)
  }
}


class MemKeyDiskStoreSpec extends Specification {

  def store(name:String) = TestStoreSource(name)

  "An empty store" should {
    "return None from get" in {
      store("1").get("Hello") must_== None
    }
  }



}
