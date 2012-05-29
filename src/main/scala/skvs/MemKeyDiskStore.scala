import scala.collection.{SortedSet, SortedMap}
import scala.collection.mutable.{ArrayBuffer}
import scala.collection.immutable.{TreeMap}
import java.io._

case class OrderedByteArray(a: Array[Byte]) extends Ordered[Array[Byte]] {
  def compare(that: Array[Byte]): Int = {
    var i = 0
    val len = Math.min(this.a.length, that.length)
    while (i < len) {
      if (this.a(i) < that(i)) return -1
      if (this.a(i) > that(i)) return 1
      i += 1
    }
    scala.math.signum(this.a.length - that.length)
  }
}

// An implementation of DiskStore that keeps all keys in memory.
class MemKeyDiskStore(storeLocation: String) extends DiskStore[Array[Byte]] {

  type BA = Array[Byte] // becuase I'm too lazy to type Array[Byte] :)

  implicit def byteArrayToOrdered(ba: Array[Byte]): OrderedByteArray = OrderedByteArray(ba)

  implicit object byteArrayOrdering extends Ordering[BA] {
    def compare(a: BA, b: BA): Int = OrderedByteArray(a).compare(b)
  }


  sealed trait StoreEntry
  case class KeyEnty(value: BA, offset: Long) extends StoreEntry
  case class ValueEnty(value: BA, refcount: Long)

  protected var keyMap = TreeMap[BA, Either[Long, BA]]()

  protected var valueOffsets = TreeMap[BA, Long]()
  protected val valueHashes = scala.collection.mutable.Map[Int, SortedSet[Long]]()
  protected val pending = ArrayBuffer[StoreEntry]()
  protected var generation: Long = 0
  protected var valueFile: RandomAccessFile = null

  protected def load {
    generation = scala.io.Source.fromFile(storeLocation + "/generation").mkString.toLong

    // Open the value file.
    valueFile = new RandomAccessFile(storeLocation + "/values", "rw")

    // Load keys from the key-file
    val keys = new java.io.DataInputStream(new FileInputStream(storeLocation + "/keys"))
    val gen: Long = 0
    try {
      while (true) {
        // Read the next key entry from the file: [generation][size][bytes][offset]
        val gen = keys.readLong()
        if (gen > generation)
          throw new Exception("corrupted key file - uncommited keys found")
        val keySize = keys.readInt()
        val keyVal = new BA(keySize)
        keys.read(keyVal); // TODO - does this always read enough bytes?
        val offset = keys.readLong()

        // Map the key to the offset of its value
        valueOffsets += keyVal -> offset

        // Read the referenced value from the value file, so we can
        // populate the valueHashes map, for looking up duplicated
        // values.
        hashOfValueAtOffset(offset) match {
          case Some(hash) => valueHashes(hash) += offset
          case None =>
        }

      }
    }
    catch{
      case e: EOFException => // expected
        //case _ => throw
    }
  }

  protected def valueAtOffset(offset: Long): Option[BA] = {
    valueFile.seek(offset) // TODO - handle out-of-range
    val valueSize = valueFile.readInt()
    val refCount = valueFile.readInt()
    val value = new BA(valueSize)
    valueFile.read(value) // TODO - handle reading too few bytes
    Some(value)
  }

  protected def hashOfValueAtOffset(offset: Long): Option[Int] = {
    // TODO - calculate hash incementally, without loading it into memory
    valueAtOffset(offset) match {
      case None => None
      case Some(value) => Some(hashOfByteArray(value))
    }
  }

  def hashOfByteArray(ba: BA): Int = {
    // http://programmers.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
    var hash = 5381
    var i = 0
    while (i < ba.length) {
      hash = ((hash << 5) + hash) + ba(i)
    }
    hash
  }


  // Public API

  def put(key: BA, value: BA): Unit = {

  }

  def get(key: BA): Option[BA] = {
    valueOffsets.get(key) match {
      case None => None
      case Some(offset) => valueAtOffset(offset)
    }
  }

  def traverse[A](start: BA, end: BA)(reader: Reader[A]): A = {
    var rdr = reader
    rdr match {
      case Done(result) => return result
      case More(_) => {

        // TODO - find a O(logN) way to get the subtree
        val inRange = valueOffsets.dropWhile(_._1 < start).takeWhile(_._1 <= end)

        inRange.foreach { kv =>
          rdr match {
            case More(fn) => rdr = fn(Some((kv._1, valueAtOffset(kv._2))))
            case Done(result) => return result
          }
                       }

        // Signal the the reader that we're done
        rdr match {
          case More(_) => throw new RuntimeException("Bad Reader!")
          case Done(result) => return result
        }
      }
    }
  }

  def flush(): Unit = {

  }

}

object MemKeyDiskStore {
  def load(storeLocation: String): MemKeyDiskStore = {
    val store = new MemKeyDiskStore(storeLocation)
    store.load
    store
  }
}
