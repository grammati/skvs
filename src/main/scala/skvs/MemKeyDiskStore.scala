import scala.collection.{SortedSet, SortedMap}
import scala.collection.mutable.{ArrayBuffer}
import scala.collection.immutable.{TreeMap}
import java.io._

// An implementation of DiskStore that keeps all keys in memory.
class MemKeyDiskStore(storeLocation: String) extends DiskStore[Array[Byte]] {

  implicit object byteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      var i = 0;
      val len = Math.min(a.length, b.length)
      while (i < len) {
        if (a(i) < b(i)) return -1;
        if (a(i) > b(i)) return 1;
        i += 1;
      }
      scala.math.signum(a.length - b.length)
    }
  }

  sealed trait StoreEntry
  case class KeyEnty(value: Array[Byte], offset: Long) extends StoreEntry
  case class ValueEnty(value: Array[Byte], refcount: Long)

  protected var valueOffsets = TreeMap[Array[Byte], Long]()
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
        val keyVal = new Array[Byte](keySize)
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

  protected def valueAtOffset(offset: Long): Option[Array[Byte]] = {
    valueFile.seek(offset) // TODO - handle out-of-range
    val valueSize = valueFile.readInt()
    val refCount = valueFile.readInt()
    val value = new Array[Byte](valueSize)
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

  def hashOfByteArray(ba: Array[Byte]): Int = {
    // http://programmers.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
    var hash = 5381
    var i = 0
    while (i < ba.length) {
        hash = ((hash << 5) + hash) + ba(i)
    }
    hash
  }

  def flush() = {

  }

}

object MemKeyDiskStore {
  def load(storeLocation: String): MemKeyDiskStore = {
    val store = new MemKeyDiskStore(storeLocation)
    store.load
    store
  }
}
