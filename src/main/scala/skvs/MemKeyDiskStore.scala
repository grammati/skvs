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

  type KeyType = Array[Byte] // for readability
  type ValueType = Array[Byte]
  type Offset = Long

  implicit def byteArrayToOrdered(ba: Array[Byte]): OrderedByteArray = OrderedByteArray(ba)

  implicit object byteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = OrderedByteArray(a).compare(b)
  }

  //type ValueRecord = Either[Offset, ValueType]
  case class ValueRecord(var offset: Offset, var value: ValueType)

  protected var keyMap = TreeMap[KeyType, ValueRecord]()
  protected val valueHashes = scala.collection.mutable.Map[Int, SortedSet[Long]]()
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
        val keyVal = new KeyType(keySize)
        keys.read(keyVal); // TODO - does this always read enough bytes?
        val offset = keys.readLong()

        // Map the key to the offset of its value
        keyMap += keyVal -> ValueRecord(offset, null)

        // Read the referenced value from the value file, so we can
        // populate the valueHashes map, for looking up duplicated
        // values.
        val hash = hashOfByteArray(valueAtOffset(offset))
        valueHashes(hash) += offset
      }
    }
    catch{
      case e: EOFException => // expected
        //case _ => throw
    }
  }

  protected def valueAtOffset(offset: Long): ValueType = {
    valueFile.seek(offset) // TODO - handle out-of-range?
    val valueSize = valueFile.readInt()
    val value = new ValueType(valueSize)
    valueFile.read(value) // TODO - handle reading too few bytes
    value
  }

  def hashOfByteArray(ba: ValueType): Int = {
    // http://programmers.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
    var hash = 5381
    var i = 0
    while (i < ba.length) {
      hash = ((hash << 5) + hash) + ba(i)
    }
    hash
  }

  def valueOf(v: ValueRecord): ValueType = {
    if (v.value == null) valueAtOffset(v.offset) else v.value
  }


  // Public API

  def put(key: KeyType, value: ValueType): Unit = {
    keyMap += key -> ValueRecord(-1, value)
  }

  def get(key: KeyType): Option[ValueType] = {
    keyMap.get(key) match {
      case None => None
      case Some(v) => v match {
        case ValueRecord(offset, null) => Some(valueAtOffset(offset))
        case ValueRecord(_, value) => Some(value)
      }
    }
  }

  def traverse[A](start: KeyType, end: KeyType)(reader: Reader[A]): A = {
    var rdr = reader
    rdr match {
      case Done(result) => return result
      case More(_) => {

        // TODO - find a O(logN) way to get the subtree
        val inRange = keyMap.dropWhile(_._1 < start).takeWhile(_._1 <= end)

        inRange.foreach { kv =>
          rdr match {
            case More(fn) => rdr = fn(Some((kv._1, valueOf(kv._2))))
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

