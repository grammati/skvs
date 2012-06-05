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
  // Convenient for comparing equality only, and faster than a.compare(b)==0
  def sameValue(that: Array[Byte]): Boolean = {
    if (this.a.length != that.length) return false
    var i = 0
    while (i < this.a.length) {
      if (this.a(i) != that(i)) return false
      i += 1
    }
    return true
  }
}

// An implementation of DiskStore that keeps all keys in memory.
class MemKeyDiskStore(storeLocation: String) extends DiskStore[Array[Byte]] {

  // Type definitions, to make intent clear
  type KeyType = Array[Byte]
  type ValueType = Array[Byte]
  type Offset = Long                    // Offset of value in file

  implicit def byteArrayToOrdered(ba: Array[Byte]): OrderedByteArray = OrderedByteArray(ba)

  // Ordering - supplies the implicit ordering parameter to TreeMap
  implicit object byteArrayOrdering extends Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = OrderedByteArray(a).compare(b)
  }

  // Instances of this type are stored as the values in the "master map".  An
  // instance should contain, at any given time, either an offset (and a null
  // value), if the value is on-disk only, or an in-memory value (and a -1
  // offset). They also keep a hash of the value, to avoid re-computing it. The
  // hash is a val because, even though the the offset and value can "change",
  // they, are conceptually constant - the value itself is constant, we just
  // change whether-or-not we have it in-memory.
  // TODO - use one var containing an Either[Offset, ValueType]?
  case class ValueRecord(
    val hash: Int,
    var offset: Offset, 
    var value: ValueType
  )

  // This is the "master" map - it maps each key to a ValueRecord, which
  // will contain either:
  //  * an offset into the value-file, or
  //  * the value itself, if it has not yet been flushed
  protected var keyMap = TreeMap[KeyType, ValueRecord]()

  // This map exists to prevent storing duplicate values.
  // It maps hash-code-of-value to the ValueRecords that have that hash.
  protected var valueHashes = Map[Int, Set[ValueRecord]]()

  // This number is incemented on each flush, and written to the generation-file
  // as the last step of the flush process.
  protected var generation: Int = 0

  // This is the value-file. It is kept open for fast (I hope) reading an writing.
  protected var valueFile: RandomAccessFile = null


  // Initialize upon construction
  initialize

  // Initializes this data store. If this.storeLocation refers to an
  // existing store, it will be loaded. If not, it is assumed to be a
  // new data store.
  protected def initialize {
    val root = new File(storeLocation)
    
    if (!root.isDirectory)
      root.mkdirs

    if (new File(storeLocation + "/generation").isFile
        && new File(storeLocation + "/values").isFile
        && new File(storeLocation + "/keys").isFile)
      load

    // Open the value file, whether or not we did a load
    valueFile = new RandomAccessFile(storeLocation + "/values", "rw")

  }

  // Loads an existing data store, by loading all the keys into memory.
  protected def load {
    generation = scala.io.Source.fromFile(storeLocation + "/generation").mkString.toInt

    // Load keys from the key-file
    val keys = new java.io.DataInputStream(new FileInputStream(storeLocation + "/keys"))
    val gen: Long = 0
    try {
      while (true) {
        // Read the next key entry from the file: [generation][size][bytes][offset]
        val gen = keys.readInt
        if (gen > generation)
          throw new Exception("corrupted key file - uncommited keys found") // TODO - handle this better!

        val hash = keys.readInt()
        val keySize = keys.readInt()
        val keyVal = new KeyType(keySize)
        keys.read(keyVal); // TODO - does this always read enough bytes?

        val offset = keys.readLong()

        // Map the key to the offset of its value
        val vr = ValueRecord(hash, offset, null)
        keyMap += keyVal -> vr

        // Populate the valueHashes map, for looking up duplicated values.
        val vrs: Set[ValueRecord] = valueHashes.getOrElse(hash, Set[ValueRecord]())
        valueHashes += hash -> (vrs + vr)
      }
    }
    catch{
      case e: EOFException => // expected
        //case _ => throw
    }
  }

  protected def valueAtOffset(offset: Long): ValueType = {
    valueFile.seek(offset) // TODO - handle out-of-range?
    valueFile.readInt()    // read and discard the hash
    val valueSize = valueFile.readInt()
    val value = new ValueType(valueSize)
    valueFile.read(value) // TODO - handle reading too few bytes
    value
  }

  protected def hashOfByteArray(ba: ValueType): Int = {
    // http://programmers.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed
    var hash = 5381
    var i = 0
    while (i < ba.length) {
      hash = ((hash << 5) + hash) + ba(i)
      i += 1
    }
    hash
  }

  protected def valueOf(v: ValueRecord): ValueType = {
    if (v.value == null)
      valueAtOffset(v.offset)
    else
      v.value
  }

  protected def recordForValue(v: ValueType): ValueRecord = {
    // Return either a new or existing ValueRecord, depending
    // on whether this is a duplicate.
    val hash = hashOfByteArray(v)
    (valueHashes.get(hash) match {
      case Some(vrs) => vrs.find( vr => v.sameValue(valueOf(vr)) ) // TODO - compare without reading the whole value from disk!
      case None => None
    }) getOrElse ValueRecord(hash, -1, v)
  }



  // Public API

  def put(key: KeyType, value: ValueType): Unit = {
    // If there is an existing value associated with the key, we need to remove
    // the link from the value-hash to the value-record.
    keyMap.get(key) match {
      case Some(vr) => {
        // This key is already mapped to a value.
        valueHashes.get(vr.hash) match {
          case Some(vrs) => {
            val newVrs: Set[ValueRecord] = vrs - vr
            if (newVrs.isEmpty)
              valueHashes -= vr.hash   // don't keep empty sets around
            else
              valueHashes += vr.hash -> newVrs
          }
          case None =>                  // shouldn't happen...
        }
      }
      case None =>
    }

    // And now we can associate the key with the new value
    keyMap += key -> recordForValue(value)
  }

  def get(key: KeyType): Option[ValueType] = {
    keyMap.get(key) match {
      case Some(v) => Some(valueOf(v))
      case None => None
    }
  }

  def traverse[A](start: KeyType, end: KeyType)(reader: Reader[A]): A = {
    var rdr = reader
    rdr match {
      case Done(result) => return result
      case More(_) => {

        // TODO - find a O(logN) way to get the subtree. dropWhile/takeWhile are
        // O(n), and do not take advantage of the tree-ness, as far as I know.
        val inRange = keyMap.dropWhile(_._1 < start).takeWhile(_._1 <= end)

        inRange.foreach { kv =>
          rdr match {
            case Done(result) => return result
            case More(fn) => rdr = fn(Some((kv._1, valueOf(kv._2))))
          }
        }

        // Signal the the reader that we're done traversing
        rdr match {
          case Done(result) => return result
          case More(fn) => fn(None) match {
            case Done(result) => result
            case _ => throw new RuntimeException("Bad Reader!")
          }
        }
      }
    }
  }

  def flush(): Unit = {
    // Ultra-simple flush for now - just traverse the whole map
    synchronized {

      generation += 1

      var pos = valueFile.length()
      valueFile.seek(pos)                       // seek to end

      // Buffer writes. I hope this makes writing faster (but I haven't checked).
      val vos = new BufferedOutputStream(new FileOutputStream(storeLocation + "/values", true)) // true for "append"
      val valFile = new DataOutputStream(vos);

      val zeros = Array[Byte](0,0,0,0,0,0,0,0)

      keyMap foreach { kv =>
        val key = kv._1
        val vr: ValueRecord = kv._2
        if (vr.offset == -1) {
          // Pending record - not yet written
          valFile.writeInt(vr.hash)
          val size = vr.value.length
          valFile.writeInt(size)
          valFile.write(vr.value)

          // In-place update of the ValueRecord, because other keys, later
          // in this iteration, may refer to it too, and we don't want to
          // write its value twice.
          vr.offset = pos
          vr.value = null               // TODO - keep values in memory, up to some size limit?

          val padding = (8 - (size % 8)) & 7
          valFile.write(zeros, 0, padding)
          
          pos += 8 + size + padding
        }
      }

      // TODO - write key file
      val kos = new BufferedOutputStream(new FileOutputStream(storeLocation + "/keys", true))
      val keyFile = new DataOutputStream(kos)
      // FIXME: write new keys

      val genFile = new FileOutputStream(storeLocation + "/generation", false) // overwrite, don't append
      new DataOutputStream(genFile).writeInt(generation)

      vos.flush
      kos.flush
      genFile.flush
    }
  }

}


object MemKeyDiskStore {
  def apply(storeLocation: String): MemKeyDiskStore = {
    new MemKeyDiskStore(storeLocation)
  }
}
