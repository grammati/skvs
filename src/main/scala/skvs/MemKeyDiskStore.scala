/**
 * MemKeyDiskStore
 * 
 * This is an implementation of the DiskStore trait with the following properties:
 *  - A store is a directory containing 3 files - "generation", "keys", and "values".
 *  - Keys are kept in memory. This store is not suitable for applications in which
 *    the total size of all keys does not fit comfortably in memory.
 *  - Values are stored on-disk, in an append-only file, and only read into memory
 *    when required.
 *  - Duplicate values are not stored more than once. Instead, multiple keys can refer
 *    to the same value.
 *  - Keys are stored in an append-only file which maps each key to the offset of its
 *    value in the value-file.
 *  - The generation-file contains a single integer, which identifies the last completed
 *    flush operation. Any flush operation that did not complete fully will leave
 *    key-records in the key-file whose generation number exceeds the store's generation,
 *    as read from the generation-file. These records are discarded upon loading.
 *    
 * TODOs
 *  - Test to make sure it's actually fast.
 *  - traverse - figure out how to do a fast traverse of the TreeMap. Right now I'm just
 *    using dropWhile/takeWhile, which I fear might be a linear iteration of the whole tree,
 *    which is pretty disgusting :(
 *  - Better handling of loading a corrupted store (i.e. where a flush was interrupted).
 *    Perhaps a mixin trait for configurable handling of the partially-flushed keys
 *    and values, so that you could try to recover or something.
 *  - other TODOs scattered through the code
 *  - Overall, I would like to split out orthogonal functionality into mix-in-able traits.
 *    For example, I can image creating a store like this:
 *    object myStore extends DiskStore
 *                      with InMemoryKeyMap
 *                      with LRUValueCache
 *                      with FooBarCorruptedStoreRecoveryStrategy
 *                      with SingleThreadedAccess
 *                      with ...etc...
 *    But for now, I've only just managed to get this thing working, so... later.
 */

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

  // These are the keys that need to be written on the next flush.
  protected var dirtyKeys = SortedSet[KeyType]()

  // This number is incremented on each flush, and written to the generation-file
  // as the last step of the flush process.
  protected var generation: Int = 0

  // This is the value-file. It is kept open for fast (I hope) reading an writing.
  protected var valueFile: RandomAccessFile = null


  /////////////////////////////////////////////////////////////////////////////
  // Initialize upon construction
  initialize

  // Initializes this data store. If this.storeLocation refers to an
  // existing store, it will be loaded. If not, it is assumed to be a
  // new data store.
  protected def initialize {
    val root = new File(storeLocation)
    
    if (!root.isDirectory)
      root.mkdirs

    // TODO - handle corrupted store (eg: missing one, but not all, files)
    val shouldLoad = (
        new File(storeLocation + "/generation").isFile
        && new File(storeLocation + "/values").isFile
        && new File(storeLocation + "/keys").isFile)
 
    // Open the value file and keep it open (TODO - does this actually make anything faster...?)
    valueFile = new RandomAccessFile(storeLocation + "/values", "rw")

    if (shouldLoad)
      load
  }

  // Loads an existing data store, by loading all the keys into memory.
  protected def load {
    val genFile = new java.io.DataInputStream(new FileInputStream(storeLocation + "/generation"))
    generation = genFile.readInt
    genFile.close

    // Load all keys from the key-file
    val keys = new java.io.DataInputStream(new FileInputStream(storeLocation + "/keys"))
    val gen = 0
    try {
      while (true) {
        // Read the next key entry from the file: [generation][size][offset][bytes]
    	val (keyVal, offset) = readKey(keys)
    	
        // Map the key to the offset of its value
    	valueFile.seek(offset)
    	val hash = valueFile.readInt
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
    keys.close
  }
  
  
  /////////////////////////////////////////////////////////////////////////////
  // Miscelaneous utility methods

  // Return the hash value of a byte array. This is a value-based hash, and is used to
  // prevent storing more than one copy of the same value.
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

  // Returns the value from a ValueRecord, either from memory, or by reading
  // it from disk.
  protected def valueOf(v: ValueRecord): ValueType = {
    if (v.value == null)
      valueAtOffset(v.offset)
    else
      v.value
  }

  // Returns either a new or existing ValueRecord, depending on whether the given 
  // value already exists in this store.
  protected def recordForValue(v: ValueType): ValueRecord = {
    val hash = hashOfByteArray(v)
    (valueHashes.get(hash) match {
      case Some(vrs) => vrs.find( vr => v.sameValue(valueOf(vr)) ) // TODO - compare without reading the whole value from disk!
      case None => None
    }) getOrElse ValueRecord(hash, -1, v)
  }

  
  /////////////////////////////////////////////////////////////////////////////
  // Reading and writing keys and values.
  
  // Reads a key from the given input stream, and returns a Pair of key 
  // and the offset of its value.
  protected def readKey(f: DataInputStream): Pair[KeyType, Offset] = {
    // [generation][value-offset][key-size][key]
    val gen = f.readInt
    if (gen > generation) {
      throw new Exception("corrupted key file - uncommited keys found") // TODO - handle this better!
    }
    
    val offset = f.readLong()
    val keySize = f.readInt()
    
    val keyVal = new KeyType(keySize)
    f.read(keyVal); // TODO - does this always read enough bytes?
    
    (keyVal, offset)
  }
  
  // Writes the given key and offset to the output stream.
  protected def writeKey(f: DataOutputStream, key: KeyType, offset: Offset): Unit = {
    // [generation][value-offset][key-size][key]
    f.writeInt(generation)
    f.writeLong(offset)
    f.writeInt(key.length)
    f.write(key)
  }
  
  // Reads and returns the value at the given offset in the value file.
  protected def valueAtOffset(offset: Long): ValueType = {
    valueFile.seek(offset) // TODO - handle out-of-range?
    valueFile.readInt()    // read and discard the hash
    val valueSize = valueFile.readInt()
    val value = new ValueType(valueSize)
    valueFile.read(value) // TODO - handle reading too few bytes
    value
  }

  // Write the given ValueRecord to the output stream, and returns the number
  // of bytes written.
  protected def writeValueRecord(f: DataOutputStream, vr: ValueRecord): Int = {
    val size = vr.value.length
    
    // [hash][size][value]
    f.writeInt(vr.hash)
    f.writeInt(size)
    f.write(vr.value)

    // Return the number of bytes written to the file
    8 + size
  }


  /////////////////////////////////////////////////////////////////////////////
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
    dirtyKeys += key
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
          var (key,vr) = (kv._1, kv._2)
          rdr match {
            case Done(result) => return result
            case More(fn) => rdr = fn(Some((key, valueOf(vr))))
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
    synchronized {

      generation += 1

      var pos = valueFile.length()

      // Buffer the writes. I hope this makes writing faster (but I haven't checked).
      val vos = new BufferedOutputStream(new FileOutputStream(storeLocation + "/values", true)) // true for "append"
      val valFile = new DataOutputStream(vos);

      val kos = new BufferedOutputStream(new FileOutputStream(storeLocation + "/keys", true))
      val keyFile = new DataOutputStream(kos)

      // Write all the values first.
      var offsets = Vector[Long]()
      dirtyKeys foreach { key =>
        keyMap.get(key) match {
          case Some(vr) => {
            if (vr.offset == -1) {
              // Pending record - not yet written
              val bytesWritten = writeValueRecord(valFile, vr)

              // In-place update of the ValueRecord, because other keys, later
              // in this iteration, may refer to it too, and we don't want to
              // write its value twice.
              vr.offset = pos
              vr.value = null               // TODO - keep values in memory, up to some size limit?

              pos += bytesWritten
            }
            offsets :+= vr.offset
          }
          case None =>                  // shouldn't happen - panic
        }
      }
      dirtyKeys.zip(offsets) foreach { keyAndOffset =>
        val (key,offset) = keyAndOffset
        writeKey(keyFile, key, offset)
      }


      dirtyKeys = dirtyKeys.empty

      val genFile = new FileOutputStream(storeLocation + "/generation", false) // overwrite, don't append
      new DataOutputStream(genFile).writeInt(generation)

      valFile.close
      keyFile.close
      genFile.close
    }
  }

}


object MemKeyDiskStore {
  def apply(storeLocation: String): MemKeyDiskStore = {
    new MemKeyDiskStore(storeLocation)
  }
}
