sealed trait Reader[A]
case class More[A](value: Option[(Array[Byte], Array[Byte])] => Reader[A]) extends Reader[A]
case class Done[A](value: A) extends Reader[A]

trait DiskStore {
  def put(key: Array[Byte], value: Array[Byte]): Unit
  
  def get(key: Array[Byte]): Option[Array[Byte]]

  def flush(): Unit
  
  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]): A
}

trait DiskStoreSource {
  def apply(name: String): DiskStore
}
