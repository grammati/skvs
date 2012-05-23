sealed trait Reader[A]
case class More[T,A](value: Option[(T,T)] => Reader[A]) extends Reader[A]
case class Done[A](value: A) extends Reader[A]

trait DiskStore[T] {
  def put(key: T, value: T): Unit
  
  def get(key: T): Option[T]

  def flush(): Unit
  
  def traverse[A](start: T, end: T)(reader: Reader[A]): A
}

trait DiskStoreSource[T] {
  def apply(name: String): DiskStore[T]
}
