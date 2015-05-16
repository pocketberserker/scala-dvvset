package dvvset

import scalaz._

// TODO: rename values member
final case class Clock[I, V](entries: Entries[I, V], values: List[V]) {

  import Clock._

  def size: Int = entries.map(_._3.length).sum + values.length

  def ids: List[I] = entries.map(_._1)

  // TODO: rename to values
  def allValues: List[V] = values ++ entries.map(_._3).flatten

  def map[W](f: V => W): Clock[I, W] =
    apply(entries.map { case (i, n, v) => (i, n, v.map(f)) }, values.map(f))

  def reconcile(f: List[V] => V): Clock[I, V] = {
    val v = f(this.allValues)
    apply(join(this), List(v))
  }

  def last(f: (V, V) => Boolean): Option[V] = findEntry(f, this).map(_._3)

  private def findEntry(f: (V, V) => Boolean, c: Clock[I, V]): Option[(Flag, Option[I], V)] = (c.entries, c.values) match {
    case (Nil, v::t) => findEntry(f, None, v, (Nil, t), Anonym)
    case ((_, _, Nil)::t, vs) => findEntry(f, apply(t, vs))
    case ((i, _, v::_)::t, vs) => findEntry(f, Some(i), v, (t, vs), Id)
    case (Nil, Nil) => None
  }

  private def findEntry(f: (V, V) => Boolean, i: Option[I], v: V, c: (Entries[I, V], List[V]), flag: Flag): Option[(Flag, Option[I], V)] = {
    def go(a: V, b: V) = if(f(a, b)) \/-(b) else -\/(a)
    findEntry2(go, i, v, c, flag)
  }

  private def findEntry2(f: (V, V) => V \/ V, i: Option[I], v: V, c: (Entries[I, V], List[V]), flag: Flag): Option[(Flag, Option[I], V)] = (c, flag) match {
    case ((Nil, Nil), Anonym) => Some((Anonym, i, v))
    case ((Nil, Nil), Id) => Some((Id, i, v))
    case ((Nil, v1::t), _) =>
      f(v, v1) match {
        case -\/(v2) => findEntry2(f, i, v2, (Nil, t), flag)
        case \/-(v2) => findEntry2(f, i, v2, (Nil, t), Anonym)
      }
    case (((_, _, Nil)::t, vs), _) => findEntry2(f, i, v, (t, vs), flag)
    case (((i1, _, v1::_)::t, vs), _) =>
      f(v, v1) match {
        case -\/(v2) => findEntry2(f, i, v2, (t, vs), flag)
        case \/-(v2) => findEntry2(f, Some(i1), v2, (t, vs), flag)
      }
    case ((Nil, Nil), _) => None
  }

  private def joinAndReplace(ir: I, v: V, e: Entries[I, V])(implicit E: Equal[I]): Entries[I, V] =
    e.map { case (i, n, _) =>
      if(E.equal(i, ir)) (i, n, List(v))
      else (i, n, Nil)
    }

  def lww(f: (V, V) => Boolean)(implicit E: Equal[I]): Option[Clock[I, V]] =
    findEntry(f, this) match {
      case Some((Id, Some(i), v)) => Some(Clock(joinAndReplace(i, v, entries), Nil))
      case Some((Anonym, _, v)) => Some(apply(join(this), List(v)))
      case _ => None
    }
}

final case class Vector[I](values: List[(I, Counter)])

object Clock {

  private[dvvset] sealed trait Flag
  private[dvvset] case object Id extends Flag
  private[dvvset] case object Anonym extends Flag

  def apply[I, V](value: V): Clock[I, V] = Clock(Nil, List(value))
  def apply[I, V](values: List[V]): Clock[I, V] = Clock(Nil, values)
  def apply[I, V](vv: Vector[I], values: List[V]): Clock[I, V] =
    //Clock(vv.values.sorted.map { case (i, n) => (i, n, Nil) }, values)
    Clock(vv.values.map { case (i, n) => (i, n, Nil) }, values)

  def less[I, V](c1: Clock[I, V], c2: Clock[I, V])(implicit O: Order[I]): Boolean =
    greater(c2.entries, c1.entries, false)

  @annotation.tailrec
  def greater[I, V](e1: Entries[I, V], e2: Entries[I, V], strict: Boolean)(implicit O: Order[I]): Boolean =
    (e1, e2) match {
      case (Nil, Nil) => strict
      case (_::_, Nil) => true
      case (Nil, _::_) => false
      case ((i1, n1, _)::t1, (i2, n2, _)::t2) if O.equal(i1, i2) =>
        if(n1 == n2) greater(t1, t2, strict)
        else if(n1 > n2) greater(t1, t2, true)
        else false
      case ((i1, _, _)::t1, (i2, _, _)::_) if O.lessThan(i1, i2) =>
        greater(t1, e2, true)
      case (_, _) => false
    }

  def sync[I, V](l: List[Clock[I, V]])(implicit O: Order[I]): Clock[I, V] =
    l.foldLeft(apply[I, V](Nil)) { case (c1, c2) => sync(c1, c2) }

  private def sync[I, V](c1: Clock[I, V], c2: Clock[I, V])(implicit O: Order[I]): Clock[I, V] = {
    val v =
      if(less(c1,c2)) c2.values
      else if(less(c2, c1)) c1.values
      else (c1.values ++ c2.values).distinct
    apply(sync2(c1.entries, c2.entries), v)
  }

  private def sync2[I, V](e1:Entries[I, V], e2: Entries[I, V])(implicit O: Order[I]): Entries[I, V] = (e1, e2) match {
    case (Nil, c) => c
    case (c, Nil) => c
    case ((i1, n1, l1)::t1, (i2, n2, l2)::t2) =>
      val h1 = (i1, n1, l1)
      val h2 = (i2, n2, l2)
      O.order(i1, i2) match {
        case Ordering.LT => h1 :: sync2(t1, e2)
        case Ordering.GT => h2 :: sync2(t2, e1)
        case Ordering.EQ => merge(i1, n1, l1, n2, l2) :: sync2(t1, t2)
       }
  }

  private def merge[I, V](i: I, n1: Counter, l1: List[V], n2: Counter, l2: List[V]): (I, Counter, List[V]) = {
    val ll1 = l1.length
    val ll2 = l2.length
    if(n1 >= n2) {
      if(n1 - ll1 >= n2 - ll2) (i, n1, l1)
      else (i, n1, l1.take(n1 -n2 + ll2))
    }
    else {
      if(n2 - ll2 >= n1 - ll1) (i, n2, l2)
      else (i, n2, l2.take(n2 -n1 + ll1))
    }
  }

  def join[I, V](c: Clock[I, V]): Vector[I] =
    Vector(c.entries.map { case (i, n, _) => (i, n) })

  private def event[I, V](es: Entries[I, V], id: I, value: V)(implicit O: Order[I]): Entries[I, V] = es match {
    case Nil => List((id, 1, List(value)))
    case (i, n, l)::t if O.equal(i,id) => (id, n + 1, value :: l) :: t
    case c@((i1, _, _)::_) if O.greaterThan(i1, id) => (id, 1, List(value)) :: c
    case h::t => h :: event(t, id, value)
  }

  // TODO: fix return type
  def update[I: Order, V](c: Clock[I, V], i: I): Option[Clock[I, V]] =
    c.values match {
      case List(v) => Some(Clock(event(c.entries, i, v), Nil))
      case _ => None
    }

  // TODO: fix return type
  def update[I: Order, V](c1: Clock[I, V], c2: Clock[I, V], i: I): Option[Clock[I, V]] =
    c1.values match {
      case List(v) =>
        val c = sync(Clock(c1.entries, Nil), c2)
        Some(Clock(event(c.entries, i, v), c.values))
      case _ => None
    }

  @annotation.tailrec
  private def equal2[I, V](e1: Entries[I, V], e2: Entries[I, V])(implicit E: Equal[I]): Boolean = (e1, e2) match {
    case (Nil, Nil) => true
    case ((i1, n1, l1)::t1, (i2, n2, l2)::t2) if E.equal(i1, i2) && n1 == n2 && l1.length == l2.length => equal2(t1, t2)
    case (_, _) => false
  }

  implicit def equalInstance[I: Equal, V] = new Equal[Clock[I, V]] {
    def equal(c1: Clock[I, V], c2: Clock[I, V]) = equal2(c1.entries, c2.entries)
  }
}
