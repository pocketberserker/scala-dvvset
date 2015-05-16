package dvvset

import org.scalatest.FunSuite
import org.scalatest.DiagrammedAssertions
import scalaz.std.string._
import scalaz.std.list._

class ClockTest extends FunSuite with DiagrammedAssertions {

  test("join") {
    val a = Clock[String, String](List("v1"))
    assert(Clock.join(a) === Vector(Nil))
    val a1 = Clock.update(a, "a").get
    assert(Clock.join(a1) === Vector(List(("a", 1))))
    val b = Clock(Clock.join(a1), List("v2"))
    val b1 = Clock.update(b, a1, "b").get
    assert(Clock.join(b1) === Vector(List(("a", 1), ("b", 1))))
  }

  test("update") {
    val a0 = Clock.update(Clock[String, String](List("v1")), "a").get
    val a1 = Clock.update(Clock(Clock.join(a0), List("v2")), a0, "a").get
    val a2 = Clock.update(Clock(Clock.join(a1), List("v3")), a1, "b").get
    val a3 = Clock.update(Clock(Clock.join(a0), List("v4")), a1, "b").get
    val a4 = Clock.update(Clock(Clock.join(a0), List("v5")), a1, "a").get
    assert(a0 === Clock(List(("a", 1, List("v1"))), Nil))
    assert(a1 === Clock(List(("a", 2, List("v2"))), Nil))
    assert(a2 === Clock(List(("a", 2, Nil), ("b", 1,List("v3"))), Nil))
    assert(a3 === Clock(List(("a", 2, List("v2")), ("b", 1, List("v4"))), Nil))
    assert(a4 === Clock(List(("a", 3, List("v5", "v2"))), Nil))
  }

  test("sync") {
    val x = Clock[String, String](List(("x", 1, Nil)), Nil)
    val a = Clock.update(Clock[String, String](List("v1")), "a").get
    val y = Clock.update(Clock[String, String](List("v2")), "b").get
    val a1 = Clock.update(Clock(Clock.join(a), List("v2")), "a").get
    val a3 = Clock.update(Clock(Clock.join(a1), List("v3")), "b").get
    val a4 = Clock.update(Clock(Clock.join(a1), List("v3")), "c").get
    val w = Clock[String, String](List(("a", 1, Nil)), Nil)
    val z = Clock[String, String](List(("a", 2, List("v2", "v1"))), Nil)
    assert(Clock.sync(List(w, z)) === Clock(List(("a", 2, List("v2"))), Nil))
    assert(Clock.sync(List(w, z)) === Clock.sync(List(z, w)))
    assert(Clock.sync(List(a, a1)) === Clock.sync(List(a1, a)))
    assert(Clock.sync(List(a4, a3)) === Clock.sync(List(a3, a4)))
    assert(Clock.sync(List(a4, a3)) === Clock(List(("a", 2, Nil), ("b", 1, List("v3")), ("c", 1, List("v3"))), Nil))
    assert(Clock.sync(List(x, a)) === Clock(List(("a", 1, List("v1")), ("x", 1, Nil)), Nil))
    assert(Clock.sync(List(x, a)) === Clock.sync(List(a, x)))
    assert(Clock.sync(List(a, y)) === Clock(List(("a", 1, List("v1")), ("b", 1, List("v2"))), Nil))
    assert(Clock.sync(List(y, a)) === Clock.sync(List(a, y)))
    assert(a4.lww(stringInstance.equal) === Some(Clock.sync(List(a4, a4.lww(stringInstance.equal).get))))
  }

  test("syn update") {
    val a0 = Clock.update(Clock[String, String](List("v1")),"a").get
    val vv1 = Clock.join(a0)
    val a1 = Clock.update(Clock[String, String](List("v2")), a0,"a").get
    val a2 = Clock.update(Clock[String, String](vv1, List("v3")), a1,"a").get
    assert(vv1 === Vector(List(("a", 1))))
    assert(a0 === Clock(List(("a", 1, List("v1"))), Nil))
    assert(a1 === Clock(List(("a", 2, List("v2", "v1"))), Nil))
    assert(a2 === Clock(List(("a", 3, List("v3", "v2"))), Nil))
  }

  test("lww last") {
    def f(a: Int, b: Int): Boolean = a <= b
    def f2(a: (Int, Int), b: (Int, Int)): Boolean = a._2 <= b._2
    val x = Clock(List(("a", 4, List(5, 2)), ("b", 1, Nil), ("c", 1, List(3))), Nil)
    val y = Clock(List(("a", 4, List(5, 2)), ("b", 1, Nil), ("c", 1, List(3))), List(10, 0))
    val z = Clock(List(("a", 4, List(5, 2)), ("b", 1, List(1))), List(3))
    val a = Clock(List(("a", 4, List((5, 1002345), (7, 1002340))), ("b", 1, List((4, 1001340)))), List((2, 1001140)))
    assert(x.last(f) === Some(5))
    assert(y.last(f) === Some(10))
    assert(x.lww(f) === Some(Clock(List(("a", 4, List(5)), ("b", 1, Nil), ("c", 1, Nil)), Nil)))
    assert(y.lww(f) === Some(Clock(List(("a", 4, Nil), ("b", 1, Nil), ("c", 1, Nil)), List(10))))
    assert(z.lww(f) === Some(Clock(List(("a", 4, List(5)), ("b", 1, Nil)), Nil)))
    assert(a.lww(f2) === Some(Clock(List(("a", 4, List((5, 1002345))), ("b", 1, Nil)), Nil)))
  }

  test("reconcile") {
    val x = Clock(List(("a", 4, List(5, 2)), ("b", 1, Nil), ("c", 1, List(3))), Nil)
    val y = Clock(List(("a", 4, List(5, 2)), ("b", 1, Nil), ("c", 1, List(3))), List(10, 0))
    assert(x.reconcile(_.sum) === Clock(List(("a", 4, Nil), ("b", 1, Nil), ("c", 1, Nil)), List(10)))
    assert(y.reconcile(_.sum) === Clock(List(("a", 4, Nil), ("b", 1, Nil), ("c", 1, Nil)), List(20)))
    assert(x.reconcile(_.sorted.head) === Clock(List(("a", 4, Nil), ("b", 1, Nil), ("c", 1, Nil)), List(2)))
    assert(y.reconcile(_.sorted.head) === Clock(List(("a", 4, Nil), ("b", 1, Nil), ("c", 1, Nil)), List(0)))
  }

  test("less") {
    val a = Clock.update(Clock[List[String], String]("v1"), List("a")).get
    val b = Clock.update(Clock(Clock.join(a), List("v2")), List("a")).get
    val b2 = Clock.update(Clock(Clock.join(a), List("v2")), List("b")).get
    val b3 = Clock.update(Clock(Clock.join(a), List("v2")), List("z")).get
    val c = Clock.update(Clock(Clock.join(b), List("v3")), a, List("c")).get
    val d = Clock.update(Clock(Clock.join(c), List("v4")), b2, List("d")).get
    assert(Clock.less(a, b))
    assert(Clock.less(a, c))
    assert(Clock.less(b, c))
    assert(Clock.less(b, d))
    assert(Clock.less(b2, d))
    assert(Clock.less(a, d))
    assert(Clock.less(b2, c) == false)
    assert(Clock.less(b, b2) == false)
    assert(Clock.less(b2, b) == false)
    assert(Clock.less(a, a) == false)
    assert(Clock.less(c, c) == false)
    assert(Clock.less(d, b2) == false)
    assert(Clock.less(b3, d) == false)
  }

  test("equal") {
    val a = Clock(List(("a", 4, List("v5", "v0")), ("b", 0, Nil), ("c", 1, List("v3"))), List("v0"))
    val b = Clock(List(("a", 4, List("v555", "v0")), ("b", 0, Nil), ("c", 1, List("v3"))), Nil)
    val c = Clock(List(("a", 4, List("v5", "v0")), ("b", 0, Nil)), List("v6", "v1"))
    assert(Clock.equalInstance[String, String].equal(a, b))
    assert(Clock.equalInstance[String, String].equal(a, c) == false)
    assert(Clock.equalInstance[String, String].equal(b, c) == false)
  }

  test("size") {
    assert(Clock(List("v1")).size == 1)
    assert(Clock(List(("a", 4, List("v5", "v0")), ("b", 0, Nil), ("c", 1, List("v3"))), List("v4", "v1")).size == 5)
  }

  test("ids allValues") {
    val a = Clock(List(("a", 4, List("v0", "v5")), ("b", 0, Nil), ("c", 1, List("v3"))), List("v1"))
    val b = Clock(List(("a", 4, List("v0", "v555")), ("b", 0, Nil), ("c", 1, List("v3"))), Nil)
    val c = Clock(List(("a", 4, Nil), ("b", 0, Nil)), List("v1", "v6"))
    assert(a.ids === List("a", "b", "c"))
    assert(b.ids === List("a", "b", "c"))
    assert(c.ids === List("a", "b"))
    assert(a.allValues.sorted === List("v0", "v1", "v3", "v5"))
    assert(b.allValues.sorted === List("v0", "v3", "v555"))
    assert(c.allValues.sorted === List("v1", "v6"))
  }

  test("map") {
    val a = Clock(List(("a", 4, Nil), ("b", 0, Nil), ("c", 1, Nil)), List(10))
    val b = Clock(List(("a", 4, List(5, 0)), ("b", 0, Nil), ("c", 1, List(2))), List(20, 10))
    assert(a.map(_ * 2) === Clock(List(("a", 4, Nil), ("b", 0, Nil), ("c", 1, Nil)), List(20)))
    assert(b.map(_ * 2) === Clock(List(("a", 4, List(10, 0)), ("b", 0, Nil), ("c", 1, List(4))), List(40, 20)))
  }
}
