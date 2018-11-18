package org.broadinstitute.dsde.workbench.leonardo.util

import org.scalatest.FunSuite

/**
  * Copied from https://github.com/broadinstitute/dig-loam-stream/blob/master/src/test/scala/loamstream/util/ValueBoxTest.scala
  */
final class ValueBoxTest extends FunSuite {
  test("mutateAndGet") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() === 42)

    val (newValue, changed) = v.mutateAndGet(_ + 1)

    assert(changed)
    assert(newValue === 43)
    assert(v() === 43)
  }

  test("mutateAndGet - no change") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() === 42)

    val (newValue, changed) = v.mutateAndGet(_ + 0)

    assert(changed === false)
    assert(newValue === 42)
    assert(v() === 42)
  }

  test("Companion object apply with initial value") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v.value == 42)
  }

  test("value_=") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v.value == 42)
    assert(v() == 42)

    v.value = 99

    assert(v.value == 99)
    assert(v() == 99)
  }

  test("apply") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)
  }

  test("update") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)

    v() = 99

    assert(v() == 99)
  }

  test(":=") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)

    v := 99

    assert(v() == 99)
  }

  test("mutate") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)

    v.mutate(_ + 1)

    assert(v() == 43)
  }

  test("get") {
    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)

    assert(v.get(_.toString) == "42")
  }

  test("getAndUpdate") {

    val v: ValueBox[Int] = ValueBox(42)

    assert(v() == 42)

    val result = v.getAndUpdate(i => (i + 1, i.toString))

    assert(v() == 43)
    assert(result == "42")
  }

  test("hashCode") {
    assert(ValueBox(42).hashCode != ValueBox("42").hashCode)

    assert(ValueBox(42).hashCode == 42.hashCode)
    assert(ValueBox("42").hashCode == "42".hashCode)
  }

  test("equals") {
    assert(ValueBox(42) != ValueBox("42"))

    assert(ValueBox(42) == ValueBox(42))
    assert(ValueBox(42) != ValueBox(43))

    assert(ValueBox("42") == ValueBox("42"))
    assert(ValueBox("42") != ValueBox("abc"))
  }

  test("unapply") {
    val v = ValueBox(42)

    val ValueBox(i) = v

    assert(i == 42)
  }

  test("foreach") {
    var foo = 0

    val v = ValueBox(42)

    assert(v() == 42)

    v.foreach(foo += _)

    assert(v() == 42)
    assert(foo == 42)

    v.foreach(foo += _)

    assert(v() == 42)
    assert(foo == 84)
  }
}
