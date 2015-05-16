package object dvvset {
  type Counter = Int
  type Entries[I, V] = List[(I, Counter, List[V])]
}
