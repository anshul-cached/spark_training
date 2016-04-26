
// map
val lines = sc.textFile("anyfile")
val lengths = lines map { l => l.length}

// flatmap
val words = lines flatMap { l => l.split(" ")}

// filter
val longLines = lines filter { l => l.length > 80}

// mapPartitions
val lengths = lines mapPartitions { iter => iter.map { l => l.length}}

// union
val linesFile1 = sc.textFile("anyfile")
val linesFile2 = sc.textFile("anyfile")
val linesFromBothFiles = linesFile1.union(linesFile2)

// intersection

val linesFile1 = sc.textFile("anyfile")
val linesFile2 = sc.textFile("anyfile")
val linesPresentInBothFiles = linesFile1.intersection(linesFile2)

	// Another Example

val mammals = sc.parallelize(List("Lion", "Dolphin", "Whale"))
val aquatics =sc.parallelize(List("Shark", "Dolphin", "Whale"))
val aquaticMammals = mammals.intersection(aquatics)

// subtract

val linesFile1 = sc.textFile("anyfile")
val linesFile2 = sc.textFile("anyfile")
val linesInFile1Only = linesFile1.subtract(linesFile2)

	// Here is another example.

val mammals = sc.parallelize(List("Lion", "Dolphin", "Whale"))
val aquatics =sc.parallelize(List("Shark", "Dolphin", "Whale"))
val fishes = aquatics.subtract(mammals)

//distinct

val numbers = sc.parallelize(List(1, 2, 3, 4, 3, 2, 1))
val uniqueNumbers = numbers.distinct

//cartesian

val numbers = sc.parallelize(List(1, 2, 3, 4))
val alphabets = sc.parallelize(List("a", "b", "c", "d"))
val cartesianProduct = numbers.cartesian(alphabets)

// zip

val numbers = sc.parallelize(List(1, 2, 3, 4))
val alphabets = sc.parallelize(List("a", "b", "c", "d"))
val zippedPairs = numbers.zip(alphabets)

// zipWithIndex

val alphabets = sc.parallelize(List("a", "b", "c", "d"))
val alphabetsWithIndex = alphabets.zipWithIndex

// groupBy

// Use any csvfile

case class Customer(name: String, age: Int, gender: String, zip: String)
val lines = sc.textFile("anyfile.csv")
val customers = lines map { l => {
 val a = l.split(",")
 Customer(a(0), a(1).toInt, a(2), a(3))
 }
 }
val groupByZip = customers.groupBy { c => c.zip}


// keyBy

case class Person(name: String, age: Int, gender: String, zip: String)
val lines = sc.textFile("anyfile.csv")
val people = lines map { l => {
 val a = l.split(",")
 Person(a(0), a(1).toInt, a(2), a(3))
 }
 }
val keyedByZip = people.keyBy { p => p.zip}

// sortBy

val numbers = sc.parallelize(List(3,2, 4, 1, 5))
val sorted = numbers.sortBy(x => x, true)

// randomSplit

val numbers = sc.parallelize((1 to 100).toList)
val splits = numbers.randomSplit(Array(0.6, 0.2, 0.2))

// coalesce
val numbers = sc.parallelize((1 to 100).toList)
val numbersWithOnePartition = numbers.coalesce(1)

// repartition

val numbers = sc.parallelize((1 to 100).toList)
val numbersWithOnePartition = numbers.repartition(4)

// transformation

val numbers = sc.parallelize((1 to 100).toList)
val sampleNumbers = numbers.sample(true, 0.2)





// Transformations on RDD of key-value Pairs

// keys

val kvRdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
val keysRdd = kvRdd.keys

// values

val kvRdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
val valuesRdd = kvRdd.values

// mapValues

val kvRdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3)))
val valuesDoubled = kvRdd mapValues { x => 2*x}

// join

val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val joinRdd = pairRdd1.join(pairRdd2)

// leftOuterJoin

val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val leftOuterJoinRdd = pairRdd1.leftOuterJoin(pairRdd2)

// rightOuterJoin

val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val rightOuterJoinRdd = pairRdd1.rightOuterJoin(pairRdd2)

//  fullOuterJoin

val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val fullOuterJoinRdd = pairRdd1.fullOuterJoin(pairRdd2)

// sampleByKey

val pairRdd = sc.parallelize(List(("a", 1), ("b",2), ("a", 11),("b",22),("a", 111), ("b",222)))
val sampleRdd = pairRdd.sampleByKey(true, Map("a"-> 0.1, "b"->0.2))

// subtractByKey

val pairRdd1 = sc.parallelize(List(("a", 1), ("b",2), ("c",3)))
val pairRdd2 = sc.parallelize(List(("b", "second"), ("c","third"), ("d","fourth")))
val resultRdd = pairRdd1.subtractByKey(pairRdd2)

// groupByKey

val pairRdd = sc.parallelize(List(("a", 1), ("b",2), ("c",3), ("a", 11), ("b",22), ("a",111)))
val groupedRdd = pairRdd.groupByKey()

// reduceByKey

val pairRdd = sc.parallelize(List(("a", 1), ("b",2), ("c",3), ("a", 11), ("b",22), ("a",111)))
val sumByKeyRdd = pairRdd.reduceByKey((x,y) => x+y)
val minByKeyRdd = pairRdd.reduceByKey((x,y) => if (x < y) x else y)