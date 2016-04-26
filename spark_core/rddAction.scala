
// collect

val rdd = sc.parallelize((1 to 10000).toList)
val filteredRdd = rdd filter { x => (x % 1000) == 0 }
val filterResult = filteredRdd.collect

// count
val rdd = sc.parallelize((1 to 10000).toList)
val total = rdd.count

//countByValue

val rdd = sc.parallelize(List(1, 2, 3, 4, 1, 2, 3, 1, 2, 1))
val counts = rdd.countByValue

// first

val rdd = sc.parallelize(List(10, 5, 3, 1))
val firstElement = rdd.first

// max

val rdd = sc.parallelize(List(2, 5, 3, 1))
val maxElement = rdd.max

// min

val rdd = sc.parallelize(List(2, 5, 3, 1))
val minElement = rdd.min

// take

val rdd = sc.parallelize(List(2, 5, 3, 1, 50, 100))
val first3 = rdd.take(3)

// takeOrdered

val rdd = sc.parallelize(List(2, 5, 3, 1, 50, 100))
val smallest3 = rdd.takeOrdered(3)

// top

val rdd = sc.parallelize(List(2, 5, 3, 1, 50, 100))
val largest3 = rdd.top(3)

// fold

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val sum = numbersRdd.fold(0) ((partialSum, x) => partialSum + x)
val product = numbersRdd.fold(1) ((partialProduct, x) => partialProduct * x)

// reduce

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val sum = numbersRdd.reduce ((x, y) => x + y)
val product = numbersRdd.reduce((x, y) => x * y)


// Actions on RDD of key-value Pairs



// countByKey

val pairRdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 11), ("b", 22), ("a", 1)))
val countOfEachKey = pairRdd.countByKey

// lookup

val pairRdd = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 11), ("b", 22), ("a", 1)))
val values = pairRdd.lookup("a")




// Actions on RDD of Numeric Types

// mean

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val mean = numbersRdd.mean

// stdev

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val stdev = numbersRdd.stdev

// sum

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val sum = numbersRdd.sum

// variance

val numbersRdd = sc.parallelize(List(2, 5, 3, 1))
val variance = numbersRdd.variance



// Saving an RDD


// saveAsTextFile

val numbersRdd = sc.parallelize((1 to 10000).toList)
val filteredRdd = numbersRdd filter { x => x % 1000 == 0}
filteredRdd.saveAsTextFile("numbers-as-text")


// saveAsObjectFile

val numbersRdd = sc.parallelize((1 to 10000).toList)
val filteredRdd = numbersRdd filter { x => x % 1000 == 0}
filteredRdd.saveAsObjectFile("numbers-as-object")

// saveAsSequenceFile

val pairs = (1 to 10000).toList map {x => (x, x*2)}
val pairsRdd = sc.parallelize(pairs)
val filteredPairsRdd = pairsRdd filter { case (x, y) => x % 1000 ==0 }
filteredPairsRdd.saveAsSequenceFile("pairs-as-sequence")
filteredPairsRdd.saveAsTextFile("pairs-as-text")

