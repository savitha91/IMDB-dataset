 // Query1 :  Find out the animated movies that are rated 4 or above
 val genreFilter = movieDf.filter(array_contains (movieDf("genres"), "Animation")) 
 val ratingFilter = ratingDf.filter($"rating" >= 4)     
 val sol1 = genreFilter.join(ratingFilter, ratingFilter("movieId")===genreFilter("movieId")) 
 sol1.show()
 
 //Query2: Find out the average ratings for movies
  val avgRating = ratingDf.groupBy($"movieId").agg(avg($"rating").as("avg_rating"))
  avgRating.agg(count("movieId")).show() //3706 (2 partitions)
  
  //Query3: Find movies count on all the genres
  val moviesRdd=sc.textFile("/user/cloudera/SparkAssign/movies.dat")
  val genreRdd = moviesRdd.map(lines=>lines.split("::")(2))
  val genreSort=genreRdd.flatMap(lines=>lines.split("\\|")).map(k=>(k,1)).reduceByKey((k,v)=>(k+v)).sortByKey().take(10)
