# Big Data Project #2 - Semantic Similarity
CSCI 493.71, Spring 2016, Professor Lei Xie

This file is best viewed as a GitHub Markdown which can be viewed here:
[Link](https://github.com/mgarod/My_First_Website/blob/master/readme.md)


### Authors
- Michael Garod
- Ryan Kallicharran


### Build Instructions
Required libraries: pyspark, boto (for Amazon S3 connection)

- Option 1: Run spark locally, with remote data from our Amazon S3
```
spark-submit similarity_s3.py
```

- Option 2: Run spark locally, with local data(same folder) 'project2_data.txt'
```
spark-submit similarity_localdata.py
```

Both scripts are identical except for the initial steps of loading the data into an RDD.

Place 'time' before 'spark-submit' to view real, user, and system time usage.

The scripts print results to std::out. If you wish to save results to a file, please use Unix output redirection.
```
time spark-submit similarity_s3.py > GAROD_KALLICHARRAN_output.txt
```


### More Information
Please see 'algorithm_design.txt' for a step-by-step walkthrough of the MapReduce algorithm

Please see 'UsingAmazonWebServices.pdf' for a step-by-step walkthrough of how we set up and execute our algorithm on Amazon Web Services

Please see 'improvements.txt' for the description of our implementation and potential improvements for the algorithm.


### Resources
##### Tutorial video for basics of Functional Programming and Apache Spark/PySpark
https://www.youtube.com/watch?v=9xYfNznjClE&list=WL&index=2

https://onedrive.live.com/view.aspx?resid=84334A138AC1CCE0!14907&app=PowerPoint

##### See this repository for simple examples, and an Ubuntu Install Script
https://github.com/okaram/spark-pycon15

https://github.com/okaram/spark-pycon15/tree/master/scripts
