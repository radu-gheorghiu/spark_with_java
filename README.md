## Master Apache Spark - Hands On! (with Java)

### Notes from the Udemy course ["Master Apache Spark - Hands On!"](https://udemy.com/course/the-ultimate-apache-spark-with-java-course-hands-on)

Although I have some professional experience with Spark, I haven't worked with the Java API and I need to have my skills sharpened for a new project I am going to be part of. I've had colleagues recommend this course to me, so I decided to also take up some random notes of things I want to refresh and remember.

### Section 1. L1_Introduction

The processing of data is **always** done on the **Workers**! All the work is done by the Workers!

![job_always_done_on_workers](/media/img1.png)

The first step that happens to run a Spark application like this:
1. You submit the code to the master node, usually done by packaging the code as a .jar file and submitting it to the master node.
2. The master node then launches processes on the worker nodes. These processes actually execute the code on the workers.

What is essential in a cluster is that data is split into partitions, which are just groups of rows.

![data_partitions](/media/img2.png)

Breaking up a file into partitions allows for smooth parallelization. For example if you have 4 cores on your  CPU, you can launch 4 threads and each thread can be working on its own set of rows, its own partition.

![each_partition_requires_a_thread](/media/img3.png)

Each worker loads partitions of a file in memory and spawns a thread for each file partition. In "Spark land" each thread is called a "Task", in a simplified way. But each Worker can be configured to have 2 or 3 or more Task threads.

These "Tasks" represents the code that we wrote earlier. Each Task is going to read a partition of the file, perform the Transformation and Filtering we specified in the code and then finally each task will make a connection with the database and load their respective partitions we specified in the code.

In the real world, data is distributed on a filesystem, like the Hadoop filesystem HDFS or S3.

![distributed_filesystem](/media/img4.png)

### Node responsibilities in the cluster [1.8 - L1_Introduction.Spark Standalone Cluster Architecture]

1. Once the application is submitted to the Master node, the Master node will tell one of the Workers to start the "Driver" process. This Driver process is the heart of the Spark job.

    ![the_heart_of_a_spark_job](/media/img5.png)

2. The Driver has all the information like: where to get the data from, how to perform the Transformations and then what to do with the transformed data (show to display, save it to db or disk etc.)
The Driver contains all the code, but does not execute that code. The Worker Tasks execute the code. 
 
3. The Driver then tells the Master node to launch an Executor JVM on each of the Worker nodes so that they can begin executing the code. The code to execute will be sent from the Driver to each of the Executors so that each node/Worker will be working on the tasks that it was assigned.

4. Because the Executors are the actual processes that do the work you want to make sure that you allocate enough memory to the Executors (which are just JVM containers). And each Executor can have one or more Tasks/Threads that are spawned withing the Executors.

5. Then the data partitions are LOADED into the Executor memory. But the Driver is the one orchestrating the entire job.
    
    ![the_executors_run_the_code](/media/img6.png)

6. Just some extra info, you can tune the master to be highly-available, so that if the Master node goes down, it can be replaced. You can also launch an Executor process on the Master node as well, so that it can also do some work.

    ![the_master_can_do_work](/media/img7.png)

7. If an Executor process crashes, the Worker is capable of starting up again, if the Worker crashes, the Master can start it up again. But if the Driver node crashes, you can configure the Master can re-launch the program, but in a situation like that, the Executor nodes will have to be restarted, the instructions will have to be sent from the Driver to the Executors to start processing again.

8. **Best practice in terms of performance**, depending on the number of cores on each worker, you should have at least 2x or 3x the number of Tasks as there are cores. So, if you have 4 cores, it would be good to have 8 to 12 Task threads running on that worker. You can configure how many Task Threads the Executor can spawn when you configure the Worker nodes.

### Section 2. Spark Java Dataset API Basics

One of the most important things mentioned in this section are:

1. When you read in data into a Dataset, you have different options when processing the data and defining types for the data. You can let Spark decide the datatype for each column using `.option("inferSchema", true)` or you can define your own schema and apply it when reading the data using the `.schema(defined_schema)` command.

2. Also, when working with JSON files, it's important to remember that there are 2 major types of JSON files:
- [single line JSON files](/L1/simple.json) - where each row in the JSON file is equivalent to a row in the Dataset
- [multiline JSON files](/L1/multiline.json) - where each JSON entry is identified by Spark as a row, automatically