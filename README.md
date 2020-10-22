# Homework 2

## Description: Designed and implemented an instance of the map/reduce computational model on the DBLP Dataset.

## Steps to run the project
1. Clone the repository using the following command: 
```git clone https://MihirK3@bitbucket.org/cs441-fall2020/mihir_kelkar_hw2.git```
2. Now, navigate to the directory 'mihir_kelkar_hw2' and run the following command:
```sbt clean compile assembly```
3. A fat far file will be created in 'mihir_kelkar_hw2/target/scala-version'.
4. This jar file has to be copied to the Hadoop File System. The following instructions are for those who have installed hortonworks and are using windows.
5. Start hortonworks in your preferred hypervisor.
6. Visit this link: https://www.cloudera.com/tutorials/learning-the-ropes-of-the-hdp-sandbox.html to setup your id and password.
7. Once started, open your browser and type the link provided in the VM for your hypervisor.
8. Now, open the Ambari Dashboard and wait for the required functionalities to start.
9. Now, using your preferred transfer software, (I used WinSCP) transfer the jar file to the VM.
10. The dataset has to be added to Ambari. Assuming that you are logged in to Ambari from terminal (git bash in my case).
11. Now, to make a directory for the dataset, use the following command:
```hdfs dfs -mkdir <path to dataset>```
12. To put the dataset in the directory, use the following command:
```hdfs dfs -put <path to dataset>/```
13. To execute the jar file, use the following command:
``` hadoop jar <path to JAR file> <class name> <path to dataset> <output path>```

## Viewing the results
The output path for each tasks are constructed in the following manner:
<output path> appended with Task name.

For example: Suppose output path is /tmp/output then,
output Sort Top Ten Author is created in tmp folder.

## Parsing the input
The XMLInputFormat only supports single start and end tags. So, In order to handle multiple start and end tags, I have used Mohammed Siddiq's implementation of the XMLInputFormat.

Link to his implementation: https://github.com/Mohammed-siddiq/Authors-Relationship/tree/master/src/main/Java/JHelpers

Link to XMLInputFormat: https://mahout.apache.org/docs/0.13.0/api/docs/mahout-integration/org/apache/mahout/text/wikipedia/XmlInputFormat.html

## Output
The entire output files are provided in the outputs/ directory.

However, I have added small snippets in the Readme for better readability.

## Map-Reduce Tasks Implementation

#### List of top 100 authors in the descending order who publish with most co-authors
In order to complete this task, I developed two map-reduce jobs.

Job number in MapReduceJobs: One
 
**Mapper class in JobMappers:** CoAuthorCountMapper

**Reducer class in JobReducers:** JobReducers

**Output path:** <path> Top 100 authors with most co-authors

The first job counts the total number of co-authors each author has worked with. 
The input to the mapper is the entire dataset from which it extracts the author list and outputs the author name as key and number of co-authors for that publication. 
The reducer receives the output from the mapper, and it sums up the co-author counts for each author to get the total number of co-authors.

**Job number in MapReduceJobs:** Two
 
**Mapper class in JobMappers:** SortingMapper

**Reducer class in JobReducers:** SortingReducer

**Output path:** <path> Top100coauthors

The second job inverts the key and values which are then sorted by the DescendingComparator class in descending order. 
I have used a custom comparator by overriding the compare method of the 'WritableComparator' class to achieve the descending order sorting functionality.
The inversion is necessary since the comparator sorts by the keys.
The reducer then just inverts the keys and values to get the output in the format of author,number of co-authors and only writes the top 100 authors.

Note: I have assumed that the co-authors would not be unique since, the professor had given an example on a piazza post, and he counted the duplicate co-authors while calculating the number of co-authors.

The output snippet looks as follows:

```
h. vincent poor,5486
wei li,5031
wen gao 0001,4840
lei zhang,4810
wei zhang,4722
yu zhang,4707
yang liu,4664
philip s. yu,4643
nassir navab,4482
.
.
.
```

#### List of 100 authors who publish without any co-authors

In order to complete this task, I developed two map-reduce jobs.

Job number in MapReduceJobs:** Three
 
**Mapper class in JobMappers:** NoCoAuthorPublicationCountMapper

**Reducer class in JobReducers:** JobReducers

**Output path:** <path> Top 100 authors with no co-author

The first job counts the total number of publications for each author. 
The input to the mapper is the entire dataset from which it extracts the author list for each publication, checks if there are no co-authors and outputs the author name as key, and a number 1 for the publication. 
The reducer receives the output from the mapper, and it sums up the publication counts for each author to get the total number of publications.

**Job number in MapReduceJobs:** Four
 
**Mapper class in JobMappers:** SortingMapper

**Reducer class in JobReducers:** SortingReducer

**Output path:** <path> Top100nocoauthor

The second job inverts the key and values which are then sorted by the DescendingComparator class in descending order. 
I have used a custom comparator by overriding the compare method of the 'WritableComparator' class to achieve the descending order sorting functionality.
The inversion is necessary since the comparator sorts by the keys.
The reducer then just inverts the keys and values to get the output in the format of (author,number of publications) and only writes the top 100 authors.

The output snippet looks as follows:

```
t. d. wilson 0001,348
ronald r. yager,346
robert l. glass,262
diane crawford,250
peter g. neumann,240
david alan grier,219
elena maceviciute,218
harold joseph highland,214
karl rihaczek,195
.
.
.
```

#### For each venue, list of publications that contains only one author

In order to complete this task, I developed one map-reduce job.

Job number in MapReduceJobs:** Five
 
**Mapper class in JobMappers:** VenuePublicationOneAuthorMapper

**Reducer class in JobReducers:** VenuePublicationOneAuthorReducer

**Output path:** <path> VenuePublicationOneAuthor
 
The input to the mapper is the entire dataset from which it extracts the author list, publication title and the venue for each publication, checks if there are no co-authors and outputs the venue as key, and the title as value. 
The reducer receives the output from the mapper, and it concatenates the publication titles for each venue. 
The delimiter used in this case is '|'. 
The output format is (venue,title1 | title2 |)

The output snippet looks as follows:

```
10th anniversary colloquium of unu/iist,towards the verifying compiler. | contract-based testing. | type systems for concurrent programs. | the development of the raise tools. | coordination technologies for just-in-time integration. | x2rel: an xml relation language with formal semantics. | unu and unu/iist. | "what is an infrastructure?" towards an informatics answer. | multi-view modeling of software systems. | where, exactly, is software development? | a formal basis for some dependability notions. | in memoriam armando martín haeberer: 4 january 1947 - 11 february 2003. | verification by abstraction. | real-time process algebra and its applications. | a grand challenge proposal for formal methods: a verified stack. | real-time systems development with duration calculi: an overview. | an algebraic approach to the verilog programming. |  | 
1999 acm sigmod workshop on research issues in data mining and knowledge discovery,chaotic mining: knowledge discovery using the fractal dimension. |  | 
25 years communicating sequential processes,industrial strength csp: opportunities and challenges in model-checking. | linking theories of concurrency. | practical application of csp and fdr to software design. | applied formal methods - from csp to executable hybrid specifications. | csp, occam and transputers. | shedding light on haunted corners of information security. | process algebra: a unifying approach. | retracing the semantics of csp. | operational semantics for fun and profit. | implementation of handshake components. | of probabilistic wp and sp-and compositionality. | models for data-flow sequential processes. | order, topology, and recursion induction in csp. | seeing beyond divergence. |  | 
25 years gulp,logic programming in italy: a historical perspective. |  | 
25 years isca: retrospectives and reprints,using cache memory to reduce processor-memory traffic. | retrospective: banyan networks for partitioning multiprocessor systems. | a study of branch prediction strategies. | retrospective: a study of branch prediction strategies. | retrospective: improving direct-mapped cache performance by the addition of a small fully-associative cache and prefetch buffers. | retrospective: improving the throughput of a pipeline by insertion of delays. | retrospective: implementing precise interrupts in pipelined processors. | retrospective: a low-overhead coherence solution for multiprocessors with private cache memories. | retrospective: lockup-free instruction fetch/prefetch cache organization. | retrospective: very long instruction word architectures and the eli-512. | retrospective: architecture of a massively parallel processor. | retrospective: the stanford flash multiprocessor. | retrospective: memory consistency and event ordering in scalable shared-memory multiprocessors. | retrospective: the turn model for adaptive routing. | retrospective: instruction issue logic for high-performance, interruptable pipelined processors. | very long instruction word architectures and the eli-512. | retrospective: a personal retrospective on the nyu ultracomputer. | retrospective: evaluation of directory dchemes for cache coherence. | retrospective: multiscalar processors. | retrospective: decoupled access/execute architectures. | retrospective: a preliminary architecture for a basic data flow processor. | lockup-free instruction fetch/prefetch cache organization. | retrospective: using cache memory to reduce processor-memory traffic. | decoupled access/execute computer architectures. | retrospective: a processor for a high-performance personal computer. | retrospective: impact: an architectural framework for multiple-instruction issue. | architecture of a massively parallel processor. | retrospective: the mit alewife machine: architecture and performance. | improving direct-mapped cache performance by the addition of a small fully-associative cache prefetch buffers. |  | 
25 years of model checking,the beginning of model checking: a personal perspective. | model checking: from tools to theory. | a retrospective on murphi. | verification technology transfer. | the birth of model checking. | fifteen years of formal property verification in intel. | from church and prior to psl. | a view from the engine room: computational support for symbolic model checking. |  | 
25th anniversary of inria,digital hdtv: a technical challenge. | coordinating vehicles in an automated highway. | opportunities and challenges in signal processing and analysis. | formal theories and software systems: fundamental connections between computer science and logic. | technology, networks, and the library of the year 2000. | dependable parallel computing by randomization (abstract). | what is knowledge representation, and where is it going? | new frontiers in database system research. | differential-geometric methods: a powerful set of new tools for optimal control. | sensing robots. | creating a design science of human-computer interaction. | stochastic control and large deviations. | autonomous control. | neural computing and stochastic optimization. | fundamentals of bicentric perspective. | system dependability. | analog and digital computing. | mosaic c: an experimental fine-grain multicomputer. | world mathematical year 2000 and computer sciences. |  | 
35 years of fuzzy set theory,on lattice-based fuzzy rough sets. | on the intuitionistic fuzzy implications and negations. part 1. |  |
.
.
.
```

#### List of authors who published without interruption for N years, where 10 <= N.

In order to complete this task, I developed one map-reduce job.

**Job number in MapReduceJobs:** Six
 
**Mapper class in JobMappers:** NYearsWithoutInterruptMapper

**Reducer class in JobReducers:** NYearsWithoutInterruptReducer

**Output path:** <path> NYearsWithoutInterrupt
 
The input to the mapper is the entire dataset from which it extracts the author list, and the year for each publication.
The mapper creates a hashmap with key as the author name and value as a hashset of year (to ensure unique years for each author).
The mapper then outputs the key as author and the years as a string. 
The reducer creates a hashmap with the same features as the mapper. However, for each author it will become the years from each mapper.
Now, the reducer creates a treemap from the hashmap and just outputs the author with the consecutive N years.
The output format is (author,year1;year2;year3;......;yearN)

The reducer output contains duplicate entries which can be filtered out by another job. Since, the core functionality is finished, I did not think it was necessary to do this.
Also, the years could be sorted for better readability.

The output snippet looks as follows: (For N = 10)

```
rong-jong wai,2016;2017;2019;2020;1998;1999;2000;2001;2002;2003;2004;2005;2006;2007;2008;2009;2010;2011;2013;2015
marco schumann,2000;2001;2002;2003;2004;2005;2006;2007;2011;1998;1999
sarah diefenbach,2016;2017;2018;2019;2020;2007;2008;2009;2010;2011;2012;2013;2014;2015
.
.
.
```

#### Top ten published authors at each venue

In order to complete this task, I developed one map-reduce job.

**Job number in MapReduceJobs:** Seven
 
**Mapper class in JobMappers:** TopTenAuthorMapper

**Reducer class in JobReducers:** TopTenAuthorReducer

**Output path:** <path> TopTenAuthor
 
The input to the mapper is the entire dataset from which it extracts the author list, and the venue for each publication.
It outputs the venue as key and value as the string conversion of the author list. 
The reducer combines all the authors for each venue into a list.
The groupby and the sortBy function is applied on the list to get the top 10 frequent authors in the  authors_list on which map is used to get the author names.
Now, the reducer outputs the key as venue, and a string conversion of the top ten author list as value.
The output format is (venue,author1;author2;author3;....;author10)

The output snippet looks as follows:

```
aistats,michael i. jordan;yoshua bengio;max welling;geoffrey j. gordon;carlos guestrin;david m. blei;zoubin ghahramani;tony jebara;john shawe-taylor;eric p. xing
algorithmic oper. res.,ralf klasing;santosh n. kabadi;katta g. murty;vangelis th. paschos;gregory z. gutin;tamás terlaky;christian laforest;juraj hromkovic;g. s. r. murthy;anders yeo
am. j. comput. math.,nong pan;faheem khan;manisha patel;ranjan kumar mohanty;takayuki fujii;a. romkes;yuki kawahara;n. m. bujurke;xuzhou chen;ailin qian
ann. oper. res.,michel gendreau;liang liang 0001;laureano f. escudero;gilbert laporte;edmund k. burke;panos m. pardalos;zvi drezner;silvano martello;herwig bruneel;fred w. glover
ann. umcs informatica,elzbieta smolka;pawel mikolajczak;miroslaw hajder;wieslawa kuniszyk-józkowiak;pawel dymora;zbigniew kotulski;waldemar suszynski;krzysztof sapiecha;vasyl ustimenko;miroslaw mazurek
.
.
.
```

#### List of publications for each venue that contain the highest number of authors for each of these venues

In order to complete this task, I developed one map-reduce job.

**Job number in MapReduceJobs:** Eight
 
**Mapper class in JobMappers:** HighestNumberofAuthorVenueMapper

**Reducer class in JobReducers:** HighestNumberofAuthorVenueReducer

**Output path:** <path> HighestNumberofAuthorVenue

The input to the mapper is the entire dataset from which it extracts the author list, the publication title and the venue for each publication.
It outputs the venue as key and value as the publication title concatenated with the number of authors. 
The reducer creates a TreeMap with key as the number of authors for that publication and value as the publication titles.
The reducer then outputs the key as venue and the value as the highest entry(value of the entry) from the TreeMap.
The output format is (venue,publication1;publication2;....;publicationN)

The output snippet looks as follows:

```
advanced topics in computer vision,learning object detectors in stationary environments.;co-recognition of images and videos: unsupervised matching of identical object patterns and its applications.;moment constraints in convex optimization for segmentation and tracking.;evaluating and extending trajectory features for activity recognition.;boosting k-nearest neighbors classification.;recognizing human actions by using effective codebooks and tracking.;large scale metric learning for distance-based image classification on open ended data sets.;video temporal super-resolution based on self-similarity.
advances and applications in sliding mode control systems,robust control of robot arms via quasi sliding modes and neural networks.;optimal sliding and decoupled sliding mode tracking control by multi-objective particle swarm optimization and genetic algorithms.
advances in applied self-organizing systems,a self-organizing sensing system for structural health monitoring of aerospace vehicles.
advances in biologically inspired information systems,context data dissemination in the bio-inspired service life cycle.
advances in chaos theory and intelligent control,a novel design approach of a nonlinear resistor based on a memristor emulator.
.
.
.
```

## Deployment and Running on AWS 
I have uploaded a video to Youtube which demonstrates my deployment and running on AWS.

Link: https://youtu.be/ViGQYixK-Wk