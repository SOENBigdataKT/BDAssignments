Assignment: Who To Follow Algorithm

Description: 

The Who to Follow Algorithm is implemented using two mapreduce jobs.
The first Map-reduce job will produce the inverted list of followers for the user X.
The Second Map-reduce job will intially filter the existing followers, counts the occurrences of followers and sorts the
resulting recommendations by number of common followed people.

Prerequisites:

Java version : JDK 1.8
Hadoop version : 2.7.3
Libraries: hadoop jars for mapreduce to be included in the environment path variable.
 

Install and Run:

There are two ways in which we can run the project.

1. Download the source code from github: https://github.com/SOENBigdataKT/BDAssignments and make an executable jar file in eclipse IDE to run from Linux
(or)
2. Directly download the executable jar file (WTF.jar) from this repository (https://github.com/SOENBigdataKT/BDAssignments)

3. Once the executable jar file is ready, we can run the jar with command line arguments with the followinhg steps:

	3.1 Navigate to the path where the executable jar is present
	3.2 Create an input file text in the same directory where the jar is present
	3.3 Execute the jar with the command:    java -jar WTF.jar input.txt output1 output2
  
Classification of Source files:

BigDataProjects/src/com/wtf/WTFAlgorithm.java : Main source file which controls the job configuration of Mapper and Reducer
BigDataProjects/src/com/wtf/FirstMapper.java:  	It is the first Mapper class which takes the input from input.txt file
BigDataProjects/src/com/wtf/FirstReducer.java:  This Reducer takes the output of FirstMapper and produces an inverted list of users.
BigDataProjects/src/com/wtf/SecondMapper.java:	This Mapper will filter the exiting followers
BigDataProjects/src/com/wtf/SecondReducer.java: The SecondReducer class will sorts the recommendations with the help of Recommendation class and produce the final output of Followers.
BigDataProjects/src/com/wtf/Recommendation.java : This class is used for getting the Recommendations.

Results:

As explained above in 3.3, there will be two output directories that will be created. 
Output1 : This contains the intermediate result of FirstMapper1 and FirstReducer
Output2: This directory contains the final output of WhotoFollow Algorithm with recommendations of the format given below

X R 1 (n 1 ) R 2 (n 2 ) R 3 (n 3 )

where Ri are the ids of the people recommended to user X and ni is the number of followed
people in common between X and Ri 



