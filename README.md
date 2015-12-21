# Materials for workshop

## [slides] (./slides/)
The slides will be presented in the workshop

## [sample codes] (./mapreduce-materials/)
A simple Hadoop MapReduce sample codes for WordCount case

## shared folder for JDK7
share (file://K204A-1-1/Users/user/Desktop/share)

### Run on sscloud

##### install maven

`$ sudo apt-get install maven git`

##### clone source code

`cd ~`

`$ git clone https://github.com/takeshimiao/workshop-materials.git`

##### cd to working dir

`$ cd workshop-materials/mapreduce-materials`

##### compile and run testing

`$ mvn compile`

`$ mvn test`

If you see BUILD FAILURE at `mvn test`

`$ jps`

find `HMaster`'s pid

`$ kill -9 [pid]`

run `mvn test` again
