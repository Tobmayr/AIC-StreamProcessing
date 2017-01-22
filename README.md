# Advanced Internet Computing Group2 Team1

[Source of this Repository](http://hyde.infosys.tuwien.ac.at/aic2016/G2T1v2/commits/master)

Toolchain
---------

* Language: **Java**
* Buildtool: **Gradle >= 3.1**

plugins

* checkstyle-idea
* gradle

# Testing in the provided VM

After starting the VM and logging in with the username **ubuntu** and password **ubuntu** there will be some scripts located under *~/Desktop* to manage the topology.

Starting the topology
---------------------

```
cd ~/Desktop

# start storm topology
# all dependencies (zookeeper, kafka, redis and dashboard server are started along with it)
# wait a bit until the necessary topics have been created
./startTopo.sh

# start the data provider which produces data into a kafka topic
# it takes the submission speed as the only parameter which is a speedup-factor
# i.e. 100 makes 100 (virtual) seconds go by in a single wall-clock second
./startProvider.sh speedup-factor

# open a browser pointing to the dashboard
./runFirefox.sh
```

Testdata
--------

We provide two sets of testdata files. One containing the full original testdata (merged, sorted, with end tokens) named  *taxi_data.csv* . 
The other file  *taxi_sub_data.csv*  contains a subset of only 20 taxis,  choosen from the original set of test data.  The subset file is more 
appropriate for demonstration purposes. 

Both of those files can be found under *~/testdata.*



Further Information
---------
For more detailed instructions and solutions for potential issues, take a look in our [DEV-README](http://hyde.infosys.tuwien.ac.at/aic2016/G2T1v2/blob/master/DEV-README.md) .  