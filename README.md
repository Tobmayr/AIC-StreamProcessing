# Advanced Internet Computing Group2 Team1

[Source of this Repository](http://hyde.infosys.tuwien.ac.at/aic2016/G2T1v2/commits/master)

Toolchain
---------

* Language: Java
* Buildtool: Gradle >= 3.1

plugins

* checkstyle-idea
* gradle

# Testing in the provided VM

After starting the VM and logging in with the username ubuntu and password ubuntu there will be some scripts located under ~/Desktop to manage the topology.

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
# i.e. 100 makes 100 (virtual) seconds in a single wall-clock second
./startProvider.sh speedup-factor

# open a browser pointing to the dashboard
./runFirefox.sh
```

Testdata
--------

We provide a single file containing the test data (merged, sorted, with end tokens to) which can be found under ~/testdata.