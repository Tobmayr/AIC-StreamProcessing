# Advanced Internet Computing Group2 Team1

[Source of this Repository](http://hyde.infosys.tuwien.ac.at/aic2016/G2T1v2/commits/master)

Toolchain

* Language: Java
* Buildtool: Gradle >= 3.1
* Testcommand: `gradle test --stacktrace`

plugins

* checkstyle-idea
* gradle

# Setup on Ubuntu 16.04 LTS

```
sudo add-apt-repository ppa:cwchien/gradle
sudo apt-get update
sudo apt install gradle default-jdk

git clone git@hyde.infosys.tuwien.ac.at:aic2016/G2T1v2.git
cd G2T1v2

mkdir bin ; cd bin
curl http://www-eu.apache.org/dist/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz | tar --extract --gzip
curl http://www-eu.apache.org/dist/storm/apache-storm-1.0.2/apache-storm-1.0.2.tar.gz | tar --extract --gzip
cd ..

# ./gradlew test --stacktrace ## tests do not work right now

# redis
sudo apt-get install redis-server
# or get it from https://redis.io/

```

Setting up a Storm Cluster
--------------------------

Adapted from [Link](http://storm.apache.org/releases/current/Setting-up-a-Storm-cluster.html)

```
./bin/kafka_2.11-0.10.1.0/bin/zookeeper-server-start.sh ./conf/zookeeper.properties &
    sleep 5
./bin/kafka_2.11-0.10.1.0/bin/kafka-server-start.sh ./conf/server.properties &
    sleep 5
./bin/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
./bin/apache-storm-1.0.2/bin/storm nimbus & 
    sleep 5
./bin/apache-storm-1.0.2/bin/storm supervisor & 
     sleep 5
./bin/apache-storm-1.0.2/bin/storm ui &
    sleep 5
./bin/apache-storm-1.0.2/bin/storm logviewer &
    sleep 5
```

Open [WebInterface on localhost:8080](http://localhost:8080)

Submitting the Topology to the cluster
--------------------------------------
```
./gradlew assemble
./bin/apache-storm-1.0.2/bin/storm jar build/libs/stream-processing-0.1-SNAPSHOT.jar at.ac.tuwien.aic.streamprocessing.storm.StreamProcessingTopology TestName

# monitor a single component
./bin/apache-storm-1.0.2/bin/storm monitor taxicab-0_0_1 -m w-calculate-speed-bolt

./bin/apache-storm-1.0.2/bin/storm kill taxicab-0_0_1
# ^^^ will wait for topology.message.timeout.secs (30s) to allow finish processing
```

Fixes
-----------------------

```
# e.g.  Nimbus Leader NotFoundException
rm -rf /tmp/kafka-logs
./bin/kafka_2.11-0.10.1.0/bin/zookeper-shell.sh localhost:2181 rmr /brokers
```

```
2016-11-26 19:20:14.834 STDERR [INFO] Caused by: java.lang.RuntimeException: java.io.IOException: Found multiple defaults.yaml resources. You're probably bundling the Storm jars with your topology jar. [jar:file:/home/kern/Code/G2T1/apache-storm-1.0.2/lib/storm-core-1.0.2.jar!/defaults.yaml, jar:file:/home/kern/Code/G2T1/apache-storm-1.0.2/storm-local/supervisor/stormdist/taxicab-0_0_1-1-1480184405/stormjar.jar!/defaults.yaml]
# don't run StormSubmitter from IDEA
# run ./bin/apache-storm/bin/storm executable to submit the jar
```

Die `NoSuchElementException` kommt wenn man zuwenige `Values` aus einem `Operator` emitted als in der Topologie gefordert.



Testdata
--------------------------

The testdata (merged, sorted) can be downloaded through [this link](https://onedrive.live.com/download?cid=B692A0DA79689448&resid=B692A0DA79689448%211976&authkey=ADhMxFK3JFPf0Jw) OR [this link] (https://drive.google.com/uc?export=download&confirm=ha6G&id=0B5_xoVJ9kw95QkNESFM0OHM2RlE)



The workflow how the original test-data has been merged and sorted
```
# 0. Install sqlite3
sudo apt install sqlite3

# 1. Navigate to downloaded and extracted original test-data and merge them
copy /b *.txt testData_merged_unsorted.csv

# 2. Import test-data into sqlite 
sqlite3 taxiData < script.sqlite

#where script.sqlite contains:
create table taxiData(id integer, timestamp datetime, lat decimal, long decimal);
.mode csv
.import 'testData_merged_unsorted.csv' TaxiData.db

# 3. Output query to file
sqlite3.exe -csv taxiData "SELECT * FROM taxiData ORDER BY 2" > testData_merged_sorted.csv

```
Redis Deskop Manager
---------
Simple UI tool for managing redis instances.
```
wget https://github.com/uglide/RedisDesktopManager/releases/download/0.9.0-alpha2/redis-desktop-manager_0.9.0.26_amd64.deb
sudo dpkg -i redis-desktop-manager_0.9.0.26_amd64.deb

#in case of dependcy errors while running above
sudo apt-get -f install

#start tool with
redis-desktop-manager
```

Dashboard
---------
Building
```
sudo apt install npm
cd src/main/resources/dashboard/
npm install
npm setup
```

Running
```
npm start
```
Fixes for potential issues
```
#/usr/bin/env: node: No such file or directory
ln -s /usr/bin/nodejs /usr/bin/node
#then retry

#"Cannot be run with sudo" message when exectuing sudo bower install
# retry wiht --allow-root option
sudo bower install --allow-root 
```

VM setup
--------
Instructions to create the Virtual Machine as required for hand in.
```sh
sudo apt install virtualbox-qt cloud-image-utils

# adapted from https://gist.github.com/smoser/6066204

wget https://cloud-images.ubuntu.com/xenial/current/xenial-server-cloudimg-amd64-disk1.vmdk

VBoxManage clonehd \
  --format VDI  \
  xenial-server-cloudimg-amd64-disk1.vmdk \
  g2t1.vdi ;

cat > my-user-data <<EOF
#cloud-config
debug: True
disable_root: false
lock_passwd: false
password: ubuntu
chpasswd: 
  list: |
    root:root
    ubuntu:ubuntu
  expire: False
ssh_pwauth: true
package_upgrade: true
apt_sources:
 - source: "ppa:cwchien/gradle"
packages:
 - virtualbox-guest-x11
 - gradle
 - default-jdk
 - virtualbox-guest-dkms
 - ubuntu-desktop
 - firefox
 - redis-server 
 - openjfx
 - npm 
 - nodejs-legacy
power_state:
 mode: poweroff
 message: Bye Bye
EOF

cloud-localds my-seed.iso my-user-data

VBoxManage createvm --name g2t1 --ostype Ubuntu_64 --register
VBoxManage modifyvm g2t1 --memory 4096 # VBoxManage modifyvm g2t1 --memory 2048 # 
VBoxManage storagectl g2t1 --name "SATA Controller" --add sata --controller IntelAHCI --portcount 2
VBoxManage storageattach g2t1 --storagectl "SATA Controller" --port 0 --device 0 --type hdd --medium g2t1.vdi
VBoxManage storageattach g2t1 --storagectl "SATA Controller" --port 1 --device 0 --type dvddrive --medium my-seed.iso
VBoxManage modifyvm g2t1 --uart1 0x3F8
VBoxManage startvm g2t1
## wait for vm to shutdown

# test for success
VBoxManage startvm g2t1
VBoxManage guestcontrol g2t1 run "/bin/date" --username root  --password root
VBoxManage controlvm g2t1 poweroff 

# fix /etc/hosts file
VBoxManage startvm g2t1
VBoxManage guestcontrol g2t1 run "/bin/sh" --username root  --password root -- -c "cd /home/ubuntu/  \
  ; echo "127.0.0.1 ubuntu" >> /etc/hosts \
  ; shutdown -h now
  ;"

# import project

git archive `git rev-parse --abbrev-ref HEAD` --output project.tar

VBoxManage sharedfolder add g2t1 --name project --hostpath `pwd` --automount
VBoxManage startvm g2t1
VBoxManage guestcontrol g2t1 run "/bin/sh" --username root  --password root -- -c "cd /home/ubuntu/  \
  ; cp /media/sf_project/project.tar /home/ubuntu/project.tar \
  ; chown ubuntu:ubuntu /home/ubuntu/project.tar \
  ;"

VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/ \
  ; tar --extract --file project.tar \
  ; mkdir bin \
  ; cd bin \
  ; curl http://www-eu.apache.org/dist/kafka/0.10.1.0/kafka_2.11-0.10.1.0.tgz | tar --extract --gzip \
  ; curl http://www-eu.apache.org/dist/storm/apache-storm-1.0.2/apache-storm-1.0.2.tar.gz | tar --extract --gzip \
  ; cd ..  \
  ;"

VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/ \
  ; sudo apt install npm nodejs-legacy \
  ; cd src/main/resources/dashboard/ \
  ; npm install \
  ; npm setup \
  ;"

# run tests
VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/;  \
  ./gradlew test
"

# fetch demodata
VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/  \
  ; https://www.dropbox.com/sh/rv7uambq691s44l/AAAT2N5W0EA-pJKkUp5Nqj_ba/taxi_sub_data.zip \
  ; unzip taxi_sub_data.zip \
  ; mv taxi_sub_data.csv testdata/taxi_data.csv \
  ;"

## or https://www.dropbox.com/sh/rv7uambq691s44l/AADHIDF0ozwcTjGJ0bMHAZdma/taxi_data.zip

VBoxManage startvm g2t1
VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/  \
  ; echo '~/gradlew runTopology' >> ~/Desktop/startTopo.sh \
  ; echo '~/gradlew runDataprovider -Pspeed=100000' >> ~/Desktop/startProvider.sh \
  ; echo 'cd ~/src/main/resources/dashboard/ ; npm start' >> ~/Desktop/startWebserver.sh \
  ; echo 'firefox localhost:3000' >> ~/Desktop/runFirefox.sh \
  ; chmod a+x ~/Desktop/*.sh \
  ;"

# TODO reset demodata
# runTopology; startDashboard; runDataprovider -Pspeed=100000
# test image import; test the vdi file
# TODO
echo "username: root \n password: root" >> g2t1.txt
```

run the test machine
```sh
VBoxManage startvm g2t1
# login username: ubuntu; password: ubuntu
VBoxManage controlvm g2t1 poweroff 
```

cleanup

```sh
# delete vm and file
VBoxManage sharedfolder  remove g2t1 --name project

VBoxManage unregistervm g2t1 --delete
rm xenial-server-cloudimg-amd64-disk1.vmdk
```

fixes

```sh
# apt install junit
systemctl redis-server stop
sudo chown --recursive ubuntu:ubuntu ./src/
./gradlew clean build

./gradlew wrapper --gradle-version=3.3
./gradlew wrapper
./gradlew clean build

# for testing  install redis desktop-manager

VBoxManage guestcontrol g2t1 run "/bin/sh" --username ubuntu  --password ubuntu -- -c "cd /home/ubuntu/  \
  ; wget https://github.com/uglide/RedisDesktopManager/releases/download/0.9.0-alpha2/redis-desktop-manager_0.9.0.26_amd64.deb \
  ; sudo dpkg -i redis-desktop-manager_0.9.0.26_amd64.deb \
  ; sudo apt -f --yes install \
  ; "
  
# npm troubles

sudo rm -rf /home/ubuntu/src/main/resources/dashboard/bower_components
sudo rm -rf /home/ubuntu/src/main/resources/dashboard/node_modules

```

Resources
---------
[SlideShare with good overview](http://www.slideshare.net/qiozas/big-data-streaming-processing-using-apache-storm-fosscomm-2016?next_slideshow=1)
[Trident Storm Tutorial with Votes](https://chawlasumit.wordpress.com/2015/08/02/how-to-manage-state-in-trident-storm-topologies/)
