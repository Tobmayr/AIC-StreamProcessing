# Advanced Internet Computing Group2 Team1

Toolchain

* Language: Java
* Buildtool: Gradle >= 3.1
* Testcommand: `gradle test --stacktrace`

# Setup on Ubuntu 16.04 LTS

```
sudo add-apt-repository ppa:cwchien/gradle
sudo apt-get update
sudo apt install gradle

git clone git@hyde.infosys.tuwien.ac.at:aic2016/G2T1.git

cd G2T1

gradle test --stacktrace
```
