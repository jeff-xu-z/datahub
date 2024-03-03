# Intro

[This Miro diagram](https://miro.com/app/board/uXjVNqY-J2I=/?share_link_id=729782654545) might help you to understand the containers that power Datahub at runtime.

[This slide deck ](https://docs.google.com/presentation/d/1opGrGqG4-QyYtaupxcnatpoGQ3wmVz9s62hbpg3q_o0/edit?usp=sharing) summarizes my evaluation.

## Quick start

The top level ***docker*** folder is where you should look into to run datahub as containers.

### Run datahub on Mac M1

If you just want to experiment datahub quickly, you can run the command below to **use images from Docker Hub** (http://localhost:9002).

```
docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
```

### Ingest demo metadata

**STEP 1:** pip install datahub

Follow [this instruction](https://datahubproject.io/docs/cli/#installation) to have datahub installed in your Python3 environment. It's up to you to choose whether or not to use virtual-env.

**STEP 2:** load demo metadata

1. You should have already cloned [pharos-goku-datahub repo](https://ghe.megaleo.com/dpoe/pharos-goku-datahub).
2. Datahub is running.
3. Run commands below and the demo data should be ingested in a few minutes.

```
cd <pharos-goku-datahub>
./goku/quickstart/metadata/load.sh
```

# Build and run (local version)

This section is for **datahub developer** who builds and runs locally built binaries.

Building datahub depends on JDK and Python. The latest datahub code needs **JDK 17** + **Python 3.10** while earlier versions (v0.12.0) might require JDK 11. Your local build environment should have setup as below (captured from my Mac M1).

```
# gradle

$ which gradle
/opt/homebrew/bin/gradle

# JDK

$ which java
/opt/homebrew/opt/openjdk@17/bin/java

$ java -version
openjdk version "17.0.9" 2023-10-17
OpenJDK Runtime Environment Homebrew (build 17.0.9+0)
OpenJDK 64-Bit Server VM Homebrew (build 17.0.9+0, mixed mode, sharing)

# Python 3.10

$ which python3
/opt/homebrew/bin/python3

$ python3 -c "import sys; print(sys.version)"
3.10.13 (main, Aug 24 2023, 22:36:46) [Clang 14.0.3 (clang-1403.0.22.14.1)]

```
## Build locally

### Build on Mac M1

Simply run ```make -f goku/Makefile build``` and you should have the following container images created.

```
$ docker image list
REPOSITORY                                                                              TAG             IMAGE ID       CREATED          SIZE
datahub-frontend-react                                                                  head            2ffbfab5be62   22 minutes ago   951MB
datahub-gms                                                                             head            76ff10c3ec4a   23 minutes ago   503MB
datahub-upgrade                                                                         head            61ff1e45af41   23 minutes ago   568MB
datahub-elasticsearch-setup                                                             head            c076ea9cd9dd   24 minutes ago   28.7MB
datahub-mysql-setup                                                                     head            7a3378153415   25 minutes ago   104MB
datahub-kafka-setup                                                                     head            be4c72802f43   26 minutes ago   588MB
```

## Run locally

### Run on Mac M1

- Command ```make -f goku/Makefile start-quickstart-mac-m1``` starts datahub using your locally built images.
- Command ```make -f goku/Makefile stop-quickstart-mac-m1``` stops datahub containers gracefully.

```
$ make -f goku/Makefile start-quickstart-mac-m1
Login to docker-dev-artifactory.workday.com ...
...

[+] Running 12/12
 ✔ Network datahub_network                        Created                                                                                                                                                              0.0s 
 ✔ Container quickstart-mysql-1                   Healthy                                                                                                                                                              0.8s 
 ✔ Container quickstart-zookeeper-1               Healthy                                                                                                                                                              0.8s 
 ✔ Container quickstart-elasticsearch-1           Healthy                                                                                                                                                              0.8s 
 ✔ Container quickstart-elasticsearch-setup-1     Exited                                                                                                                                                               0.1s 
 ✔ Container quickstart-mysql-setup-1             Exited                                                                                                                                                               0.1s 
 ✔ Container quickstart-broker-1                  Healthy                                                                                                                                                              0.1s 
 ✔ Container quickstart-schema-registry-1         Healthy                                                                                                                                                              0.0s 
 ✔ Container quickstart-kafka-setup-1             Exited                                                                                                                                                               0.0s 
 ✔ Container quickstart-datahub-upgrade-1         Exited                                                                                                                                                               0.0s 
 ✔ Container quickstart-datahub-gms-1             Healthy                                                                                                                                                              0.0s 
 ✔ Container quickstart-datahub-frontend-react-1  Started                              
 ```

Your datahub should use locally built images, plus additional dependencies pulled from Workday dev artifactory (except mariadb). 

```
AQ4JR76DJ7W:datahub jeff.xu$ docker image list 
REPOSITORY                                                           TAG       IMAGE ID       CREATED        SIZE
datahub-frontend-react                                               head      23d5b3a09457   33 hours ago   951MB
datahub-gms                                                          head      72b2a0a6fb09   33 hours ago   503MB
datahub-upgrade                                                      head      d6b8aed88211   33 hours ago   568MB
datahub-elasticsearch-setup                                          head      c076ea9cd9dd   2 days ago     28.7MB
datahub-mysql-setup                                                  head      7a3378153415   2 days ago     104MB
datahub-kafka-setup                                                  head      be4c72802f43   2 days ago     588MB
docker-dev-artifactory.workday.com/ambt/artifactory-client           latest    5e8d4aaa35e8   8 days ago     608MB
docker-dev-artifactory.workday.com/elasticsearch/elasticsearch       7.17.18   21bc9f75815d   4 weeks ago    628MB
docker-dev-artifactory.workday.com/confluentinc/cp-schema-registry   7.5.3     7fb754a15eba   2 months ago   1.97GB
docker-dev-artifactory.workday.com/confluentinc/cp-kafka             7.4.0     3647af234321   9 months ago   864MB
docker-dev-artifactory.workday.com/confluentinc/cp-zookeeper         7.4.0     5c6143de34a4   9 months ago   864MB
mariadb                                                              10.5.8    dd3ab601101c   3 years ago    392MB
```

Here's a reference of how to stop the running datahub service.

```
AQ4JR76DJ7W:datahub jeff.xu$ make -f goku/Makefile stop-quickstart-mac-m1
docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml down

[+] Running 0/0
[+] Running 12/11kstart-datahub-frontend-react-1  Stopping                                                                                                                                                             0.1s 
 ✔ Container quickstart-datahub-frontend-react-1  Removed                                                                                                                                                              0.7s 
 ✔ Container quickstart-datahub-gms-1             Removed                                                                                                                                                              1.0s 
 ✔ Container quickstart-datahub-upgrade-1         Removed                                                                                                                                                              0.0s 
 ✔ Container quickstart-mysql-setup-1             Removed                                                                                                                                                              0.0s 
 ✔ Container quickstart-kafka-setup-1             Removed                                                                                                                                                              0.0s 
 ✔ Container quickstart-elasticsearch-setup-1     Removed                                                                                                                                                              0.0s 
 ✔ Container quickstart-schema-registry-1         Removed                                                                                                                                                              0.9s 
 ✔ Container quickstart-mysql-1                   Removed                                                                                                                                                              1.0s 
 ✔ Container quickstart-elasticsearch-1           Removed                                                                                                                                                              0.8s 
 ✔ Container quickstart-broker-1                  Removed                                                                                                                                                              0.9s 
 ✔ Container quickstart-zookeeper-1               Removed                                                                                                                                                              0.5s 
 ✔ Network datahub_network                        Removed                                                                                                                                                              0.1s 
```

# Build (Jenkins)

# Deploy in AWS

