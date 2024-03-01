# Intro

[This Miro diagram](https://miro.com/app/board/uXjVNqY-J2I=/?share_link_id=729782654545) might help you to understand the containers that power Datahub at runtime.

[This slide deck ](https://docs.google.com/presentation/d/1opGrGqG4-QyYtaupxcnatpoGQ3wmVz9s62hbpg3q_o0/edit?usp=sharing) summarizes my evaluation.

## Quick start

The top level ***docker*** folder is where you should look into to run datahub as containers.

### Run datahub on Mac M1

You can run the command below to **use images from Docker Hub** to launch datahub (http://localhost:9002).

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

Building datahub depends on JDK and Python. The latest datahub code needs JDK 17 + Python 3.10 while earlier versions (v0.12.0) might require JDK 11. Your local build environment should have setup like the sample as below (captured from my Mac M1).

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

- Command ```make -f goku/Makefile run-local-m1``` launches datahub using your locally built images.
- Command ```make -f goku/Makefile stop-local-m1``` stops datahub containers gracefully.

```
$ make -f goku/Makefile run-local-m1
DATAHUB_FRONTEND_IMAGE=datahub-frontend-react \
	DATAHUB_GMS_IMAGE=datahub-gms \
	DATAHUB_UPGRADE_IMAGE=datahub-upgrade \
	DATAHUB_ELASTIC_SETUP_IMAGE=datahub-elasticsearch-setup \
	DATAHUB_KAFKA_SETUP_IMAGE=datahub-kafka-setup \
	DATAHUB_MYSQL_SETUP_IMAGE=datahub-mysql-setup \
	docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
[+] Running 70/6
 ✔ elasticsearch 9 layers [⣿⣿⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                                             52.1s 
 ✔ schema-registry 11 layers [⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                                        40.2s 
 ✔ datahub-actions 28 layers [⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                       61.4s 
 ✔ zookeeper 2 layers [⣿⣿]      0B/0B      Pulled                                                                                                                                        38.5s 
 ✔ mysql 12 layers [⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿]      0B/0B      Pulled                                                                                                                                 43.8s 
 ✔ broker 2 layers [⣿⣿]      0B/0B      Pulled                                                                                                                                           15.1s 
[+] Building 0.0s (0/0)                                                                                                                                                   docker:desktop-linux
[+] Running 13/13
 ✔ Network datahub_network                        Created                                                                                                                                 0.0s 
 ✔ Container quickstart-mysql-1                   Healthy                                                                                                                                 1.3s 
 ✔ Container quickstart-elasticsearch-1           Healthy                                                                                                                                 1.3s 
 ✔ Container quickstart-zookeeper-1               Healthy                                                                                                                                 1.3s 
 ✔ Container quickstart-elasticsearch-setup-1     Exited                                                                                                                                  0.1s 
 ✔ Container quickstart-mysql-setup-1             Exited                                                                                                                                  0.1s 
 ✔ Container quickstart-broker-1                  Healthy                                                                                                                                 0.1s 
 ✔ Container quickstart-schema-registry-1         Healthy                                                                                                                                 0.0s 
 ✔ Container quickstart-kafka-setup-1             Exited                                                                                                                                  0.0s 
 ✔ Container quickstart-datahub-upgrade-1         Exited                                                                                                                                  0.0s 
 ✔ Container quickstart-datahub-gms-1             Healthy                                                                                                                                 0.0s 
 ✔ Container quickstart-datahub-actions-1         Started                                                                                                                                 0.0s 
 ✔ Container quickstart-datahub-frontend-react-1  Started                                                                                                                                 0.0s 
 ```


# Build (Jenkins)

# Deploy in AWS

