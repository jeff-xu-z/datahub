# Intro

[This Miro diagram](https://miro.com/app/board/uXjVNqY-J2I=/?share_link_id=729782654545) might help you to understand the containers that power Datahub at runtime.

[This slide deck ](https://docs.google.com/presentation/d/1opGrGqG4-QyYtaupxcnatpoGQ3wmVz9s62hbpg3q_o0/edit?usp=sharing) summarizes my evaluation.

## Quick start using official images from docker hub

The top level ***docker*** folder is where you should look into to run datahub as containers.

#### Run datahub on Mac M1

If you just want to experiment datahub quickly, you can run the command below to **use images from Docker Hub** (http://localhost:9002).

```
docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml up -d
```

#### Ingest demo metadata

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

#### Stop datahub on Mac M1

```
docker-compose -f docker/quickstart/docker-compose-without-neo4j-m1.quickstart.yml down
```

# Build and run (local version)

This section is for **datahub developer** who builds and runs locally built binaries.

## Build environment

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

## Understand datahub build

We only need to build a few components (gms, frontend and upgrade) by ourselves and leverage docker hub official images for other components.

Building a datahub component involves two steps. We use datahub-gms as example.

1. Compile source code to generate the component's distribution artifact ($DATAHUB/metadata-service/war/build/libs/war.war).
1. Build docker image from the component's distribution artifacts ($DATAHUB/docker/datahub-gms/Dockerfile).

You can refer to following targets in ```goku/Makefile``` for details.

1. ```local-build-datahub-gms```
1. ```local-build-datahub-frontend-react```
1. ```local-build-datahub-upgrade```
1. ```build-datahub-docker-images```

## Build and run locally on Mac M1

This section is for developer who have made code changes and want to test things out locally.

1. Run ```make -f goku/Makefile <target>```, where target is one of the following values (depending on which module you've made changes).
   1. local-build-datahub-gms
   1. local-build-datahub-frontend-react
   1. local-build-datahub-upgrade
1. Whence previous step passes, you run the following two steps to build datahub locally.
   1. ```make -f goku/Makefile local-build-datahub```
   1. ```make -f goku/Makefile build-datahub-docker-images```
1. Now you can launch datahub with your local container images by running ```make -f goku/Makefile start-datahub-mac-m1```.
1. To stop datahub, run ```make -f goku/Makefile stop-datahub-mac-m1```.
