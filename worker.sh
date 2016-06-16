#!/bin/sh
CONFIG=/worker.conf
JAR_PACKAGE=target/load-distributor-0.1.jar

# run
java -Dconfig.resource=$CONFIG -Dmaster.hostname=127.0.0.1 -Dmaster.port=9000 -jar $JAR_PACKAGE $1
