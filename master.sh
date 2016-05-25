#!/bin/sh
CONFIG=/master.conf
JAR_PACKAGE=target/load-distributor-0.1.jar

# run
java -Dconfig.resource=$CONFIG -Ddiscover.hostname=127.0.0.1 -Ddiscover.port=9000 -jar $JAR_PACKAGE
