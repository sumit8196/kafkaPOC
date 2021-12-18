#!/usr/bin/env bash
# run the twitter connector
connect-standalone connect-standalone.properties twitter.properties
# OR (linux / mac OSX)
connect-standalone.sh connect-standalone.properties twitter.properties
# OR (Windows)
connect-standalone.bat connect-standalone.properties twitter.properties

@REM Add the jars in classpath
set CLASSPATH=.\libs\annotations-23.0.0.jar;.\libs\connect-utils-0.7.171.jar;.\libs\freemarker-2.3.31.jar;.\libs\guava-23.0.jar;.\libs\jackson-annotations-2.13.0.jar;.\libs\jackson-core-2.12.3.jar;.\libs\jackson-databind-2.12.3.jar;.\libs\javassist-3.19.0-GA.jar;.\libs\kafka-connect-twitter-0.3.34.jar;.\libs\reflections-0.9.10.jar;.\libs\twitter4j-core-4.0.6.jar;.\libs\twitter4j-stream-4.0.6.jar;

@REM running the standalone
@REM Add the jars in classpath
set CLASSPATH=.\libs\annotations-23.0.0.jar;.\libs\connect-utils-0.7.171.jar;.\libs\freemarker-2.3.31.jar;.\libs\guava-23.0.jar;.\libs\jackson-annotations-2.13.0.jar;.\libs\jackson-core-2.12.3.jar;.\libs\jackson-databind-2.12.3.jar;.\libs\javassist-3.19.0-GA.jar;.\libs\kafka-connect-twitter-0.3.34.jar;.\libs\reflections-0.9.10.jar;.\libs\twitter4j-core-4.0.6.jar;.\libs\twitter4j-stream-4.0.6.jar;

@REM running the standalone
connect-standalone connect-standalone.properties twitter.properties