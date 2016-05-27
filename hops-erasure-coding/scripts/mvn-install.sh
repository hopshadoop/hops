#!/bin/bash
mvn install:install-file -Dfile=target/erasure-coding-1.1-SNAPSHOT-jar-with-dependencies.jar -DgroupId=io.hops -DartifactId=erasure_coding -Dversion=1.0.0 -Dpackaging=jar
