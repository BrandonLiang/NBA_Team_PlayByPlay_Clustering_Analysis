#!/bin/bash
scalac -cp ".:/usr/local/Cellar/apache-spark/2.0.1/libexec/jars/*" Multilayer_Perceptron_NN.scala
scala -J-Xmx4g -cp ".:/usr/local/Cellar/apache-spark/2.0.1/libexec/jars/*" Multilayer_Perceptron_NN $1
