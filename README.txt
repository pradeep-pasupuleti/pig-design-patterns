This repository contains the Pig Latin scripts, UDFs and datasets used in the book Pig Design Patterns by Pradeep Pasupuleti, published by Packt. All Pig Latin scripts and associated user defined functions are released under the Apache 2.0 license .

The code and the datasets are organized chapter wise, the source files of Java/ Python / R script are placed inside the respective chapters for ease of use and modularity. This modular approach would enable you to modify the source files in a sub-module or chapter without having to worry about breaking the functionality elsewhere. The resources folder contains third-party jars used by the code.

Below are the steps to build this project.

Change to your home directory by executing the below command
$ cd ~

Execute the below command to get a copy of the repository
$ git clone https://github.com/pradeep-pasupuleti/pig-design-patterns.git pdp

Change directory to pdp by executing the below command
$ cd pdp

Use the below command to add cb2java jar file to your local repository
$ mvn install:install-file -Dfile=resources/cb2java0.3.1.jar -DgroupId=cb2java -DartifactId=cb2java -Dversion=0.3.1 -Dpackaging=jar

Use the below command to package the compiled code into jar files and place them in the respective target directories. 
$ mvn package 

Copy the jar files into the folder pdp/jars as the code expects them to be at this path or you can choose to reference these jars by pointing to their corresponding path in the Pig scripts.


NOTE: We have used multiple third-party jar files in our Pig scripts, recompile the source code of these third-party jars if you face issues in using them on the latest versions of Hadoop