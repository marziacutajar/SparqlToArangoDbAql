# SPARQL query to ArangoDB AQL query translator

This command-line program is a prototype implementation for translating a given SPARQL query into an ArangoDB AQL query. 
It is meant for querying RDF data transformed into an ArangoDB-compliant JSON format using the RDF-to-ArangoDB tool that can be found at https://github.com/Ponsietta/RdfToArangoDBJson, and stored in ArangoDB. 

This tool loads a SPARQL query from a given file, transforms it into an equivalent AQL query, and saves the latter to file for the user to use as required.

The program is also able to run the produced AQL query on a user-specified ArangoDB database and output the data results obtained, as well as the measured query runtime, to CSV file. Moreover, it can also execute the original SPARQL query against a user-specified Virtuoso database and, also in this case, the data results and measured query runtime is saved to CSV file. This has only been done for performance evaluation, i.e. to compare the performance of ArangoDB against a traditional RDF store. 

If you do not wish to run the queries against the databases directly in the program, please omit the related command-line parameters described in the below "Running the program" section.

The database connection settings for both ArangoDB and Virtuoso are set in the config.properties file. If you want the program to execute the queries, kindly update them as required before running the program. This file also contains four properties for specifying the names of the ArangoDB collections storing your transformed RDF data, as these are required for generating the AQL queries. As per the RDF-to-ArangoDB tool, one property is for specifying the name of the collection storing data transformed using the Basic Approach, while the other three properties are for specifying the names of the two vertex collections and the edge collection storing data transformed using the Graph Approach. Kindly update these properties to reflect the name(s) of the collection(s) you are using.

Please note that currently the tool only supports the SELECT query form of the SPARQL query language.

## Running the program

The command-line program expects two mandatory input parameters as below:
- -f <file_directory> : Path to a text file containing a SPARQL query, or a directory path such that all the text files containing valid SPARQL queries within the directory are processed and transformed
- -m <data_model> : The approach that was used to transform the RDF data using the RDF-to-ArangoDB tool. The value for this parameter must be 'D' if the Basic Approach was used, or 'G' if the Graph Approach was used

You can also specify the below two optional parameters if desired:
- -runOnArangoDb : If this argument is present, all generated AQL queries will be executed against the configured ArangoDB database
- -runOnVirtuoso : If this argument is present, all SPARQL queries will be executed against the configured Virtuoso database

To run the program easily without an IDE, you need to make sure to have the Gradle build tool installed. The program has been built and run succesfully with Gradle version 6.0.1, thus newer backward-compatible versions should also work. Refer to https://gradle.org/install/ for download and installation details.

The program can then be built and run with a single command as below:

    gradle run --args="-f='C:\Users\marzia\Documents\SPARQL queries\query1.txt' -m=D"
    
Please note that this command, as well as all others given below, work on a Windows OS. Please modify accordingly if using any other operating system.

Another option is to create a fat JAR file using Gradle by executing the below in command-line:

    gradle fatJar
    
The jar file will be saved to the build\libs directory within the main project directory. To run the jar file, navigate to the directory containing the file and run as follows:

    cd build\libs

    java -jar SPARQL-to-AQL-Transformer.jar -f "C:\Users\marzia\Documents\SPARQL queries\query1.txt" -m D

## Program Outputs

The program outputs all generated AQL queries to file, as well as the results of query executions.

The tool creates three directories within the main project directory:
- transformed_queries - upon transforming a SPARQL query, a text file containing the created AQL query is stored here
- query_results - CSV files containing AQL query results are saved here
- runtime_results - query runtimes are saved in CSV files in this directory

