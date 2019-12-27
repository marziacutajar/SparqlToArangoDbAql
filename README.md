# SPARQL query expression to ArangoDB AQL query translator

This command-line program is a prototype for translating a given SPARQL query expression into an ArangoDB AQL query expression. 
It is meant for querying RDF data transformed into an ArangoDB-compliant format using the RDF-to-ArangoDB tool that can be found at https://github.com/Ponsietta/RdfToArangoDBJson. The program also runs the produced AQL query on a user-specified ArangoDB database and outputs the results obtained in CSV format.

The tool also executes the original SPARQL query against a user-specified Virtuoso database and compares the query runtime against that of 
the AQL query. This has only been done for research purposes and can be deactivated by the user.
#TODO consider accepting two more parameters - one to specify whether the AQL query should be executed, and one to specify whether we should
make the comparison to Virtuoso.

## Running the program

To run the program easily without an IDE, you need to make sure to have the Gradle build tool installed. 
Refer to https://gradle.org/install/ for download and installation details.

The program can then be built and run with a single command as below:
gradle run --args="-f=C:\Users\marzia\Documents\SPARQL queries\query1.txt -m=D"

The command-line program expects two input parameters as below:
-f: Path to a text file containing a SPARQL query, or a directory path such that all the text files containing valid SPARQL queries within the directory are processed
-m: The approach that was used to transform the RDF data using the RDF-to-ArangoDB tool. The value for this parameter must be 'D' if the Document Approach was used,
or 'G' if the Graph Approach was used

#java -jar SPARQL-to-AQL-Transformer.jar -f "C:\Users\marzia\Documents\SPARQL queries\query.txt" -m D

The tool creates three directories within the main project directory:
- query_results - CSV files containing AQL query results are saved here
- transformed_queries - upon transforming a SPARQL query, a text file containing the created AQL query is stored here
- runtime_results - query runtimes are saved in CSV files in this directory

