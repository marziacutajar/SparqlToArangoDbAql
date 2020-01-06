# SPARQL query to ArangoDB AQL query translator

This command-line program is a prototype implementation for translating a given SPARQL query into an ArangoDB AQL query. 
It is meant for querying RDF data transformed into an ArangoDB-compliant JSON format using the RDF-to-ArangoDB tool that can be found at https://github.com/Ponsietta/RdfToArangoDBJson. 

After translating a SPARQL query, the program runs the produced AQL query on a user-specified ArangoDB database and outputs the data results obtained, as well as the measured query runtime, to CSV file. Moreover, it executes the original SPARQL query against a user-specified Virtuoso database and, also in this case, the data results and measured query runtime is saved to CSV file. This has only been done for performance evaluation, i.e. to compare the performance of ArangoDB against a traditional RDF store. If you do not wish to run the queries against the databases directly in the program, simply clone or fork the repository and comment out the relevant lines of code in the Main class.

Currently, the tool only supports the SELECT query form of the SPARQL query language.

## Running the program

To run the program easily without an IDE, you need to make sure to have the Gradle build tool installed. 
Refer to https://gradle.org/install/ for download and installation details.

The program can then be built and run with a single command as below:
    gradle run --args="-f=C:\Users\marzia\Documents\SPARQL queries\query1.txt -m=D"

The command-line program expects two input parameters as below:
-f: Path to a text file containing a SPARQL query, or a directory path such that all the text files containing valid SPARQL queries within the directory are processed and transformed
-m: The approach that was used to transform the RDF data using the RDF-to-ArangoDB tool. The value for this parameter must be 'D' if the Document Approach was used, or 'G' if the Graph Approach was used

#java -jar SPARQL-to-AQL-Transformer.jar -f "C:\Users\marzia\Documents\SPARQL queries\query.txt" -m D

The tool creates three directories within the main project directory:
- query_results - CSV files containing AQL query results are saved here
- transformed_queries - upon transforming a SPARQL query, a text file containing the created AQL query is stored here
- runtime_results - query runtimes are saved in CSV files in this directory

