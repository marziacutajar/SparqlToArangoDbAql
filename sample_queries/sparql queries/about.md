| Query         | Description     | 
| ------------- |:-------------:| 
| Q1            | Find products for a given set of generic features -- touches a large amount of data and uses \texttt{ORDER BY} and \texttt{LIMIT} | 
| Q2            | Retrieve basic information about a specific product -- touches only a small amount of data, contains many triple patterns, and uses \texttt{OPTIONAL} graph patterns      | 
| Q3            | Find products having some specific features and not having one feature -- uses negation, \texttt{ORDER BY} and \texttt{LIMIT}      |   
| Q4            | Find products matching two different sets of features -- uses \texttt{UNION}, \texttt{ORDER BY}, \texttt{LIMIT} and \texttt{OFFSET} |
| Q5            | Find products that are similar to a given product -- touches a large amount of data, uses complex \texttt{FILTER} conditions involving arithmetic operations, uses \texttt{LIMIT} |
| Q6            | Retrieve in-depth information about a specific product including offers and reviews -- touches a large amount of data including products, offers, vendors, reviews and reviewers, uses \texttt{OPTIONAL} graph patterns |
| Q7            | Get recent reviews in English for a specific product -- uses \texttt{langMatches} function, \texttt{ORDER BY} and \texttt{LIMIT} |
| Q8            | Get all information about an offer -- contains triple patterns with unbound predicates and uses \texttt{UNION} |