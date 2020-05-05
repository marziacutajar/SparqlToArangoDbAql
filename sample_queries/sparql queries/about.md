| Query         | Description                                                                                                        | 
| ------------- |:------------------------------------------------------------------------------------------------------------------:| 
| Q1            | Find products for a given set of generic features - touches a large amount of data and uses ORDER BY and LIMIT | 
| Q2            | Retrieve basic information about a specific product - touches only a small amount of data, contains many triple patterns, and uses OPTIONAL graph patterns      | 
| Q3            | Find products having some specific features and not having one feature - uses negation, ORDER BY and LIMIT      |   
| Q4            | Find products matching two different sets of features - uses UNION, ORDER BY, LIMIT and OFFSET |
| Q5            | Find products that are similar to a given product - touches a large amount of data, uses complex FILTER conditions involving arithmetic operations, uses LIMIT |
| Q6            | Retrieve in-depth information about a specific product including offers and reviews - touches a large amount of data including products, offers, vendors, reviews and reviewers, uses OPTIONAL graph patterns |
| Q7            | Get recent reviews in English for a specific product - uses langMatches function, ORDER BY and LIMIT |
| Q8            | Get all information about an offer - contains triple patterns with unbound predicates and uses UNION |
