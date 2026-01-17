# Database Normalization Forms

## Difference Between 1NF, 2NF, and 3NF

The three normal forms (1NF, 2NF, and 3NF) are successive levels of database normalization, each building on the previous one to reduce redundancy and improve data integrity.

### First Normal Form (1NF)

**1NF** requires that each column contain only atomic (indivisible) values, and each row must be unique. This means no repeating groups or arrays within cells.

**Example violation:** A "Phone Numbers" column contains multiple phone numbers like "555-1234, 555-5678"

**Solution:** Create separate rows for each phone number or split them into separate columns.

### Second Normal Form (2NF)

**2NF** builds on 1NF by eliminating partial dependencies. This means that every non-key column must depend on the entire primary key, not just part of it. This mainly applies to tables with composite keys (keys made of multiple columns).

**Example violation:** A table with OrderID and ProductID as a composite key stores CustomerName. The CustomerName depends only on OrderID, not on the full composite key.

**Solution:** Split this into separate tables.

### Third Normal Form (3NF)

**3NF** goes further by eliminating transitive dependencies. This means non-key columns shouldn't depend on other non-key columns.

**Example violation:** A table with EmployeeID, DepartmentID, and DepartmentName has DepartmentName depending on DepartmentID (a non-key column), not directly on EmployeeID.

**Solution:** Move DepartmentName to a separate Department table.

### Practical Application

In practice, **3NF is often considered the sweet spot** for most database designs, balancing normalization benefits with practical usability. Each level progressively reduces data redundancy and update anomalies while making relationships between data more explicit.

---

## OLTP vs OLAP: Where Do Normal Forms Apply?

These normalization forms (1NF, 2NF, 3NF) primarily come under **OLTP (Online Transaction Processing)** systems.

### OLTP Systems

**OLTP systems** are designed for day-to-day operational transactions and emphasize:

- Data integrity and consistency
- Fast INSERT, UPDATE, and DELETE operations
- Minimizing data redundancy
- Preventing anomalies during data modifications

This is exactly what normalization achieves, which is why OLTP databases are typically **highly normalized** (often to 3NF or even higher).

### OLAP Systems

**OLAP systems** (Online Analytical Processing) take a different approach:

- Designed for complex queries and analysis, not frequent updates
- Often use **denormalized** structures like star schemas or snowflake schemas
- Denormalization is intentional to improve query performance
- Prioritize read speed over write efficiency
- Data redundancy is acceptable because it reduces the need for complex joins

### Example

In a data warehouse (OLAP), you might store customer names in multiple fact tables even though it's redundant, because it makes queries faster by avoiding joins. This would violate normalization principles but serves the analytical purpose better.

### Summary

- **Normalization** is a core principle of **OLTP** design
- **OLAP** systems often deliberately **denormalize** data to optimize for analytical queries