In data modeling, **cardinality** refers to the numerical relationship between two entities or tables - essentially, how many instances of one entity can be associated with instances of another entity.

There are three main types of cardinality relationships:

**One-to-One (1:1)**: Each record in Table A relates to exactly one record in Table B, and vice versa. For example, each person has one passport, and each passport belongs to one person.

**One-to-Many (1:N)**: Each record in Table A can relate to multiple records in Table B, but each record in Table B relates to only one record in Table A. For example, one customer can place many orders, but each order belongs to only one customer.

**Many-to-Many (M:N)**: Records in Table A can relate to multiple records in Table B, and records in Table B can relate to multiple records in Table A. For example, students can enroll in many courses, and each course can have many students. This relationship typically requires a junction table (also called a bridge or linking table) to implement in a relational database.

Understanding cardinality is crucial for database design because it determines how you structure your tables, where you place foreign keys, and whether you need junction tables. It directly impacts data integrity, query performance, and how accurately your database represents real-world relationships.