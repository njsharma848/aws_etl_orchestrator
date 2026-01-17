# Slowly Changing Dimensions (SCD) in Data Warehousing

In data warehousing, Slowly Changing Dimensions (SCD) are techniques for handling changes to dimension data over time. Here's how each type works:

## SCD Type 0 - No Changes Allowed

**SCD Type 0** is the simplest approach where no changes are allowed. Once a dimension record is created, it remains static and never gets updated. 

- **Use Case**: Truly fixed attributes like a person's date of birth or an original hire date
- **Behavior**: If source data changes, those changes are simply ignored in the warehouse
- **Advantages**: Simple, no complexity
- **Disadvantages**: Cannot track any changes

---

## SCD Type 1 - Overwrite

**SCD Type 1** overwrites old data with new data, keeping only the current value. When a customer moves to a new address, for example, you would simply update the address field with the new value, losing all history of the previous address.

- **Use Case**: When you don't need to preserve history or when making corrections to incorrect data
- **Behavior**: Updates existing record in place
- **Advantages**: 
  - Straightforward implementation
  - Saves storage space
  - Simple queries
- **Disadvantages**: 
  - Loses all historical data
  - Cannot report on past states

---

## SCD Type 2 - Add New Row

**SCD Type 2** preserves full history by creating a new record for each change. This is the most common approach when historical tracking is important.

Each record typically includes:
- Effective date (start date)
- End date (or expiration date)
- Current flag (to identify active record)
- Version number (optional)

**Example**: If a customer moves, you'd keep the old record with an end date and create a new record with the current address and a new start date.

- **Use Case**: When historical accuracy is important for reporting and analysis
- **Behavior**: Creates new record for each change, marks old record as historical
- **Advantages**: 
  - Complete history preservation
  - Accurate historical reporting
  - Point-in-time analysis capability
- **Disadvantages**: 
  - Larger table sizes
  - More complex queries (need to filter on current flag or date ranges)
  - Requires surrogate keys

---

## SCD Type 3 - Add New Column

**SCD Type 3** tracks limited history by adding new columns to store previous values. For instance, you might have "Current_Address" and "Previous_Address" columns.

**Example**: When an address changes:
- Current address moves to the "Previous_Address" column
- New address goes into the "Current_Address" column

- **Use Case**: When you only need to track one or two previous values
- **Behavior**: Adds columns for previous values
- **Advantages**: 
  - Some history without multiple records
  - Simple queries
  - Fixed table size
- **Disadvantages**: 
  - Limited history (typically only 1-2 previous values)
  - Requires schema changes when adding tracking for new attributes
  - Not scalable for complete history

---

## SCD Type 4 - Historical Table

**SCD Type 4** uses a hybrid approach with separate current and history tables. Instead of managing everything in a single dimension table, you maintain:

1. **Current table** - Contains only the most recent, active version of each dimension record
2. **History table** - Stores all historical versions of the records with their effective date ranges

**Example**: With customer data:
- `Customer` table with current information
- `Customer_History` table that tracks all changes over time

When a customer's address changes:
- Update the current table with the new address
- Insert the old record into the history table with appropriate effective dates

- **Use Case**: High query volumes against current data with need for complete history
- **Behavior**: Maintains two separate tables - one for current, one for history
- **Advantages**: 
  - Current table stays small and performs well
  - Complete history preserved in separate table
  - Clean separation of concerns
  - Optimized for most common query pattern (current data)
- **Disadvantages**: 
  - Increased complexity in managing two tables
  - Need to ensure synchronization between tables
  - More complex queries when combining current and historical data
  - Requires UNION queries for complete view

---

## Summary Comparison

| Type | History Tracking | Storage Impact | Query Complexity | Common Use |
|------|-----------------|----------------|------------------|------------|
| Type 0 | None | Minimal | Simple | Fixed attributes |
| Type 1 | None | Minimal | Simple | No history needed |
| Type 2 | Complete | High | Moderate | Full history required |
| Type 3 | Limited | Low | Simple | Recent changes only |
| Type 4 | Complete | High | Moderate-High | Performance + history |

---

## Choosing the Right SCD Type

The choice depends on your business requirements:

- **Need complete history?** → Type 2 or Type 4
- **Only current values matter?** → Type 1
- **Need to track just one previous value?** → Type 3
- **Attribute should never change?** → Type 0
- **High query volume on current data + complete history?** → Type 4
- **Standard historical tracking?** → Type 2 (most common)