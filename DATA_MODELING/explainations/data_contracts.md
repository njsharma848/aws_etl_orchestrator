Data contracts in data modeling are formal agreements that define the structure, quality, and semantics of data being shared between systems or teams. Think of them as an SLA (Service Level Agreement) for data.

## Core Concept

A data contract explicitly specifies what data producers promise to deliver and what data consumers can expect to receive. This includes the schema, data types, validation rules, freshness guarantees, and semantic meaning of the data.

## Key Components

Typical data contracts include:

- **Schema definition**: Field names, data types, required vs. optional fields
- **Data quality rules**: Constraints like "customer_id cannot be null" or "order_total must be positive"
- **Semantic meaning**: Clear definitions of what each field represents
- **SLAs**: Delivery frequency, latency expectations, availability guarantees
- **Versioning**: How changes will be managed and communicated
- **Ownership**: Who's responsible for maintaining the data

## Why They Matter

Data contracts help prevent the "breaking changes" problem common in data pipelines. Without them, an upstream team might rename a column or change a data type, unknowingly breaking downstream analytics or ML models. With contracts in place, producers commit to either maintaining backwards compatibility or following a formal deprecation process.

## Practical Example

Instead of a producer just sending customer data however they want, a data contract might specify: "The `customers` table will have a `customer_id` (integer, non-null, unique), `email` (string, validated format), and `created_at` (timestamp, UTC), refreshed daily by 6 AM."

This approach brings software engineering principles—like API contracts—into the data world, making data infrastructure more reliable and maintainable.