# NoSQL Justification Report

## Section A: Limitations of RDBMS

### 1. Products having different attributes (e.g., `laptops have RAM/processor, shoes have size/color`)
- Relational databases require a `fixed table structure`.
- Products like `laptops, shoes`, and `mobiles` have different attributes.
- Handling this requires many `nullable columns or multiple tables`.
- This increases `complexity` and `wastes storage`.

### 2. Frequent schema changes when adding new product types
- Adding new product types requires `altering table schemas`.
- Schema changes can `impact existing data` and `applications`.
- `Downtime` may be `required` during schema updates.
- This `reduces agility` in fast-changing business environments.

### 3. Storing customer reviews as nested data
- RDBMS stores data in `normalized tables`.
- Reviews need separate tables linked using `foreign keys`.
- Fetching products with reviews requires `complex joins`.
- `Performance degrades` as data `volume grows`.



## Section B: NoSQL Benefits

### 1. Flexible schema (document structure)
- MongoDB uses a `document-based structure`.
- Each product can have `different attributes`.
- New fields can be added `without schema migration`.
- Ideal for `diverse and evolving product catalogs`.

### 2. Embedded documents (reviews within products)
- Customer reviews can be stored inside `product documents`.
- `Eliminates` the need for joins.
- `Faster` read `operations`.
- Data is more `intuitive and closer` to real-world representation.

### 3. Horizontal Scalability
- MongoDB supports `sharding`.
- Data is `distributed` across multiple servers.
- Handles `large-scale` data efficiently.
- Suitable for `high-traffic` applications.



## Section C: Trade-offs

### 1. Limited ACID Transactions
- MongoDB has `limited` multi-document transaction support.
- Not ideal for `complex transactional` systems.
- Risky for `financial or banking` applications.

### 2. Data Redundancy
- Embedded documents may `duplicate data`.
- `Increases` storage usage.
- Requires careful data `consistency management`.

