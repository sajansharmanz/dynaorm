# DynaORM

DynaORM is a lightweight, type-safe Object-Relational Mapper (ORM) for Amazon DynamoDB. It's built with TypeScript and Zod, providing a fluent API for defining schemas and interacting with your DynamoDB tables.

## Features ✨

- **Type-Safe Schemas**: Define your data models using Zod, ensuring type safety from schema definition to database interaction.
- **Fluent Query Builder**: Construct complex queries with a chainable, type-safe API.
- **Comprehensive CRUD**: Supports create, findOne, findMany, update, delete, scan, and more.
- **Batch & Transactional Operations**: Seamlessly handle batch and transactional writes and gets for multiple items.
- **Index Support**: Define and use Global and Local Secondary Indexes effortlessly.
- **Throttling**: Built-in support for throttling to manage your DynamoDB request limits.
- **Advanced Query Builder**: Supports key conditions, sort key operators, filters, projections, pagination, consistent reads, and querying secondary indexes.

---

## Installation

```sh
npm install dynaorm zod @aws-sdk/client-dynamodb @aws-sdk/util-dynamodb
```

## Getting Started

### 1. Define Your Schema

Use `defineSchema` from dynaorm and `z` from Zod to create your data model.

```ts
import { defineSchema } from "dynaorm";
import { z } from "zod";

const userSchema = defineSchema({
  tableName: "users",
  partitionKey: "id",
  fields: z.object({
    id: z.string().uuid(),
    username: z.string().min(3),
    email: z.string().email(),
    age: z.number().int().positive().optional(),
    createdAt: z.string().datetime(),
    updatedAt: z.string().datetime().optional(),
  }),
});

const postSchema = defineSchema({
  tableName: "posts",
  partitionKey: "authorId",
  sortKey: "postId",
  fields: z.object({
    authorId: z.string().uuid(),
    postId: z.string().uuid(),
    title: z.string(),
    content: z.string(),
    tags: z.array(z.string()).default([]),
    createdAt: z.string().datetime(),
  }),
  globalSecondaryIndexes: {
    postsByTags: {
      partitionKey: "tags",
      sortKey: "createdAt",
      projection: { type: "ALL" },
    },
  },
});
```

### 2. Create the Client

Instantiate your DynamoDB client and create the DynaORM client with your defined schemas.

```ts
import { createClient } from "dynaorm";

const dynaClient = createClient(
  {
    users: userSchema,
    posts: postSchema,
  },
  {
    config: {
      region: "us-east-1",
    },
    modelOptions: {
      throttle: { limit: 10, interval: 1000 }, // 10 requests per second
    },
  },
);

// Access your models
const userModel = dynaClient.users;
const postModel = dynaClient.posts;
```

## API Reference

### `createClient(schemas, options)`

Creates the main client instance with access to all your defined models.

**Parameters:**

- `schemas`: A record of schema names to their `defineSchema` results.
- `options`:
  - `config`: AWS SDK `DynamoDBClientConfig`.
  - `modelOptions` (Optional): Default options applied to all models.
    - `throttle`: `{ limit: number; interval: number }` object for throttling requests.

### Model API

#### `model.create(item)`

Creates a new item in the table. Validates the item against the schema.

#### `model.findOne(key, options)`

Retrieves a single item by primary key. Returns `null` if not found.

#### `model.findMany(partitionKeyValue, sortKeyCondition?, options?)`

Retrieves multiple items matching the partition key and optional sort key condition.

#### `model.query()`

Fluent, schema-aware query builder with full support for:

- Partition key and sort key conditions
- Filter conditions with operators: `=`, `<>`, `<`, `>`, `<=`, `>=`, `contains`, `begins_with`, `attribute_exists`, `attribute_not_exists`, `IN`
- Projection of specific attributes
- Pagination and `execAll()` for retrieving all pages
- Ordering (ascending/descending) and start key pagination
- Querying on secondary indexes
- Consistent reads

**Example:**

```ts
const posts = await dynaClient.posts
  .query()
  .wherePK("author-123")
  .whereSK("begins_with", "post-")
  .filter("tags", "contains", "typescript")
  .limit(10)
  .orderBy(true)
  .execAll();
```

### `model.scanAll(options)`

Scans the entire table with optional filtering, projections, and parallel scanning.

### Batch & Transaction Operations

- `transactWrite(items)` – Atomic writes/updates/deletes (up to 100 items per transaction)
- `transactGet(keys)` – Atomic retrieval of up to 100 items
- `batchWrite(items)` – Batch write/delete with retries (up to 25 items per batch)
- `batchGet(keys)` – Batch retrieval with retries (up to 100 items per batch)
- `upsertMany(items)` – Convenience wrapper for batch put
- `updateMany(items)` – Convenience wrapper for batch updates
- `deleteMany(partitionKeyValue, sortKeyCondition?)` – Deletes multiple items; non-atomic for >100 items
- `findByIndex(indexName, keyValues, options?)` – Query items using secondary index
- `deleteByIndex(indexName, keyValues)` – Delete items via secondary index
- `updateByIndex(indexName, keyValues, updates)` – Update items via secondary index

### Throttling

All operations can respect request limits using the `throttle` option in `modelOptions` when creating the client.

---

## Notes

- Type safety is enforced throughout, from schema definition to query building.
- Queries automatically generate placeholders for attribute names and values.
- Supports DynamoDB features like conditional expressions, atomic counters, list append, and set addition.
- Pagination and streaming queries are supported via `paginate()` and `execAll()` in the query builder.
