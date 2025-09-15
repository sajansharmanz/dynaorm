# DynaORM

DynaORM is a lightweight, type-safe Object-Relational Mapper (ORM) for Amazon DynamoDB. It's built with TypeScript and Zod, providing a fluent API for defining schemas and interacting with your DynamoDB tables.

## Features ✨

- **Type-Safe Schemas**: Define your data models using Zod, ensuring type safety from schema definition to database interaction.

- **Fluent Query Builder**: Construct complex queries with a chainable API.

- **Comprehensive CRUD**: Supports create, findOne, findMany, update, delete, scan, and more.

- **Batch & Transactional Operations**: Seamlessly handle batch and transactional writes and gets for multiple items.

- **Index Support**: Define and use Global and Local Secondary Indexes effortlessly.

- **Throttling**: Built-in support for throttling to manage your DynamoDB request limits.

---

## Installation

```sh
npm install dynaorm zod @aws-sdk/client-dynamodb @aws-sdk/util-dynamodb
```

---

## Getting Started

**1. Define Your Schema**

Use `defineSchema` from `dynaorm` and `z` from `zod` to create your data model.

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
      projection: { type: "ALL" },
    },
  },
});
```

**2. Create the Client**

Instantiate your DynamoDB client and create the `dynaorm` client with your defined schemas.

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

---

# API Reference

## `createClient(schemas, options)`

Creates the main client instance with access to all your defined models.

**Parameters:**

- `schemas`: A record of schema names to their `defineSchema` results.
- `options`:
  - `config`: AWS SDK `DynamoDBClientConfig`.
  - `modelOptions` (Optional): Default options applied to all models.
  - `throttle`: `{ limit: number; interval: number }` object for throttling requests.
  - `perModelOptions` (Optional): Per-model overrides for `modelOptions`.

---

## Model API

The `Model` class provides the core methods for interacting with a DynamoDB table.

### `model.create(item)`

Creates a new item in the table. Validates the item against the schema.

```ts
const newUser = await dynaClient.users.create({
  id: "some-uuid",
  username: "johndoe",
  email: "john@example.com",
  createdAt: new Date().toISOString(),
});
```

---

### `model.findOne(key, options)`

Retrieves a single item by primary key. Returns `null` if not found.

**Options:**

- `attributes`: Optional array of attributes to project.
- `consistentRead`: Optional boolean for consistent reads.

```ts
const user = await dynaClient.users.findOne({ id: "some-uuid" });
```

---

### `model.findMany(partitionKeyValue, sortKeyCondition, options)`

Queries multiple items sharing the same partition key.

**Parameters:**

- `partitionKeyValue`: Partition key value.
- `sortKeyCondition` (Optional): `{ operator, value }` for the sort key.
- `options`:
  - `limit`: Maximum number of items to return.
  - `consistentRead`: Boolean for consistent reads.
  - `attributes`: Attributes to project.

```ts
const posts = await dynaClient.posts.findMany("author-123", {
  operator: "begins_with",
  value: "post-",
});
```

---

### `model.findByIndex(indexName, keyValues, options)`

Queries a secondary index (GSI or LSI).

**Parameters:**

- `indexName`: Name of the index.
- `keyValues`: Partition key and optional sort key values for the index.
- `options`: Same as `findMany`, plus optional `operators`.

```ts
const taggedPosts = await dynaClient.posts.findByIndex(
  "postsByTags",
  { tags: "typescript" },
  { attributes: ["postId", "title"] },
);
```

---

### `model.query()` – Fluent, Schema-Aware Query Builder

The `QueryBuilder` provides a type-safe, chainable API for building complex queries on your DynamoDB tables. All keys and filter values are validated against the schema, ensuring correct types at compile time.

#### Basic Usage

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123") // partition key
  .limit(10) // limit number of results
  .exec();
```

---

#### Sort Key Conditions

You can optionally filter by the sort key with any valid operator except `"="` (reserved for partition keys):

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123", {
    key: "postId",
    operator: "begins_with",
    value: "post-",
  })
  .exec();
```

Supported sort key operators: `<`, `<=`, `>`, `>=`, `<>`, `BETWEEN`, `begins_with`.

---

#### Filter Conditions

Add filters with `AND` / `OR` joins. Values are automatically type-checked against your schema.

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123")
  .filter("tags", "contains", "programming") // single value
  .filter("count", ">", 10) // numeric comparison
  .filter("tags", "IN", ["typescript", "webdev"]) // array of valid type
  .exec();
```

Supported operators:
`=`, `<>`, `<`, `>`, `<=`, `>=`, `contains`, `begins_with`, `IN`, `attribute_exists`, `attribute_not_exists`.

---

#### Secondary Index Queries

Type-safe index queries using schema-defined GSI/LSI names:

```ts
const posts = await dynaClient.posts
  .query()
  .where("tags", "typescript")
  .onIndex("postsByTags") // type-safe index name
  .limit(5)
  .exec();
```

---

#### Projection of Attributes

Project only the attributes you need. Type-checked against your schema:

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123")
  .project(["postId", "title"]) // only selected attributes returned
  .exec();
```

---

#### Ordering & Pagination

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123")
  .orderBy(true) // ascending (default: true)
  .startKey({ authorId: "author-123", postId: "post-42" }) // pagination
  .exec();
```

---

#### Consistent Reads & Select Mode

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123")
  .consistentRead(true) // strongly consistent read
  .select("ALL_ATTRIBUTES") // choose what to return
  .exec();
```

`select` modes:

- `ALL_ATTRIBUTES` – full items
- `ALL_PROJECTED_ATTRIBUTES` – index projection only
- `SPECIFIC_ATTRIBUTES` – only attributes selected via `.project()`
- `COUNT` – returns only the count of matching items

---

#### Return Consumed Capacity

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123")
  .returnConsumedCapacity("TOTAL")
  .exec();
```

Options: `"INDEXES" | "TOTAL" | "NONE"`

---

#### Full Example

```ts
const posts = await dynaClient.posts
  .query()
  .where("authorId", "author-123", {
    key: "postId",
    operator: "begins_with",
    value: "post-",
  })
  .filter("tags", "IN", ["typescript", "programming"])
  .onIndex("postsByTags")
  .project(["postId", "title", "tags"])
  .orderBy(false)
  .limit(10)
  .consistentRead(true)
  .returnConsumedCapacity("TOTAL")
  .exec();
```

**Notes:**

- All keys and filter values are **validated against the schema** at compile time.
- Placeholders for DynamoDB attribute names and values are **auto-generated** to prevent conflicts.
- Fully supports **partition key, sort key, filters, secondary indexes, projections, pagination, and DynamoDB operators**.

---

### `model.scanAll(options)`

Scans the entire table with optional filtering and parallel scanning.

```ts
const allPosts = await dynaClient.posts.scanAll({
  parallelism: 4,
  filter: { tags: { operator: "contains", value: "AI" } },
});
```

**Options:**

- `filter`: Filter object for results.
- `limit`: Max number of items.
- `parallelism`: Number of parallel scan segments.
- `segment` / `totalSegments`: For manual parallel scan control.
- `onSegmentData`: Callback per segment batch.

---

### Batch & Transaction Operations

| Method                                                  | Description                                                             |
| ------------------------------------------------------- | ----------------------------------------------------------------------- |
| `model.transactWrite(items)`                            | Atomically writes, updates, or deletes up to 100 items.                 |
| `model.transactGet(keys)`                               | Atomically retrieves up to 100 items.                                   |
| `model.batchWrite(items)`                               | Writes/deletes up to 25 items with automatic retries.                   |
| `model.batchGet(keys)`                                  | Retrieves up to 100 items with retries.                                 |
| `model.upsertMany(items)`                               | Convenience wrapper for batch `put` operations.                         |
| `model.updateMany(items)`                               | Convenience wrapper for batch `update` operations.                      |
| `model.deleteMany(partitionKeyValue, sortKeyCondition)` | Deletes all items matching a query. Warning: non-atomic for >100 items. |
