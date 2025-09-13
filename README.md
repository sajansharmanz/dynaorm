# DynaORM

A simple, type-safe DynamoDB ORM built with TypeScript and Zod. DynaORM simplifies interactions with AWS DynamoDB by providing a clean, object-oriented interface, strong typing,

## 🚀 Features

- **Type-Safe**: Built with TypeScript and Zod to provide end-to-end type safety from schema definition to database queries.

- **Zod Integration**: Leverage the power of Zod for schema validation and infer types directly from your schemas.

- **Simple API**: A clean, easy-to-use API for common CRUD (Create, Read, Update, Delete) and batch operations.

- **Flexible** Queries: Build complex queries using a fluid QueryBuilder for both tables and indexes.

- **Configurable**: Supports per-model or global throttling and custom AWS DynamoDB client configurations.

## 📦 Installation

To get started, add DynaORM and its peer dependencies to your project:

```bash
npm install dynaorm @aws-sdk/client-dynamodb @aws-sdk/util-dynamodb p-throttle zod
```

## 🛠️ Usage

**1. Define Your Schema**

Use `defineSchema` to create a type-safe schema for your DynamoDB table. The schema defines the table name, keys, and fields, along with any global or local secondary indexes. The fields are defined using a Zod object.

```ts
// schemas/user-schema.ts
import { defineSchema, defineSchema } from "dynaorm";
// schemas/product-schema.ts
import { z, z } from "zod";

export const userSchema = defineSchema({
  tableName: "Users",
  partitionKey: "userId",
  sortKey: "createdAt",
  fields: z.object({
    userId: z.string().uuid(),
    createdAt: z.string().datetime(),
    email: z.string().email(),
    username: z.string().min(3).max(20),
    age: z.number().optional(),
  }),
});

export const productSchema = defineSchema({
  tableName: "Products",
  partitionKey: "productId",
  fields: z.object({
    productId: z.string().uuid(),
    name: z.string(),
    price: z.number(),
    category: z.string(),
  }),
  globalSecondaryIndexes: {
    byCategory: {
      partitionKey: "category",
    },
  },
});
```

**Type Inference 🤝**

You can leverage DynaORM for full type safety by using the `InferSchema` utility type. Since `InferSchema` is exported directly from the main package, you can easily create types for your schema models without needing a separate codegen step. This allows you to define type-safe data and functions that work seamlessly with your DynamoDB models.

```ts
import { InferSchema } from "dynaorm";

import { productSchema, userSchema } from "./schemas"; // Assuming a central schemas index file

// Create a type for the User model
export type User = InferSchema<typeof userSchema>;

// Create a type for the Product model
export type Product = InferSchema<typeof productSchema>;

// Example usage with a typed object
const newUser: User = {
  userId: "0f4a8e2d-3c2b-4d1a-8e2b-4a5c9f8d7e6f",
  createdAt: new Date().toISOString(),
  email: "test@example.com",
  username: "testuser",
  age: 25,
};
```

**2. Create the Client**

The `createClient` function initializes a DynamoDB client and returns a type-safe object with a `Model` instance for each schema provided. You can configure options per model using `perModelOptions` or globally using `modelOptions`.

```ts
// client.ts
import { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";
import { createClient } from "dynaorm";

import { productSchema } from "./schemas/product-schema";
import { userSchema } from "./schemas/user-schema";

const schemas = {
  users: userSchema,
  products: productSchema,
};

const clientConfig: DynamoDBClientConfig = {
  region: "ap-southeast-2",
};

export const client = createClient(schemas, {
  config: clientConfig,
  // Optional: Global throttling for all models
  modelOptions: {
    throttle: {
      limit: 10,
      interval: 1000,
    },
  },
  // Optional: Per-model throttling to override global settings
  perModelOptions: {
    users: {
      throttle: {
        limit: 5, // A different throttle limit for the 'users' model
        interval: 1000,
      },
    },
  },
});
```

**3. Perform CRUD Operations**

The `client` object provides access to each model, which exposes a set of methods for interacting with the database.

```ts
// app.ts
import { client } from "./client";

async function run() {
  // Create a new item
  const newUser = {
    userId: "0f4a8e2d-3c2b-4d1a-8e2b-4a5c9f8d7e6f",
    createdAt: new Date().toISOString(),
    email: "test@example.com",
    username: "testuser",
  };
  await client.users.create(newUser);
  console.log("User created successfully!");

  // Find a single item by key
  const user = await client.users.findOne({
    userId: newUser.userId,
    createdAt: newUser.createdAt,
  });
  console.log("Found user:", user);

  // Update an item
  await client.users.update(
    { userId: newUser.userId, createdAt: newUser.createdAt },
    { age: 30 },
  );
  const updatedUser = await client.users.findOne({
    userId: newUser.userId,
    createdAt: newUser.createdAt,
  });
  console.log("Updated user:", updatedUser);

  // Delete an item
  await client.users.delete({
    userId: newUser.userId,
    createdAt: newUser.createdAt,
  });
  console.log("User deleted.");
}

run();
```

## 📚 Advanced Operations

**Querying**

Use the `query()` method to build complex, type-safe queries. This is useful for fetching items based on partition keys and optional sort key conditions.

```ts
// app.ts
import { client } from "./client";

async function queryData() {
  const userId = "0f4a8e2d-3c2b-4d1a-8e2b-4a5c9f8d7e6f";

  // Find all items for a partition key
  const userItems = await client.users.findMany(userId);

  // Use the QueryBuilder for more control
  const recentUsers = await client.users
    .query()
    .where("userId", "=", userId)
    .where("createdAt", ">", "2024-01-01T00:00:00Z")
    .exec();

  console.log("Recent users:", recentUsers);
}
```

**Scanning**

The `scan()` method retrieves all items from a table. This is highly discouraged for large tables as it can be very expensive. Use it only for small tables or administrative tasks.

```ts
// app.ts
import { client } from "./client";

async function scanData() {
  // Scan with a filter
  const { items: products } = await client.products.scan({
    category: "electronics",
  });
  console.log("Electronics products:", products);

  // Scan all items with a limit
  const { items: limitedItems } = await client.products.scan(undefined, 10);
  console.log("First 10 products:", limitedItems);
}
```

**Batch Operations**

DynaORM provides convenient methods for performing batch reads and writes, which are more efficient for multiple items.

```ts
// app.ts
import { client } from "./client";

async function batchOperations() {
  const productKeys = [{ productId: "id1" }, { productId: "id2" }];

  // Batch get items
  const products = await client.products.batchGet(productKeys);
  console.log("Batch retrieved products:", products);

  // Batch write/upsert
  const newProducts = [
    { productId: "id3", name: "Laptop", price: 1200, category: "electronics" },
    { productId: "id4", name: "Mouse", price: 25, category: "electronics" },
  ];
  await client.products.upsertMany(newProducts);
  console.log("New products added via batch upsert.");

  // Batch delete items
  await client.products.batchWrite([
    { type: "delete", item: { productId: "id1" } },
    { type: "delete", item: { productId: "id2" } },
  ]);
  console.log("Products deleted via batch write.");
}
```

**Index Operations**

Interact with Global and Local Secondary Indexes using the `findByIndex`, `countByIndex`, and `deleteByIndex` methods.

```ts
// app.ts
import { client } from "./client";

async function indexOperations() {
  // Find products by GSI
  const electronics = await client.products.findByIndex("byCategory", {
    category: "electronics",
  });
  console.log("Electronics found by index:", electronics);

  // Count items in an index
  const count = await client.products.countByIndex("byCategory", {
    category: "electronics",
  });
  console.log("Number of electronics:", count);

  // Check existence
  const exists = await client.products.existsByIndex("byCategory", {
    category: "electronics",
  });
  console.log("Electronics category exists:", exists);
}
```

## 📝 API Reference

**`Model` Class**

| Method                                | Description                                                           |
| ------------------------------------- | --------------------------------------------------------------------- |
| `create(item)`                        | Validates and creates a new item.                                     |
| `findOne(key)`                        | Retrieves a single item by its primary key.                           |
| `findMany(pkValue, [skCondition])`    | Queries for all items with a given partition key.                     |
| `update(key, updates)`                | Updates specific attributes of an item.                               |
| `delete(key)`                         | Deletes a single item by its primary key.                             |
| `upsert(item)`                        | Creates or replaces an item.                                          |
| `batchGet(keys)`                      | Retrieves multiple items in a single request.                         |
| `batchWrite(items)`                   | Performs a mix of put and delete operations in a single request.      |
| `upsertMany(items)`                   | Batch-upserts multiple items.                                         |
| `deleteMany(pkValue, [skCondition])`  | Deletes multiple items with a given partition key.                    |
| `findByIndex(indexName, keyValues)`   | Queries a secondary index.                                            |
| `countByIndex(indexName, keyValues)`  | Counts items in a secondary index.                                    |
| `exists(key)`                         | Checks if an item exists by its primary key.                          |
| `countAll()`                          | Scans the table to count all items. Use with caution.                 |
| `scan([filter], [limit], [startKey])` | Scans the table for items, with optional filtering. Use with caution. |
| `query()`                             | Returns a QueryBuilder instance for building complex queries.         |

**`QueryBuilder` Class**

| Method                                 | Description                                                         |
| -------------------------------------- | ------------------------------------------------------------------- |
| `index(name)`                          | Specifies the index to query.                                       |
| `where(key, operator, value)`          | Adds a key condition. Can be chained.                               |
| `filter(key, operator, value, [join])` | Adds a filter expression. Can be chained.                           |
| `limit(count)`                         | Sets the maximum number of items to return.                         |
| `orderBy(asc)`                         | Sets the sort order (`true` for ascending, `false` for descending). |
| `startKey(key)`                        | Starts the query from a specific key for pagination.                |
| `exec()`                               | Executes the query and returns the results.                         |
