# DynaORM - A Type-Safe DynamoDB Client for TypeScript

This package provides a lightweight, type-safe client for Amazon DynamoDB, simplifying common operations with a schema-first approach. By leveraging Zod for schema validation, it ensures your data conforms to a defined structure before interacting with the database. The library abstracts away the complexities of the AWS SDK, such as marshaling and unmarshaling data, and provides a fluent API for building queries.

## Key Features

* **Schema-first**: Define your table structure and indexes using a simple `defineSchema` function.
* **Type Safety**: All operations are fully typed, leveraging TypeScript's generics to provide autocompletion and prevent runtime errors.
* **Zod Validation**: Automatically validates data on create and upsert operations, ensuring data integrity.
* **Fluent Query Builder**: Construct complex queries with a chainable `query()` API for intuitive read operations.
* **Bulk Operations**: Includes methods for efficient `batchWrite`, `batchGet`, `transactWrite`, and `transactGet` operations.
* **Throttling**: Built-in support for throttling requests to manage DynamoDB throughput.

## Installation

```bash
npm install dynaorm zod @aws-sdk/client-dynamodb
```

## How to Use DynaORM

### 1. Define your Schema

First, define the structure of your data using Zod. This library uses Zod to validate the data before it's sent to DynamoDB. You then use the `defineSchema` function to specify your DynamoDB table's primary keys and any secondary indexes.

```typescript
import { z } from "zod";
import { defineSchema } from "dynaorm";

const userSchema = z.object({
  userId: z.string().uuid(),
  email: z.string().email(),
  username: z.string().min(3),
  createdAt: z.string().datetime(),
  status: z.enum(["active", "inactive", "suspended"]),
});

const userTableSchema = defineSchema({
  tableName: "UsersTable",
  fields: userSchema,
  partitionKey: "userId",
  globalSecondaryIndexes: {
    byEmail: { partitionKey: "email", projection: { type: "ALL" } },
    byStatus: { partitionKey: "status", sortKey: "createdAt", projection: { type: "KEYS_ONLY" } },
  },
});
```

### 2. Create the Client

The `createClient` function is the entry point for your application. It takes a record of your schemas and an AWS SDK client configuration. It returns a type-safe client with a model for each schema you defined.

```typescript
import { createClient } from "dynaorm";
import { DynamoDBClientConfig } from "@aws-sdk/client-dynamodb";

const config: DynamoDBClientConfig = {
  region: "us-east-1",
};

const client = createClient(
  { users: userTableSchema },
  {
    config,
    modelOptions: {
      throttle: { limit: 100, interval: 1000 },
    },
  },
);

const userModel = client.users;
```

### 3. Perform Operations

#### Create and Upsert

```typescript
const newUser = {
  userId: "some-uuid-1",
  email: "test@example.com",
  username: "testuser",
  createdAt: new Date().toISOString(),
  status: "active",
};

await userModel.create(newUser);

const updatedUser = { ...newUser, status: "inactive" };
await userModel.upsert(updatedUser);
```

#### Read Operations

```typescript
const user = await userModel.findOne({ userId: "some-uuid-1" });
console.log(user);

const activeUsers = await userModel.findByIndex("byStatus", { status: "active" }, { limit: 10 });
console.log(activeUsers);

const recentActiveUsers = await userModel
  .query()
  .onIndex("byStatus")
  .wherePK("active")
  .whereSK("begins_with", "2023-10")
  .execAll();
console.log(recentActiveUsers);
```

#### Update and Delete

```typescript
await userModel.update({ userId: "some-uuid-1" }, { status: "suspended" });
await userModel.delete({ userId: "some-uuid-1" });
```

#### Bulk Operations

```typescript
await userModel.batchWrite([
  { type: "put", item: { userId: "uuid-2", email: "a@b.com", username: "userA", createdAt: new Date().toISOString(), status: "active" } },
  { type: "delete", item: { userId: "uuid-3" } },
]);

const items = await userModel.batchGet([{ userId: "uuid-2" }]);
console.log(items);

await userModel.transactWrite([
  { type: "put", item: { userId: "uuid-4", email: "c@d.com", username: "userC", createdAt: new Date().toISOString(), status: "active" } },
  { type: "update", key: { userId: "uuid-2" }, updates: { username: "userA-updated" } },
]);
```

## API Reference

### createClient(schemas, options)

Initializes the DynamoDB client and attaches models for each schema.

* **schemas**: A record mapping model names to schemas.
* **options**: Contains `config` (DynamoDBClientConfig), optional `modelOptions`, and optional `perModelOptions`.

### defineSchema(schema)

Defines a new DynamoDB table schema with strong typing.

* **tableName**: DynamoDB table name.
* **fields**: Zod object for item structure.
* **partitionKey**: Partition key field name.
* **sortKey**?: Optional sort key.
* **globalSecondaryIndexes**?: Optional GSIs.
* **localSecondaryIndexes**?: Optional LSIs.

### Model<S>

Provides methods for interacting with a DynamoDB table.

#### CRUD Operations

* `create(item)`
* `upsert(item, options?)`
* `update(key, updates, options?)`
* `delete(key, options?)`
* `findOne(key, options?)`

#### Query and Scan

* `query()`
* `findMany(partitionKeyValue, sortKeyCondition?, options?)`
* `findByIndex(indexName, keyValues, options?)`
* `scanAll(options?)`
* `getItemCount(options?)`

#### Bulk & Transactional Operations

* `batchWrite(items)`
* `batchGet(keys)`
* `transactWrite(items)`
* `transactGet(keys)`
* `upsertMany(items)`
* `updateMany(items)`
* `deleteMany(partitionKeyValue, sortKeyCondition?)`

### QueryBuilder<S>

Fluent API for building DynamoDB Query operations.

* `wherePK(value)`
* `whereSK(operator, value)`
* `filter(key, operator, value, join?)`
* `limit(count)`
* `orderBy(asc)`
* `startKey(key)`
* `project(attrs)`
* `onIndex(index)`
* `consistentRead()`

#### Execution Methods

* `exec()`
* `paginate()`
* `execAll()`
