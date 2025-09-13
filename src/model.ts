import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  ScanCommand,
  UpdateItemCommand,
  type BatchGetItemCommandOutput,
  type GetItemCommandOutput,
  type QueryCommandOutput,
  type ScanCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import pThrottle from "p-throttle";

import type { InferSchema, Schema } from "./schema.js";

import { QueryBuilder, type KeyOperators } from "./query-builder.js";

// --------------------
// Types
// --------------------
export type IndexName<S extends Schema<any, any, any, any, any>> =
  | keyof S["globalSecondaryIndexes"]
  | keyof S["localSecondaryIndexes"];

export type IndexKeySchema<
  S extends Schema<any, any, any, any, any>,
  I extends IndexName<S>,
> = I extends keyof S["globalSecondaryIndexes"]
  ? S["globalSecondaryIndexes"][I] extends {
      partitionKey: infer PK extends keyof InferSchema<S>;
      sortKey?: infer SK extends keyof InferSchema<S>;
    }
    ? SK extends keyof InferSchema<S>
      ? { [P in PK]: InferSchema<S>[P] } & { [Q in SK]?: InferSchema<S>[Q] }
      : { [P in PK]: InferSchema<S>[P] }
    : never
  : I extends keyof S["localSecondaryIndexes"]
    ? S["localSecondaryIndexes"][I] extends {
        sortKey: infer SK extends keyof InferSchema<S>;
      }
      ? { [P in S["partitionKey"]]: InferSchema<S>[P] } & {
          [Q in SK]?: InferSchema<S>[Q];
        }
      : { [P in S["partitionKey"]]: InferSchema<S>[P] }
    : never;

type Key<S extends Schema<any, any, any, any, any>> =
  S["sortKey"] extends undefined
    ? Pick<InferSchema<S>, S["partitionKey"]>
    : Pick<InferSchema<S>, S["partitionKey"] | NonNullable<S["sortKey"]>>;

type PartitionKeyValue<S extends Schema<any, any, any, any, any>> =
  S["partitionKey"] extends keyof InferSchema<S>
    ? InferSchema<S>[S["partitionKey"]]
    : never;

// type SortKeyValue<S extends Schema<any, any, any, any, any>> =
//   S["sortKey"] extends keyof InferSchema<S>
//     ? InferSchema<S>[S["sortKey"]]
//     : never;

export interface ModelOptions {
  throttle?: { limit: number; interval: number };
}

// --------------------
// Model Class
// --------------------
export class Model<S extends Schema<any, any, any, any, any>> {
  public schema: S;
  public tableName: S["tableName"];
  private client: DynamoDBClient;
  private partitionKey: S["partitionKey"];
  private sortKey?: S["sortKey"];
  private throttle?: ReturnType<typeof pThrottle>;

  constructor(schema: S, client: DynamoDBClient, options?: ModelOptions) {
    this.schema = schema;
    this.client = client;
    this.tableName = schema.tableName;
    this.partitionKey = schema.partitionKey;
    this.sortKey = schema.sortKey;
    if (options?.throttle) this.throttle = pThrottle(options.throttle);
  }

  // --------------------
  // Internal Helpers
  // --------------------
  private async execute<T>(fn: () => Promise<T>): Promise<T> {
    return this.throttle ? this.throttle(fn)() : fn();
  }

  public sendCommand<T>(command: any) {
    return this.execute(() => this.client.send(command)) as Promise<T>;
  }

  private marshallItem(item: any) {
    return marshall(item as object);
  }

  private validateKey(key: object) {
    if (!(this.partitionKey in key))
      throw new Error(`Partition key ${String(this.partitionKey)} is required`);
    if (this.sortKey && !(this.sortKey in key))
      throw new Error(`Sort key ${String(this.sortKey)} is required`);
  }

  private buildKeyCondition(keyValues: Record<string, any>) {
    const ExpressionAttributeNames = Object.fromEntries(
      Object.keys(keyValues).map((k, i) => [`#k${i}`, k]),
    );
    const ExpressionAttributeValues = this.marshallItem(keyValues);
    const KeyConditionExpression = Object.keys(keyValues)
      .map((_k, i) => `#k${i} = :v${i}`)
      .join(" AND ");

    return {
      KeyConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
  }

  public getIndex<I extends IndexName<S>>(indexName: I) {
    if (indexName in this.schema.globalSecondaryIndexes)
      return this.schema.globalSecondaryIndexes[
        indexName as keyof S["globalSecondaryIndexes"]
      ];
    if (indexName in this.schema.localSecondaryIndexes)
      return this.schema.localSecondaryIndexes[
        indexName as keyof S["localSecondaryIndexes"]
      ];
    throw new Error(`Index ${String(indexName)} does not exist`);
  }

  // --------------------
  // CRUD Operations
  // --------------------
  async create(item: InferSchema<S>): Promise<void> {
    this.schema.fields.parse(item);
    await this.sendCommand(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.marshallItem(item),
      }),
    );
  }

  async findOne(key: Key<S>): Promise<InferSchema<S> | null> {
    this.validateKey(key);
    const result = await this.sendCommand<GetItemCommandOutput>(
      new GetItemCommand({
        TableName: this.tableName,
        Key: this.marshallItem(key),
      }),
    );
    return result.Item ? (unmarshall(result.Item) as InferSchema<S>) : null;
  }

  async findMany(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: { operator: KeyOperators; value: any | [any, any] },
  ) {
    const q = this.query().where(this.partitionKey, "=", partitionKeyValue);
    if (this.sortKey && sortKeyCondition)
      q.where(this.sortKey, sortKeyCondition.operator, sortKeyCondition.value);
    return q.exec();
  }

  async update(key: Key<S>, updates: Partial<InferSchema<S>>): Promise<void> {
    this.schema.fields.partial().parse(updates);
    this.validateKey(key);

    const UpdateExpression: string[] = [];
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    let i = 0;

    for (const k in updates) {
      const val = updates[k];
      if (val !== undefined) {
        UpdateExpression.push(`#k${i} = :v${i}`);
        ExpressionAttributeNames[`#k${i}`] = k;
        ExpressionAttributeValues[`:v${i}`] = val;
        i++;
      }
    }
    if (!UpdateExpression.length) return;

    await this.sendCommand(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: this.marshallItem(key),
        UpdateExpression: `SET ${UpdateExpression.join(", ")}`,
        ExpressionAttributeNames,
        ExpressionAttributeValues: this.marshallItem(ExpressionAttributeValues),
      }),
    );
  }

  async delete(key: Key<S>): Promise<void> {
    this.validateKey(key);
    await this.sendCommand(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: this.marshallItem(key),
      }),
    );
  }

  async upsert(item: InferSchema<S>): Promise<void> {
    const validated = this.schema.fields.parse(item) as InferSchema<S>;
    await this.sendCommand(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.marshallItem(validated),
      }),
    );
  }

  // --------------------
  // Batch Operations
  // --------------------
  async batchGet(keys: Array<Key<S>>): Promise<InferSchema<S>[]> {
    if (!keys.length) return [];
    keys.forEach((k) => this.validateKey(k));
    const result = await this.sendCommand<BatchGetItemCommandOutput>(
      new BatchGetItemCommand({
        RequestItems: {
          [this.tableName]: { Keys: keys.map((k) => this.marshallItem(k)) },
        },
      }),
    );
    return (result.Responses?.[this.tableName] || []).map(
      (i) => unmarshall(i) as InferSchema<S>,
    );
  }

  async batchWrite(
    items: Array<{ type: "put" | "delete"; item: InferSchema<S> | Key<S> }>,
  ) {
    if (!items.length) return;
    const RequestItems = {
      [this.tableName]: items.map((op) => {
        if (op.type === "put") {
          this.schema.fields.parse(op.item as InferSchema<S>);
          return { PutRequest: { Item: this.marshallItem(op.item) } };
        } else {
          this.validateKey(op.item);
          return { DeleteRequest: { Key: this.marshallItem(op.item) } };
        }
      }),
    };
    await this.sendCommand(new BatchWriteItemCommand({ RequestItems }));
  }

  async upsertMany(items: InferSchema<S>[]) {
    if (!items.length) return;

    const validatedItems = items.map(
      (item) => this.schema.fields.parse(item) as InferSchema<S>,
    );

    const batchOps = validatedItems.map((item) => ({
      type: "put" as const,
      item,
    }));

    await this.batchWrite(batchOps);
  }

  async deleteMany(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: { operator: KeyOperators; value: any | [any, any] },
  ) {
    const items = await this.findMany(partitionKeyValue, sortKeyCondition);
    if (!items.length) return;

    const batchOps = items.map((item) => ({ type: "delete" as const, item }));
    await this.batchWrite(batchOps);
  }

  // --------------------
  // Index Operations
  // --------------------
  async findByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
  ) {
    this.getIndex(indexName);
    const {
      KeyConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildKeyCondition(keyValues);
    const result = await this.sendCommand<QueryCommandOutput>(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: String(indexName),
        KeyConditionExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
      }),
    );
    return result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
  }

  async countByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
  ) {
    this.getIndex(indexName);
    const {
      KeyConditionExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildKeyCondition(keyValues);
    const result = await this.sendCommand<QueryCommandOutput>(
      new QueryCommand({
        TableName: this.tableName,
        IndexName: String(indexName),
        KeyConditionExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
        Select: "COUNT",
      }),
    );
    return result.Count ?? 0;
  }

  async existsByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
  ): Promise<boolean> {
    return (await this.countByIndex(indexName, keyValues)) > 0;
  }

  async deleteByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
  ) {
    const items = await this.findByIndex(indexName, keyValues);
    if (!items.length) return;
    await this.batchWrite(
      items.map((i) => ({ type: "delete" as const, item: i })),
    );
  }

  async updateByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
    updates: Partial<InferSchema<S>>,
  ) {
    const items = await this.findByIndex(indexName, keyValues);
    if (!items.length) return;

    const batchOps = items.map((item) => {
      const updated = this.schema.fields.parse({
        ...item,
        ...updates,
      }) as InferSchema<S>;
      return { type: "put" as const, item: updated };
    });

    await this.batchWrite(batchOps);
  }

  async upsertByIndex<I extends IndexName<S>>(
    indexName: I,
    keyValues: IndexKeySchema<S, I>,
    updates: Partial<InferSchema<S>>,
  ) {
    await this.updateByIndex(indexName, keyValues, updates);
  }

  // --------------------
  // Queries & Scan
  // --------------------
  query(): QueryBuilder<S> {
    return new QueryBuilder(this);
  }

  async scan(
    filter?: Partial<InferSchema<S>>,
    limit?: number,
    startKey?: Record<string, any>,
  ): Promise<{ items: InferSchema<S>[]; lastKey?: Record<string, any> }> {
    let ExclusiveStartKey = startKey;
    const results: InferSchema<S>[] = [];
    let remaining = limit ?? Infinity;

    do {
      const result: ScanCommandOutput = await this.sendCommand(
        new ScanCommand({
          TableName: this.tableName,
          FilterExpression: filter
            ? Object.keys(filter)
                .map((_k, i) => `#k${i} = :v${i}`)
                .join(" AND ")
            : undefined,
          ExpressionAttributeNames: filter
            ? Object.fromEntries(
                Object.keys(filter).map((k, i) => [`#k${i}`, k]),
              )
            : undefined,
          ExpressionAttributeValues: filter ? marshall(filter) : undefined,
          ExclusiveStartKey,
          Limit: remaining,
        }),
      );
      const items =
        result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
      results.push(...items);
      ExclusiveStartKey = result.LastEvaluatedKey;
      remaining -= items.length;
    } while (ExclusiveStartKey && remaining > 0);

    return { items: results, lastKey: ExclusiveStartKey };
  }

  // --------------------
  // Existence & Count
  // --------------------
  async exists(key: Key<S>): Promise<boolean> {
    return (await this.findOne(key)) !== null;
  }

  async countAll(): Promise<number> {
    let count = 0;
    let ExclusiveStartKey: Record<string, any> | undefined = undefined;
    do {
      const result: ScanCommandOutput = await this.sendCommand(
        new ScanCommand({
          TableName: this.tableName,
          Select: "COUNT",
          ExclusiveStartKey,
        }),
      );
      count += result.Count ?? 0;
      ExclusiveStartKey = result.LastEvaluatedKey;
    } while (ExclusiveStartKey);
    return count;
  }

  async countByPartitionKey(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: { operator: KeyOperators; value: any | [any, any] },
  ): Promise<number> {
    const q = this.query().where(this.partitionKey, "=", partitionKeyValue);

    if (this.sortKey && sortKeyCondition) {
      q.where(this.sortKey, sortKeyCondition.operator, sortKeyCondition.value);
    }

    const results = await q.exec();
    return results.length;
  }
}
