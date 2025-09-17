import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  ScanCommand,
  TransactGetItemsCommand,
  TransactWriteItemsCommand,
  UpdateItemCommand,
  type BatchGetItemCommandOutput,
  type DeleteItemCommandOutput,
  type GetItemCommandOutput,
  type PutItemCommandOutput,
  type ScanCommandOutput,
  type TransactGetItemsCommandOutput,
  type UpdateItemCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { marshall as awsMarshall, unmarshall } from "@aws-sdk/util-dynamodb";
import pThrottle from "p-throttle";

import type { InferSchema, Schema } from "./schema.js";

import { QueryBuilder, type KeyOperators } from "./query-builder.js";

export type Key<S extends Schema<any, any, any, any, any>> =
  S["sortKey"] extends undefined
    ? Pick<InferSchema<S>, S["partitionKey"]>
    : Pick<InferSchema<S>, S["partitionKey"] | NonNullable<S["sortKey"]>>;

export type PartitionKeyValue<S extends Schema<any, any, any, any, any>> =
  S["partitionKey"] extends keyof InferSchema<S>
    ? InferSchema<S>[S["partitionKey"]]
    : never;

type IndexKey<
  S extends Schema<any, any, any, any, any>,
  I extends
    | keyof S["globalSecondaryIndexes"]
    | keyof S["localSecondaryIndexes"],
> = I extends keyof S["globalSecondaryIndexes"]
  ? S["globalSecondaryIndexes"][I] extends {
      partitionKey: keyof InferSchema<S>;
      sortKey?: keyof InferSchema<S>;
    }
    ? S["globalSecondaryIndexes"][I]["sortKey"] extends keyof InferSchema<S>
      ? Pick<
          InferSchema<S>,
          | S["globalSecondaryIndexes"][I]["partitionKey"]
          | S["globalSecondaryIndexes"][I]["sortKey"]
        >
      : Pick<InferSchema<S>, S["globalSecondaryIndexes"][I]["partitionKey"]>
    : never
  : I extends keyof S["localSecondaryIndexes"]
    ? S["localSecondaryIndexes"][I] extends {
        partitionKey: keyof InferSchema<S>;
        sortKey?: keyof InferSchema<S>;
      }
      ? S["localSecondaryIndexes"][I]["sortKey"] extends keyof InferSchema<S>
        ? Pick<
            InferSchema<S>,
            | S["localSecondaryIndexes"][I]["partitionKey"]
            | S["localSecondaryIndexes"][I]["sortKey"]
          >
        : Pick<InferSchema<S>, S["localSecondaryIndexes"][I]["partitionKey"]>
      : never
    : never;

export interface ModelOptions {
  throttle?: { limit: number; interval: number };
}

type AtomicValue<T> =
  | T
  | { __operation: "increment" | "append" | "addSet"; value: any };

type FilterOperators =
  | KeyOperators
  | "contains"
  | "begins_with"
  | "attribute_exists"
  | "attribute_not_exists";

function chunk<T>(arr: T[], size: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += size)
    result.push(arr.slice(i, i + size));
  return result;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

  private async execute<T>(fn: () => Promise<T>): Promise<T> {
    return this.throttle ? this.throttle(fn)() : fn();
  }

  public sendCommand<T>(command: any): Promise<T> {
    return this.execute(() => this.client.send(command)) as Promise<T>;
  }

  private validateKey(key: object) {
    if (!(this.partitionKey in key))
      throw new Error(`Partition key ${String(this.partitionKey)} is required`);
    if (this.sortKey && !(this.sortKey in key))
      throw new Error(`Sort key ${String(this.sortKey)} is required`);
  }

  public getIndex(indexName: string) {
    if (indexName in this.schema.globalSecondaryIndexes)
      return this.schema.globalSecondaryIndexes[indexName];
    if (indexName in this.schema.localSecondaryIndexes)
      return this.schema.localSecondaryIndexes[indexName];
    throw new Error(`Index ${indexName} does not exist`);
  }

  private getNameHelper() {
    let nameCounter = 0;
    const map = new Map<string, string>();
    return (key: string) => {
      if (!map.has(key)) {
        const placeholder = `#a${nameCounter++}`;
        map.set(key, placeholder);
      }
      return map.get(key)!;
    };
  }

  private isAtomicOperation<T>(
    val: AtomicValue<T>,
  ): val is { __operation: "increment" | "append" | "addSet"; value: any } {
    return (
      typeof val === "object" &&
      val !== null &&
      "__operation" in val &&
      "value" in val
    );
  }

  private marshall(data: object) {
    return awsMarshall(data, { removeUndefinedValues: true });
  }

  private buildProjectionExpression(attributes?: Array<keyof InferSchema<S>>): {
    ProjectionExpression?: string;
    ExpressionAttributeNames?: Record<string, string>;
  } {
    if (!attributes?.length) return {};
    const getName = this.getNameHelper();
    const names: Record<string, string> = {};
    const projection = attributes.map((attr) => {
      const placeholder = getName(String(attr));
      names[placeholder] = String(attr);
      return placeholder;
    });
    return {
      ProjectionExpression: projection.join(", "),
      ExpressionAttributeNames: names,
    };
  }

  private buildKeyCondition<K extends keyof InferSchema<S>>(
    keyValues: Pick<InferSchema<S>, K>,
    operators?: Partial<Record<K, KeyOperators>>,
  ) {
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const conditions: string[] = [];
    const getName = this.getNameHelper();
    let valueCounter = 0;

    for (const k in keyValues) {
      const val = keyValues[k];
      const op = operators?.[k] || "=";
      const namePlaceholder = getName(k);

      if (op === "BETWEEN" && Array.isArray(val) && val.length === 2) {
        const vp1 = `:v${valueCounter++}`;
        const vp2 = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp1] = awsMarshall({ v: val[0] }).v;
        ExpressionAttributeValues[vp2] = awsMarshall({ v: val[1] }).v;
        conditions.push(`${namePlaceholder} BETWEEN ${vp1} AND ${vp2}`);
      } else if (op === "begins_with") {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: val }).v;
        conditions.push(`begins_with(${namePlaceholder}, ${vp})`);
      } else {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: val }).v;
        conditions.push(`${namePlaceholder} ${op} ${vp}`);
      }

      ExpressionAttributeNames[namePlaceholder] = k;
    }

    return {
      KeyConditionExpression: conditions.join(" AND "),
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
  }

  private buildFilterExpression<
    F extends Partial<Record<keyof InferSchema<S>, any>>,
  >(filter?: {
    [K in keyof F]?: { __operation: FilterOperators; value: F[K] } | F[K];
  }) {
    if (!filter) return {};

    const FilterExpression: string[] = [];
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const getName = this.getNameHelper();
    let valueCounter = 0;

    for (const k in filter) {
      const val = filter[k];
      const isOperatorObject =
        val != null &&
        typeof val === "object" &&
        "__operation" in val &&
        "value" in val;

      const operator = isOperatorObject ? (val as any).__operation : "=";
      const value = isOperatorObject ? (val as any).value : val;
      const namePlaceholder = getName(k);

      if (
        operator === "BETWEEN" &&
        Array.isArray(value) &&
        value.length === 2
      ) {
        const vp1 = `:v${valueCounter++}`;
        const vp2 = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp1] = awsMarshall({ v: value[0] }).v;
        ExpressionAttributeValues[vp2] = awsMarshall({ v: value[1] }).v;
        FilterExpression.push(`${namePlaceholder} BETWEEN ${vp1} AND ${vp2}`);
      } else if (operator === "begins_with" || operator === "contains") {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: value }).v;
        FilterExpression.push(`${operator}(${namePlaceholder}, ${vp})`);
      } else if (
        operator === "attribute_exists" ||
        operator === "attribute_not_exists"
      ) {
        FilterExpression.push(`${operator}(${namePlaceholder})`);
      } else {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: value }).v;
        FilterExpression.push(`${namePlaceholder} ${operator} ${vp}`);
      }

      ExpressionAttributeNames[namePlaceholder] = k;
    }

    return {
      FilterExpression: FilterExpression.join(" AND "),
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
  }

  private buildUpdateExpression<U extends Partial<InferSchema<S>>>(updates: {
    [K in keyof U]?: AtomicValue<U[K]>;
  }) {
    const setExpressions: string[] = [];
    const addExpressions: string[] = [];
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const getName = this.getNameHelper();
    let valueCounter = 0;

    for (const k in updates) {
      const val = updates[k];
      if (val === undefined) continue;

      const namePlaceholder = getName(k);
      ExpressionAttributeNames[namePlaceholder] = k;

      if (this.isAtomicOperation(val)) {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: val.value }).v;

        if (val.__operation === "increment") {
          setExpressions.push(
            `${namePlaceholder} = ${namePlaceholder} + ${vp}`,
          );
        } else if (val.__operation === "append") {
          setExpressions.push(
            `${namePlaceholder} = list_append(${namePlaceholder}, ${vp})`,
          );
        } else if (val.__operation === "addSet") {
          addExpressions.push(`${namePlaceholder} ${vp}`);
        }
      } else {
        const vp = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: val }).v;
        setExpressions.push(`${namePlaceholder} = ${vp}`);
      }
    }

    if (!setExpressions.length && !addExpressions.length) {
      return {
        UpdateExpression: null,
        ExpressionAttributeNames: null,
        ExpressionAttributeValues: null,
      };
    }

    const UpdateExpression: string[] = [];
    if (setExpressions.length)
      UpdateExpression.push("SET " + setExpressions.join(", "));
    if (addExpressions.length)
      UpdateExpression.push("ADD " + addExpressions.join(" "));

    return {
      UpdateExpression: UpdateExpression.join(" "),
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    };
  }

  private async parallelScan(
    parallelism: number,
    options: Omit<Parameters<Model<S>["scanAll"]>[0], "parallelism">,
  ) {
    const scans = Array.from({ length: parallelism }, (_, segment) =>
      this.scanAll({
        ...options,
        startKey: undefined,
        segment,
        totalSegments: parallelism,
      }),
    );
    const allResults = await Promise.all(scans);
    return { items: allResults.flatMap((r) => r.items), lastKey: null };
  }

  private async *paginateQuery(
    commandFactory: (exclusiveStartKey?: Record<string, any>) => any,
  ): AsyncGenerator<any, void, unknown> {
    let ExclusiveStartKey: Record<string, any> | undefined;
    do {
      const command = commandFactory(ExclusiveStartKey);
      const response = await this.sendCommand<any>(command);
      const items =
        response.Items?.map((i: any) => unmarshall(i) as InferSchema<S>) || [];
      yield items;
      ExclusiveStartKey = response.LastEvaluatedKey;
    } while (ExclusiveStartKey);
  }

  async create(item: InferSchema<S>) {
    this.schema.fields.parse(item);
    await this.sendCommand<PutItemCommandOutput>(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.marshall(item),
      }),
    );
  }

  async upsert(
    item: InferSchema<S>,
    options?: { condition?: string },
  ): Promise<void> {
    this.schema.fields.parse(item);
    await this.sendCommand<PutItemCommandOutput>(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.marshall(item),
        ConditionExpression: options?.condition,
      }),
    );
  }

  async update(
    key: Key<S>,
    updates: Partial<{
      [K in keyof InferSchema<S>]: AtomicValue<InferSchema<S>[K]>;
    }>,
    options?: { condition?: string },
  ): Promise<void> {
    this.validateKey(key);
    const {
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildUpdateExpression(updates);

    if (!UpdateExpression) {
      throw new Error("Update must include at least one attribute");
    }

    await this.sendCommand<UpdateItemCommandOutput>(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: this.marshall(key),
        UpdateExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
        ConditionExpression: options?.condition,
      }),
    );
  }

  async delete(key: Key<S>, options?: { condition?: string }): Promise<void> {
    this.validateKey(key);
    await this.sendCommand<DeleteItemCommandOutput>(
      new DeleteItemCommand({
        TableName: this.tableName,
        Key: this.marshall(key),
        ConditionExpression: options?.condition,
      }),
    );
  }

  async findOne(
    key: Key<S>,
    options?: {
      attributes?: Array<keyof InferSchema<S>>;
      consistentRead?: boolean;
    },
  ) {
    this.validateKey(key);
    const projection = this.buildProjectionExpression(options?.attributes);

    const result = await this.sendCommand<GetItemCommandOutput>(
      new GetItemCommand({
        TableName: this.tableName,
        Key: this.marshall(key),
        ProjectionExpression: projection.ProjectionExpression,
        ExpressionAttributeNames: projection.ExpressionAttributeNames,
        ConsistentRead: options?.consistentRead,
      }),
    );
    return result.Item ? (unmarshall(result.Item) as InferSchema<S>) : null;
  }

  query(): QueryBuilder<S> {
    return new QueryBuilder(this);
  }

  async findMany<K extends keyof InferSchema<S>>(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: S["sortKey"] extends K
      ? {
          operator: KeyOperators;
          value: InferSchema<S>[K] | [InferSchema<S>[K], InferSchema<S>[K]];
        }
      : never,
    options?: {
      limit?: number;
      consistentRead?: boolean;
      attributes?: Array<keyof InferSchema<S>>;
    },
  ): Promise<InferSchema<S>[]> {
    // Build keyValues
    const keyValues = this.sortKey
      ? ({
          [this.partitionKey]: partitionKeyValue,
          [this.sortKey]: sortKeyCondition?.value,
        } as Pick<InferSchema<S>, typeof this.partitionKey | S["sortKey"]>)
      : ({ [this.partitionKey]: partitionKeyValue } as Pick<
          InferSchema<S>,
          typeof this.partitionKey
        >);

    // Build operators
    const operators: Partial<Record<keyof typeof keyValues, KeyOperators>> = {};
    if (this.sortKey && sortKeyCondition)
      operators[this.sortKey] = sortKeyCondition.operator;

    // Build projection expression
    const projection = this.buildProjectionExpression(options?.attributes);
    const results: InferSchema<S>[] = [];

    // Paginate the query
    for await (const items of this.paginateQuery(
      (ExclusiveStartKey?: Record<string, any>) =>
        new QueryCommand({
          TableName: this.tableName,
          ...this.buildKeyCondition(keyValues, operators),
          ProjectionExpression: projection.ProjectionExpression,
          ExpressionAttributeNames: {
            ...projection.ExpressionAttributeNames,
            ...this.buildKeyCondition(keyValues, operators)
              .ExpressionAttributeNames,
          },
          Limit: options?.limit,
          ExclusiveStartKey,
          ConsistentRead: options?.consistentRead,
        }),
    )) {
      results.push(...items);
      if (options?.limit && results.length >= options.limit) break;
    }

    return options?.limit ? results.slice(0, options.limit) : results;
  }

  async findByIndex<
    I extends
      | keyof S["globalSecondaryIndexes"]
      | keyof S["localSecondaryIndexes"],
  >(
    indexName: I,
    keyValues: IndexKey<S, I>,
    options?: {
      limit?: number;
      consistentRead?: boolean;
      attributes?: Array<keyof InferSchema<S>>;
      operators?: Partial<Record<keyof IndexKey<S, I>, KeyOperators>>;
    },
  ): Promise<InferSchema<S>[]> {
    this.getIndex(String(indexName));

    const projection = this.buildProjectionExpression(options?.attributes);
    const results: InferSchema<S>[] = [];

    const keyCondition = this.buildKeyCondition(keyValues, options?.operators);

    for await (const items of this.paginateQuery(
      (ExclusiveStartKey?: Record<string, any>) =>
        new QueryCommand({
          TableName: this.tableName,
          IndexName: String(indexName),
          KeyConditionExpression: keyCondition.KeyConditionExpression,
          ExpressionAttributeNames: {
            ...projection.ExpressionAttributeNames,
            ...keyCondition.ExpressionAttributeNames,
          },
          ExpressionAttributeValues: keyCondition.ExpressionAttributeValues,
          ProjectionExpression: projection.ProjectionExpression,
          Limit: options?.limit,
          ExclusiveStartKey,
          ConsistentRead: options?.consistentRead,
        }),
    )) {
      results.push(...items);
      if (options?.limit && results.length >= options.limit) break;
    }

    return options?.limit ? results.slice(0, options.limit) : results;
  }

  async deleteByIndex<
    I extends
      | keyof S["globalSecondaryIndexes"]
      | keyof S["localSecondaryIndexes"],
  >(indexName: I, keyValues: IndexKey<S, I>) {
    const items = await this.findByIndex(indexName, keyValues);
    if (!items.length) return;

    const batchOps = items.map((item) => ({
      type: "delete" as const,
      key: {
        [this.partitionKey]: item[this.partitionKey],
        ...(this.sortKey ? { [this.sortKey]: item[this.sortKey] } : {}),
      } as Key<S>,
    }));

    if (batchOps.length <= 100) {
      await this.transactWrite(batchOps);
    } else {
      console.warn("deleteByIndex is not atomic for >100 items");
      await this.batchWrite(
        batchOps.map((op) => ({ type: "delete" as const, item: op.key })),
      );
    }
  }

  async updateByIndex<
    I extends
      | keyof S["globalSecondaryIndexes"]
      | keyof S["localSecondaryIndexes"],
  >(
    indexName: I,
    keyValues: IndexKey<S, I>,
    updates: Partial<{
      [K in keyof InferSchema<S>]: AtomicValue<InferSchema<S>[K]>;
    }>,
  ) {
    const items = await this.findByIndex(indexName, keyValues);
    if (!items.length) return;

    const updateOps = items.map((item) => ({
      key: {
        [this.partitionKey]: item[this.partitionKey],
        ...(this.sortKey ? { [this.sortKey]: item[this.sortKey] } : {}),
      } as Key<S>,
      updates,
    }));

    await this.updateMany(updateOps);
  }

  async scanAll(options?: {
    filter?: {
      [K in keyof InferSchema<S>]?:
        | { __operation: FilterOperators; value: InferSchema<S>[K] }
        | InferSchema<S>[K];
    };
    limit?: number;
    startKey?: Record<string, any>;
    attributes?: Array<keyof InferSchema<S>>;
    parallelism?: number;
    segment?: number;
    totalSegments?: number;
    onSegmentData?: (items: InferSchema<S>[]) => Promise<void> | void;
  }): Promise<{
    items: InferSchema<S>[];
    lastKey: Record<string, any> | null;
  }> {
    if (options?.parallelism && options.parallelism > 1) {
      const { parallelism, ...rest } = options;
      return this.parallelScan(parallelism, rest);
    }

    let ExclusiveStartKey = options?.startKey;
    const results: InferSchema<S>[] = [];
    let remaining = options?.limit ?? Infinity;

    const {
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildFilterExpression(options?.filter);
    const projection = this.buildProjectionExpression(options?.attributes);

    do {
      const result = await this.sendCommand<ScanCommandOutput>(
        new ScanCommand({
          TableName: this.tableName,
          FilterExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
          ExclusiveStartKey,
          Limit: remaining,
          ProjectionExpression: projection.ProjectionExpression,
          Segment: options?.segment,
          TotalSegments: options?.totalSegments,
        }),
      );

      const items =
        result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
      if (options?.onSegmentData) await options.onSegmentData(items);
      results.push(...items);
      ExclusiveStartKey = result.LastEvaluatedKey;
      remaining -= items.length;
    } while (ExclusiveStartKey && remaining > 0);

    return { items: results, lastKey: ExclusiveStartKey ?? null };
  }

  async getItemCount(options?: {
    filter?: Partial<
      Record<
        keyof InferSchema<S>,
        { __operation: FilterOperators; value: any } | any
      >
    >;
    consistentRead?: boolean;
  }): Promise<number> {
    const {
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildFilterExpression(options?.filter);

    const result = await this.sendCommand<ScanCommandOutput>(
      new ScanCommand({
        TableName: this.tableName,
        Select: "COUNT",
        FilterExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
        ConsistentRead: options?.consistentRead,
      }),
    );

    return result.Count ?? 0;
  }

  async transactWrite(
    items: Array<
      | { type: "put"; item: InferSchema<S>; condition?: string }
      | {
          type: "update";
          key: Key<S>;
          updates: Record<string, AtomicValue<any>>;
          condition?: string;
        }
      | { type: "delete"; key: Key<S>; condition?: string }
    >,
  ) {
    if (!items.length) return;
    const chunksOf100 = chunk(items, 100);

    for (const chunkItems of chunksOf100) {
      const TransactItems = chunkItems.map((op) => {
        if (op.type === "put") {
          this.schema.fields.parse(op.item);
          return {
            Put: {
              TableName: this.tableName,
              Item: this.marshall(op.item),
              ConditionExpression: op.condition,
            },
          };
        }
        if (op.type === "update") {
          const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
          } = this.buildUpdateExpression(op.updates);
          if (!UpdateExpression)
            throw new Error("Update must include at least one attribute");
          return {
            Update: {
              TableName: this.tableName,
              Key: this.marshall(op.key),
              UpdateExpression,
              ExpressionAttributeNames,
              ExpressionAttributeValues,
              ConditionExpression: op.condition,
            },
          };
        }
        this.validateKey(op.key);
        return {
          Delete: {
            TableName: this.tableName,
            Key: this.marshall(op.key),
            ConditionExpression: op.condition,
          },
        };
      });
      await this.sendCommand<void>(
        new TransactWriteItemsCommand({ TransactItems }),
      );
    }
  }

  async transactGet(keys: Key<S>[]): Promise<InferSchema<S>[]> {
    if (!keys.length) return [];
    keys.forEach((k) => this.validateKey(k));
    const chunksOf100 = chunk(keys, 100);
    const results: InferSchema<S>[] = [];

    for (const chunkKeys of chunksOf100) {
      const TransactItems = chunkKeys.map((key) => ({
        Get: { TableName: this.tableName, Key: this.marshall(key) },
      }));
      const res = await this.sendCommand<TransactGetItemsCommandOutput>(
        new TransactGetItemsCommand({ TransactItems }),
      );
      results.push(
        ...(res.Responses?.map((r) =>
          r.Item ? (unmarshall(r.Item) as InferSchema<S>) : null,
        ).filter(Boolean) as InferSchema<S>[]),
      );
    }
    return results;
  }

  async batchWrite(
    items: Array<{ type: "put" | "delete"; item: InferSchema<S> | Key<S> }>,
  ) {
    if (!items.length) return;
    const chunksOf25 = chunk(items, 25);
    for (const chunkItems of chunksOf25) {
      let unprocessed = chunkItems.map((op) => {
        if (op.type === "put") {
          this.schema.fields.parse(op.item as InferSchema<S>);
          return { PutRequest: { Item: this.marshall(op.item) } };
        } else {
          this.validateKey(op.item);
          return { DeleteRequest: { Key: this.marshall(op.item) } };
        }
      });

      let attempt = 0;
      do {
        const result = await this.sendCommand<any>(
          new BatchWriteItemCommand({
            RequestItems: { [this.tableName]: unprocessed },
          }),
        );
        unprocessed = result.UnprocessedItems?.[this.tableName] || [];
        if (unprocessed.length) await sleep(2 ** attempt * 50);
        attempt++;
      } while (unprocessed.length);
    }
  }

  async batchGet(keys: Key<S>[]): Promise<InferSchema<S>[]> {
    if (!keys.length) return [];
    keys.forEach((k) => this.validateKey(k));
    const results: InferSchema<S>[] = [];
    let remainingKeys = [...keys];

    while (remainingKeys.length > 0) {
      const chunkKeys = remainingKeys.slice(0, 100);
      remainingKeys = remainingKeys.slice(100);
      let unprocessed = chunkKeys.map((k) => this.marshall(k));
      let attempt = 0;

      do {
        const response = await this.sendCommand<BatchGetItemCommandOutput>(
          new BatchGetItemCommand({
            RequestItems: { [this.tableName]: { Keys: unprocessed } },
          }),
        );
        const fetchedItems =
          response.Responses?.[this.tableName]?.map(
            (i) => unmarshall(i) as InferSchema<S>,
          ) || [];
        results.push(...fetchedItems);

        unprocessed = response.UnprocessedKeys?.[this.tableName]?.Keys || [];
        if (unprocessed.length) await sleep(2 ** attempt * 50);
        attempt++;
      } while (unprocessed.length);
    }
    return results;
  }

  async upsertMany(items: InferSchema<S>[]) {
    if (!items.length) return;
    const batchOps = items.map((item) => ({
      type: "put" as const,
      item: this.schema.fields.parse(item) as InferSchema<S>,
    }));
    await this.batchWrite(batchOps);
  }

  async updateMany(
    items: Array<{
      key: Key<S>;
      updates: Partial<{
        [K in keyof InferSchema<S>]: AtomicValue<InferSchema<S>[K]>;
      }>;
    }>,
  ) {
    if (!items.length) return;
    const chunksOf25 = chunk(items, 25);
    for (const chunkItems of chunksOf25) {
      const TransactItems = chunkItems.map(({ key, updates }) => {
        const {
          UpdateExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
        } = this.buildUpdateExpression(updates);
        if (!UpdateExpression)
          throw new Error("Update must include at least one attribute");
        return {
          Update: {
            TableName: this.tableName,
            Key: this.marshall(key),
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
          },
        };
      });
      await this.sendCommand<void>(
        new TransactWriteItemsCommand({ TransactItems }),
      );
    }
  }

  async deleteMany(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: S["sortKey"] extends keyof InferSchema<S>
      ? {
          operator: KeyOperators;
          value:
            | InferSchema<S>[S["sortKey"]]
            | [InferSchema<S>[S["sortKey"]], InferSchema<S>[S["sortKey"]]];
        }
      : never,
  ) {
    const items = await this.findMany(partitionKeyValue, sortKeyCondition);
    if (!items.length) return;

    if (items.length <= 100) {
      await this.transactWrite(
        items.map((item) => ({
          type: "delete",
          key: {
            [this.partitionKey]: item[this.partitionKey],
            ...(this.sortKey ? { [this.sortKey]: item[this.sortKey] } : {}),
          } as Key<S>,
        })),
      );
    } else {
      console.warn("deleteMany is not atomic for >100 items");
      const batchOps = items.map((item) => ({
        type: "delete" as const,
        item: {
          [this.partitionKey]: item[this.partitionKey],
          ...(this.sortKey ? { [this.sortKey]: item[this.sortKey] } : {}),
        } as Key<S>,
      }));
      await this.batchWrite(batchOps);
    }
  }
}
