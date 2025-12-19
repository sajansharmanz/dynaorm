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

type ExtractKeys<T> = T extends any[] ? T[number] : T;

type IndexKey<
  S extends Schema<any, any, any, any, any>,
  I extends
    | keyof S["globalSecondaryIndexes"]
    | keyof S["localSecondaryIndexes"],
> = I extends keyof S["globalSecondaryIndexes"]
  ? Pick<
      InferSchema<S>,
      | ExtractKeys<S["globalSecondaryIndexes"][I]["partitionKey"]>
      | ExtractKeys<S["globalSecondaryIndexes"][I]["sortKey"]>
    >
  : I extends keyof S["localSecondaryIndexes"]
    ? Pick<
        InferSchema<S>,
        | S["partitionKey"] // LSIs always share the table partition key
        | ExtractKeys<S["localSecondaryIndexes"][I]["sortKey"]>
      >
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

  private buildProjectionExpression(attributes?: Array<keyof InferSchema<S>>) {
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
      ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
    };
  }

  private buildKeyCondition(
    keyValues: Record<string, any>,
    operators?: Partial<Record<string, KeyOperators>>,
  ) {
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const conditions: string[] = [];
    const getName = this.getNameHelper();
    let valueCounter = 0;

    for (const k in keyValues) {
      const val = keyValues[k];
      if (val === undefined) continue;

      const op = operators?.[k] || "=";
      const namePlaceholder = getName(k);
      const vp = `:v${valueCounter++}`;

      ExpressionAttributeNames[namePlaceholder] = k;

      if (op === "BETWEEN" && Array.isArray(val) && val.length === 2) {
        const vp2 = `:v${valueCounter++}`;
        ExpressionAttributeValues[vp] = awsMarshall({ v: val[0] }).v;
        ExpressionAttributeValues[vp2] = awsMarshall({ v: val[1] }).v;
        conditions.push(`${namePlaceholder} BETWEEN ${vp} AND ${vp2}`);
      } else if (op === "begins_with") {
        ExpressionAttributeValues[vp] = awsMarshall({ v: val }).v;
        conditions.push(`begins_with(${namePlaceholder}, ${vp})`);
      } else {
        ExpressionAttributeValues[vp] = awsMarshall({ v: val }).v;
        conditions.push(`${namePlaceholder} ${op} ${vp}`);
      }
    }

    return {
      KeyConditionExpression: conditions.join(" AND "),
      ExpressionAttributeNames: Object.keys(ExpressionAttributeNames).length
        ? ExpressionAttributeNames
        : undefined,
      ExpressionAttributeValues: Object.keys(ExpressionAttributeValues).length
        ? ExpressionAttributeValues
        : undefined,
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
      FilterExpression: FilterExpression.length
        ? FilterExpression.join(" AND ")
        : undefined,
      ExpressionAttributeNames: Object.keys(ExpressionAttributeNames).length
        ? ExpressionAttributeNames
        : undefined,
      ExpressionAttributeValues: Object.keys(ExpressionAttributeValues).length
        ? ExpressionAttributeValues
        : undefined,
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
        ExpressionAttributeValues[vp] = awsMarshall(
          { v: val.value },
          { removeUndefinedValues: true },
        ).v;

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
        ExpressionAttributeValues[vp] = awsMarshall(
          { v: val },
          { removeUndefinedValues: true },
        ).v;
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
    const parsed = this.schema.fields.parse(item);
    await this.sendCommand<PutItemCommandOutput>(
      new PutItemCommand({
        TableName: this.tableName,
        Item: this.marshall(parsed),
        ConditionExpression: `attribute_not_exists(#pk)`,
        ExpressionAttributeNames: { "#pk": String(this.partitionKey) },
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

  async findMany<K extends keyof InferSchema<S>>(
    partitionKeyValue: PartitionKeyValue<S>,
    sortKeyCondition?: K extends keyof InferSchema<S>
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
    const keyValues: Record<string, any> = {
      [this.partitionKey as string]: partitionKeyValue,
    };
    const operators: Partial<Record<string, KeyOperators>> = {};

    if (this.sortKey && sortKeyCondition) {
      keyValues[this.sortKey as string] = sortKeyCondition.value;
      operators[this.sortKey as string] = sortKeyCondition.operator;
    }

    const projection = this.buildProjectionExpression(options?.attributes);
    const results: InferSchema<S>[] = [];

    for await (const items of this.paginateQuery(
      (ExclusiveStartKey?: Record<string, any>) => {
        const keyCond = this.buildKeyCondition(keyValues, operators);
        const mergedNames = {
          ...(projection.ExpressionAttributeNames ?? {}),
          ...(keyCond.ExpressionAttributeNames ?? {}),
        };

        return new QueryCommand({
          TableName: this.tableName,
          KeyConditionExpression: keyCond.KeyConditionExpression,
          ExpressionAttributeNames: Object.keys(mergedNames).length
            ? mergedNames
            : undefined,
          ExpressionAttributeValues: keyCond.ExpressionAttributeValues,
          ProjectionExpression: projection.ProjectionExpression,
          ...(options?.limit !== undefined && Number.isFinite(options.limit)
            ? { Limit: options.limit }
            : {}),
          ExclusiveStartKey,
          ConsistentRead: options?.consistentRead,
        });
      },
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
    const index = this.getIndex(String(indexName));

    const pks = Array.isArray(index.partitionKey)
      ? index.partitionKey
      : [index.partitionKey];
    for (const pk of pks) {
      if (!(pk in (keyValues as any))) {
        throw new Error(
          `Attribute ${String(pk)} is a required partition key for index ${String(indexName)}`,
        );
      }
    }

    const projection = this.buildProjectionExpression(options?.attributes);
    const results: InferSchema<S>[] = [];
    const keyCond = this.buildKeyCondition(
      keyValues as Record<string, any>,
      options?.operators as any,
    );

    for await (const items of this.paginateQuery(
      (ExclusiveStartKey?: Record<string, any>) => {
        const mergedNames = {
          ...(projection.ExpressionAttributeNames ?? {}),
          ...(keyCond.ExpressionAttributeNames ?? {}),
        };

        return new QueryCommand({
          TableName: this.tableName,
          IndexName: String(indexName),
          KeyConditionExpression: keyCond.KeyConditionExpression,
          ExpressionAttributeNames: Object.keys(mergedNames).length
            ? mergedNames
            : undefined,
          ExpressionAttributeValues: keyCond.ExpressionAttributeValues,
          ProjectionExpression: projection.ProjectionExpression,
          ...(options?.limit !== undefined && Number.isFinite(options.limit)
            ? { Limit: options.limit }
            : {}),
          ExclusiveStartKey,
          ConsistentRead: options?.consistentRead,
        });
      },
    )) {
      results.push(...items);
      if (options?.limit && results.length >= options.limit) break;
    }
    return options?.limit ? results.slice(0, options.limit) : results;
  }

  async update(
    key: Key<S>,
    updates: Partial<{
      [K in keyof InferSchema<S>]: AtomicValue<InferSchema<S>[K]>;
    }>,
    options?: { condition?: string },
  ) {
    this.validateKey(key);
    const validatedUpdates: Partial<InferSchema<S>> = {};
    for (const k in updates) {
      const val = updates[k];
      validatedUpdates[k] = this.isAtomicOperation(val)
        ? val
        : this.schema.fields.shape[k].parse(val);
    }

    const {
      UpdateExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
    } = this.buildUpdateExpression(validatedUpdates);
    if (!UpdateExpression)
      throw new Error("Update must include at least one attribute");

    await this.sendCommand<UpdateItemCommandOutput>(
      new UpdateItemCommand({
        TableName: this.tableName,
        Key: this.marshall(key),
        UpdateExpression,
        ExpressionAttributeNames: Object.keys(ExpressionAttributeNames || {})
          .length
          ? ExpressionAttributeNames
          : undefined,
        ExpressionAttributeValues: Object.keys(ExpressionAttributeValues || {})
          .length
          ? ExpressionAttributeValues
          : undefined,
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

    const filterExpr = this.buildFilterExpression(options?.filter);
    const projection = this.buildProjectionExpression(options?.attributes);

    do {
      const cmdParams: any = {
        TableName: this.tableName,
        ExclusiveStartKey,
        Segment: options?.segment,
        TotalSegments: options?.totalSegments,
        ProjectionExpression: projection.ProjectionExpression,
        ...(Number.isFinite(remaining) ? { Limit: remaining } : {}),
      };

      if (filterExpr.FilterExpression)
        cmdParams.FilterExpression = filterExpr.FilterExpression;

      if (
        filterExpr.ExpressionAttributeNames ||
        projection.ExpressionAttributeNames
      ) {
        cmdParams.ExpressionAttributeNames = {
          ...(filterExpr.ExpressionAttributeNames ?? {}),
          ...(projection.ExpressionAttributeNames ?? {}),
        };
      }

      if (filterExpr.ExpressionAttributeValues)
        cmdParams.ExpressionAttributeValues =
          filterExpr.ExpressionAttributeValues;

      const result = await this.sendCommand<ScanCommandOutput>(
        new ScanCommand(cmdParams),
      );
      const items =
        result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];

      if (options?.onSegmentData) await options.onSegmentData(items);
      results.push(...items);
      ExclusiveStartKey = result.LastEvaluatedKey;

      if (Number.isFinite(remaining)) {
        remaining -= items.length;
      }
    } while (ExclusiveStartKey && (remaining === Infinity || remaining > 0));

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

  async transactGet(keys: Key<S>[]): Promise<InferSchema<S>[]> {
    if (!keys.length) return [];
    keys.forEach((k) => this.validateKey(k));
    const chunks = chunk(keys, 100);
    const results: InferSchema<S>[] = [];

    for (const chunkKeys of chunks) {
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
    const chunks = chunk(items, 25);

    for (const chunkItems of chunks) {
      const TransactItems = chunkItems.map(({ key, updates }) => {
        this.validateKey(key);
        const validatedUpdates: Partial<InferSchema<S>> = {};
        for (const k in updates) {
          const val = updates[k];
          validatedUpdates[k] = this.isAtomicOperation(val)
            ? val
            : this.schema.fields.shape[k].parse(val);
        }

        const {
          UpdateExpression,
          ExpressionAttributeNames,
          ExpressionAttributeValues,
        } = this.buildUpdateExpression(validatedUpdates);

        if (!UpdateExpression)
          throw new Error("Update must include at least one attribute");

        return {
          Update: {
            TableName: this.tableName,
            Key: this.marshall(key),
            UpdateExpression,
            ExpressionAttributeNames: Object.keys(
              ExpressionAttributeNames || {},
            ).length
              ? ExpressionAttributeNames
              : undefined,
            ExpressionAttributeValues: Object.keys(
              ExpressionAttributeValues || {},
            ).length
              ? ExpressionAttributeValues
              : undefined,
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

    const ops = items.map((item) => ({
      type: "delete" as const,
      key: {
        [this.partitionKey as string]:
          item[this.partitionKey as keyof InferSchema<S>],
        ...(this.sortKey
          ? {
              [this.sortKey as string]:
                item[this.sortKey as keyof InferSchema<S>],
            }
          : {}),
      } as Key<S>,
    }));

    if (ops.length <= 100) {
      await this.transactWrite(ops);
    } else {
      await this.batchWrite(
        ops.map((op) => ({ type: "delete", item: op.key })),
      );
    }
  }

  async deleteByIndex<
    I extends
      | keyof S["globalSecondaryIndexes"]
      | keyof S["localSecondaryIndexes"],
  >(indexName: I, keyValues: IndexKey<S, I>) {
    const items = await this.findByIndex(indexName, keyValues);
    if (!items.length) return;

    const ops = items.map((item) => ({
      key: {
        [this.partitionKey as string]:
          item[this.partitionKey as keyof InferSchema<S>],
        ...(this.sortKey
          ? {
              [this.sortKey as string]:
                item[this.sortKey as keyof InferSchema<S>],
            }
          : {}),
      } as Key<S>,
    }));

    if (ops.length <= 100) {
      await this.transactWrite(
        ops.map((op) => ({ type: "delete", key: op.key })),
      );
    } else {
      await this.batchWrite(
        ops.map((op) => ({ type: "delete", item: op.key })),
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
        [this.partitionKey as string]:
          item[this.partitionKey as keyof InferSchema<S>],
        ...(this.sortKey
          ? {
              [this.sortKey as string]:
                item[this.sortKey as keyof InferSchema<S>],
            }
          : {}),
      } as Key<S>,
      updates,
    }));

    await this.updateMany(updateOps);
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

  query(): QueryBuilder<S> {
    return new QueryBuilder(this);
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
    const chunks = chunk(items, 100);
    for (const chunkItems of chunks) {
      const TransactItems = chunkItems.map((op) => {
        if (op.type === "put") {
          const parsed = this.schema.fields.parse(op.item);
          return {
            Put: {
              TableName: this.tableName,
              Item: this.marshall(parsed),
              ConditionExpression: op.condition || undefined,
            },
          };
        }
        if (op.type === "update") {
          const {
            UpdateExpression,
            ExpressionAttributeNames,
            ExpressionAttributeValues,
          } = this.buildUpdateExpression(op.updates);

          // Prepare Names: Ensure it's Record<string, string> or undefined
          const names =
            ExpressionAttributeNames &&
            Object.keys(ExpressionAttributeNames).length > 0
              ? (ExpressionAttributeNames as Record<string, string>)
              : undefined;

          // Prepare Values: Ensure it's Record<string, AttributeValue> or undefined
          const values =
            ExpressionAttributeValues &&
            Object.keys(ExpressionAttributeValues).length > 0
              ? (ExpressionAttributeValues as any)
              : undefined;

          return {
            Update: {
              TableName: this.tableName,
              Key: this.marshall(op.key),
              UpdateExpression: UpdateExpression ?? undefined,
              ExpressionAttributeNames: names,
              ExpressionAttributeValues: values,
              ConditionExpression: op.condition || undefined,
            },
          };
        }
        return {
          Delete: {
            TableName: this.tableName,
            Key: this.marshall(op.key),
            ConditionExpression: op.condition || undefined,
          },
        };
      });

      await this.sendCommand<void>(
        new TransactWriteItemsCommand({ TransactItems }),
      );
    }
  }

  async batchWrite(
    items: Array<{ type: "put" | "delete"; item: InferSchema<S> | Key<S> }>,
  ) {
    if (!items.length) return;
    const chunks = chunk(items, 25);
    for (const chunkItems of chunks) {
      let unprocessed = chunkItems.map((op) =>
        op.type === "put"
          ? {
              PutRequest: {
                Item: this.marshall(
                  this.schema.fields.parse(op.item as InferSchema<S>),
                ),
              },
            }
          : { DeleteRequest: { Key: this.marshall(op.item) } },
      );
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
}
