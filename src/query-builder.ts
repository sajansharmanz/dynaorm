import {
  QueryCommand,
  type QueryCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

import type { InferSchema, Schema } from "./schema.js";

import { Model } from "./model.js";

export type KeyOperators =
  | "="
  | "<"
  | "<="
  | ">"
  | ">="
  | "<>"
  | "BETWEEN"
  | "begins_with";

export type FilterOperators =
  | "="
  | "<>"
  | "<"
  | ">"
  | "<="
  | ">="
  | "contains"
  | "begins_with"
  | "attribute_exists"
  | "attribute_not_exists"
  | "IN";

interface KeyCondition {
  conditions: Array<{
    key: string;
    operator: KeyOperators;
    value: any;
  }>;
}

interface FilterCondition<
  S extends Schema<any, any, any, any, any>,
  K extends keyof InferSchema<S>,
> {
  key: K;
  operator: FilterOperators;
  value?: InferSchema<S>[K] | InferSchema<S>[K][];
  join?: "AND" | "OR";
}

export class QueryBuilder<S extends Schema<any, any, any, any, any>> {
  private model: Model<S>;
  private partitionKeyConditions: KeyCondition = { conditions: [] };
  private sortKeyConditions: KeyCondition = { conditions: [] };
  private filterConditions: FilterCondition<S, keyof InferSchema<S>>[] = [];
  private limitCount?: number;
  private scanIndexForward?: boolean;
  private ExclusiveStartKey?: Record<string, any>;
  private ProjectionAttributes?: Array<keyof InferSchema<S>>;
  private indexName?:
    | keyof S["globalSecondaryIndexes"]
    | keyof S["localSecondaryIndexes"];
  private consistent?: boolean;

  constructor(model: Model<S>) {
    this.model = model;
  }

  private clone(): QueryBuilder<S> {
    const qb = new QueryBuilder<S>(this.model);
    Object.assign(qb, {
      partitionKeyConditions: {
        conditions: [...this.partitionKeyConditions.conditions],
      },
      sortKeyConditions: { conditions: [...this.sortKeyConditions.conditions] },
      filterConditions: [...this.filterConditions],
      limitCount: this.limitCount,
      scanIndexForward: this.scanIndexForward,
      ExclusiveStartKey: this.ExclusiveStartKey,
      ProjectionAttributes: this.ProjectionAttributes,
      indexName: this.indexName,
      consistent: this.consistent,
    });
    return qb;
  }

  wherePK(values: Partial<InferSchema<S>>) {
    for (const [key, value] of Object.entries(values)) {
      this.partitionKeyConditions.conditions.push({
        key,
        operator: "=",
        value,
      });
    }
    return this;
  }

  whereSK<K extends keyof InferSchema<S>>(
    key: K,
    operator: KeyOperators,
    value: InferSchema<S>[K] | [InferSchema<S>[K], InferSchema<S>[K]],
  ) {
    if (
      operator === "BETWEEN" &&
      (!Array.isArray(value) || value.length !== 2)
    ) {
      throw new Error("BETWEEN operator requires an array of two values.");
    }

    this.sortKeyConditions.conditions.push({
      key: key as string,
      operator,
      value,
    });
    return this;
  }

  filter<K extends keyof InferSchema<S>>(
    key: K,
    operator: Exclude<
      FilterOperators,
      "IN" | "attribute_exists" | "attribute_not_exists"
    >,
    value: InferSchema<S>[K],
    join?: "AND" | "OR",
  ): this;
  filter<K extends keyof InferSchema<S>>(
    key: K,
    operator: "IN",
    value: InferSchema<S>[K][],
    join?: "AND" | "OR",
  ): this;
  filter<K extends keyof InferSchema<S>>(
    key: K,
    operator: "attribute_exists" | "attribute_not_exists",
    value?: never,
    join?: "AND" | "OR",
  ): this;
  filter<K extends keyof InferSchema<S>>(
    key: K,
    operator: FilterOperators,
    value?: InferSchema<S>[K] | InferSchema<S>[K][],
    join: "AND" | "OR" = "AND",
  ): this {
    if (
      operator !== "attribute_exists" &&
      operator !== "attribute_not_exists" &&
      value === undefined
    ) {
      throw new Error(
        `Filter value for key "${String(key)}" cannot be undefined.`,
      );
    }

    this.filterConditions.push({ key, operator, value, join });
    return this;
  }

  limit(count: number) {
    this.limitCount = count;
    return this;
  }
  orderBy(asc: boolean) {
    this.scanIndexForward = asc;
    return this;
  }
  startKey(key?: Record<string, any>) {
    this.ExclusiveStartKey = key;
    return this;
  }
  project(attrs: Array<keyof InferSchema<S>>) {
    this.ProjectionAttributes = attrs;
    return this;
  }
  onIndex(
    index: keyof S["globalSecondaryIndexes"] | keyof S["localSecondaryIndexes"],
  ) {
    this.indexName = index;
    return this;
  }
  consistentRead(isConsistent = true) {
    this.consistent = isConsistent;
    return this;
  }

  private validateIndexKeys() {
    if (!this.indexName) return;
    const index = this.model.getIndex(this.indexName as string);

    const requiredPKs = Array.isArray(index.partitionKey)
      ? index.partitionKey
      : [index.partitionKey];
    const providedPKs = this.partitionKeyConditions.conditions.map(
      (c) => c.key,
    );

    for (const req of requiredPKs) {
      if (!providedPKs.includes(req as string)) {
        throw new Error(
          `Partition key attribute "${String(req)}" is missing for index query.`,
        );
      }
    }
  }

  private buildKeyExpression(
    ExpressionAttributeNames: Record<string, string>,
    ExpressionAttributeValues: Record<string, any>,
    nameCounter: { count: number },
    valueCounter: { count: number },
  ): string {
    const getName = (key: string) => {
      const existing = Object.entries(ExpressionAttributeNames).find(
        ([, v]) => v === key,
      );
      if (existing) return existing[0];
      const name = `#a${nameCounter.count++}`;
      ExpressionAttributeNames[name] = key;
      return name;
    };

    const getValue = (value: any) => {
      const name = `:v${valueCounter.count++}`;
      ExpressionAttributeValues[name] = value;
      return name;
    };

    const keyExpressions: string[] = [];

    for (const cond of this.partitionKeyConditions.conditions) {
      keyExpressions.push(`${getName(cond.key)} = ${getValue(cond.value)}`);
    }

    for (const cond of this.sortKeyConditions.conditions) {
      const name = getName(cond.key);
      if (cond.operator === "BETWEEN" && Array.isArray(cond.value)) {
        keyExpressions.push(
          `${name} BETWEEN ${getValue(cond.value[0])} AND ${getValue(cond.value[1])}`,
        );
      } else if (cond.operator === "begins_with") {
        keyExpressions.push(`begins_with(${name}, ${getValue(cond.value)})`);
      } else {
        keyExpressions.push(`${name} ${cond.operator} ${getValue(cond.value)}`);
      }
    }

    return keyExpressions.join(" AND ");
  }

  private buildFilterExpression(
    ExpressionAttributeNames: Record<string, string>,
    ExpressionAttributeValues: Record<string, any>,
    nameCounter: { count: number },
    valueCounter: { count: number },
  ): string | undefined {
    if (this.filterConditions.length === 0) return undefined;
    const getName = (key: string) => {
      const existing = Object.entries(ExpressionAttributeNames).find(
        ([, v]) => v === key,
      );
      if (existing) return existing[0];
      const name = `#a${nameCounter.count++}`;
      ExpressionAttributeNames[name] = key;
      return name;
    };
    const getValue = (value: any) => {
      const name = `:v${valueCounter.count++}`;
      ExpressionAttributeValues[name] = value;
      return name;
    };

    const filterExpr: string[] = [];
    this.filterConditions.forEach((f, i) => {
      const name = getName(f.key as string);
      let expr = "";
      if (
        f.operator === "attribute_exists" ||
        f.operator === "attribute_not_exists"
      ) {
        expr = `${f.operator}(${name})`;
      } else if (f.operator === "begins_with" || f.operator === "contains") {
        expr = `${f.operator}(${name}, ${getValue(f.value)})`;
      } else if (f.operator === "IN") {
        const values = (f.value as any[]).map((v) => getValue(v));
        expr = `${name} IN (${values.join(", ")})`;
      } else {
        expr = `${name} ${f.operator} ${getValue(f.value)}`;
      }
      filterExpr.push(i > 0 ? `${f.join} ${expr}` : expr);
    });
    return filterExpr.join(" ");
  }

  private buildProjectionExpression(
    ExpressionAttributeNames: Record<string, string>,
    nameCounter: { count: number },
  ): string | undefined {
    if (!this.ProjectionAttributes?.length) return undefined;
    const getName = (key: string) => {
      const existing = Object.entries(ExpressionAttributeNames).find(
        ([, v]) => v === key,
      );
      if (existing) return existing[0];
      const name = `#a${nameCounter.count++}`;
      ExpressionAttributeNames[name] = key;
      return name;
    };
    return this.ProjectionAttributes.map((a) => getName(a as string)).join(
      ", ",
    );
  }

  private buildExpression() {
    if (this.partitionKeyConditions.conditions.length === 0) {
      throw new Error("Partition key(s) must be specified via wherePK()");
    }
    if (this.indexName) this.validateIndexKeys();

    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const nameCounter = { count: 0 };
    const valueCounter = { count: 0 };

    const KeyConditionExpression = this.buildKeyExpression(
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      nameCounter,
      valueCounter,
    );

    const FilterExpression = this.buildFilterExpression(
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      nameCounter,
      valueCounter,
    );

    const ProjectionExpression = this.buildProjectionExpression(
      ExpressionAttributeNames,
      nameCounter,
    );

    return {
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues, {
        removeUndefinedValues: true,
      }),
      ProjectionExpression,
    };
  }

  async exec(): Promise<{
    items: InferSchema<S>[];
    lastKey?: Record<string, any>;
  }> {
    const {
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ProjectionExpression,
    } = this.buildExpression();
    const accumulatedItems: InferSchema<S>[] = [];
    let currentStartKey = this.ExclusiveStartKey;

    do {
      const remainingToFetch = this.limitCount
        ? Math.max(0, this.limitCount - accumulatedItems.length)
        : undefined;
      const command = new QueryCommand({
        TableName: this.model.tableName,
        IndexName: this.indexName ? String(this.indexName) : undefined,
        KeyConditionExpression,
        FilterExpression,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
        ProjectionExpression,
        Limit: remainingToFetch,
        ScanIndexForward: this.scanIndexForward,
        ExclusiveStartKey: currentStartKey,
        ConsistentRead: this.consistent,
      });

      const result = await this.model.sendCommand<QueryCommandOutput>(command);
      const pageItems =
        result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
      accumulatedItems.push(...pageItems);
      currentStartKey = result.LastEvaluatedKey;
    } while (
      (!this.limitCount || accumulatedItems.length < this.limitCount) &&
      currentStartKey
    );

    return { items: accumulatedItems, lastKey: currentStartKey };
  }

  async *paginate(): AsyncGenerator<InferSchema<S>[], void, unknown> {
    let nextKey = this.ExclusiveStartKey;
    do {
      const qb = this.clone();
      if (nextKey) qb.startKey(nextKey);
      const { items, lastKey } = await qb.exec();
      if (items.length > 0) yield items;
      nextKey = lastKey;
    } while (nextKey);
  }

  async execAll(): Promise<InferSchema<S>[]> {
    const results: InferSchema<S>[] = [];
    for await (const page of this.paginate()) {
      results.push(...page);
    }
    return results;
  }
}
