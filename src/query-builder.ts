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

interface KeyCondition<
  S extends Schema<any, any, any, any, any>,
  K extends S["partitionKey"] | NonNullable<S["sortKey"]>,
> {
  key: K;
  operator: KeyOperators;
  value: InferSchema<S>[K] | [InferSchema<S>[K], InferSchema<S>[K]];
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

// type SortKeyCondition<S extends Schema<any, any, any, any, any>> = {
//   key: NonNullable<S["sortKey"]>;
//   operator: KeyOperators;
//   value:
//     | InferSchema<S>[NonNullable<S["sortKey"]>]
//     | [
//         InferSchema<S>[NonNullable<S["sortKey"]>],
//         InferSchema<S>[NonNullable<S["sortKey"]>],
//       ];
// };

export class QueryBuilder<S extends Schema<any, any, any, any, any>> {
  private model: Model<S>;
  private partitionKeyCondition?: KeyCondition<S, S["partitionKey"]>;
  private sortKeyCondition?: S["sortKey"] extends keyof InferSchema<S>
    ? {
        key: NonNullable<S["sortKey"]>;
        operator: KeyOperators;
        value:
          | InferSchema<S>[NonNullable<S["sortKey"]>]
          | [
              InferSchema<S>[NonNullable<S["sortKey"]>],
              InferSchema<S>[NonNullable<S["sortKey"]>],
            ];
      }
    : never;
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
      partitionKeyCondition: this.partitionKeyCondition,
      sortKeyCondition: this.sortKeyCondition,
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

  wherePK(value: InferSchema<S>[S["partitionKey"]]) {
    this.partitionKeyCondition = {
      key: this.model.schema.partitionKey,
      operator: "=",
      value,
    };
    return this;
  }

  whereSK(
    operator: Exclude<KeyOperators, "BETWEEN" | "begins_with">,
    value: InferSchema<S>[NonNullable<S["sortKey"]>],
  ): this;
  whereSK(
    operator: "BETWEEN",
    value: [
      InferSchema<S>[NonNullable<S["sortKey"]>],
      InferSchema<S>[NonNullable<S["sortKey"]>],
    ],
  ): this;
  whereSK(
    operator: "begins_with",
    value: InferSchema<S>[NonNullable<S["sortKey"]>],
  ): this;
  whereSK(
    operator: KeyOperators,
    value:
      | InferSchema<S>[NonNullable<S["sortKey"]>]
      | [
          InferSchema<S>[NonNullable<S["sortKey"]>],
          InferSchema<S>[NonNullable<S["sortKey"]>],
        ],
  ): this {
    const sortKey = this.model.schema.sortKey!;
    if (!sortKey) {
      throw new Error("Table has no sort key to query on.");
    }

    if (operator === "BETWEEN") {
      if (!Array.isArray(value) || value.length !== 2) {
        throw new Error("BETWEEN operator requires an array of two values.");
      }
    } else if (operator === "begins_with") {
      if (Array.isArray(value)) {
        throw new Error("begins_with operator requires a single value.");
      }
    } else {
      if (Array.isArray(value)) {
        throw new Error(`${operator} operator cannot use an array`);
      }
    }

    this.sortKeyCondition = {
      key: sortKey,
      operator,
      value,
    } as S["sortKey"] extends keyof InferSchema<S>
      ? {
          key: NonNullable<S["sortKey"]>;
          operator: KeyOperators;
          value: typeof value;
        }
      : never;
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

    if (operator === "IN") {
      if (!Array.isArray(value) || value.length === 0) {
        throw new Error("IN operator requires a non-empty array of values.");
      }
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
    const indexPK = index.partitionKey;
    const indexSK = index.sortKey;

    if (this.partitionKeyCondition!.key !== indexPK) {
      throw new Error(
        `Partition key for query builder (${String(
          this.partitionKeyCondition!.key,
        )}) does not match index partition key (${String(indexPK)})`,
      );
    }

    if (indexSK && this.sortKeyCondition?.key !== indexSK) {
      throw new Error(
        `Sort key for query builder (${String(
          this.sortKeyCondition?.key,
        )}) does not match index sort key (${String(indexSK)})`,
      );
    }
  }

  private buildKeyExpression(
    ExpressionAttributeNames: Record<string, string>,
    ExpressionAttributeValues: Record<string, any>,
    nameCounter: { count: number },
    valueCounter: { count: number },
    valueMap: Map<any, string>,
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
      if (valueMap.has(value)) {
        return valueMap.get(value)!;
      }
      const name = `:v${valueCounter.count++}`;
      ExpressionAttributeValues[name] = value;
      valueMap.set(value, name);
      return name;
    };

    const keyConditions: string[] = [];
    const pk = this.partitionKeyCondition!;
    const pkName = getName(pk.key as string);
    keyConditions.push(`${pkName} = ${getValue(pk.value)}`);

    if (this.sortKeyCondition) {
      const sk = this.sortKeyCondition;
      const skName = getName(sk.key as string);
      if (sk.operator === "BETWEEN" && Array.isArray(sk.value)) {
        keyConditions.push(
          `${skName} BETWEEN ${getValue(sk.value[0])} AND ${getValue(
            sk.value[1],
          )}`,
        );
      } else if (sk.operator === "begins_with") {
        keyConditions.push(`begins_with(${skName}, ${getValue(sk.value)})`);
      } else {
        keyConditions.push(`${skName} ${sk.operator} ${getValue(sk.value)}`);
      }
    }
    return keyConditions.join(" AND ");
  }

  private buildFilterExpression(
    ExpressionAttributeNames: Record<string, string>,
    ExpressionAttributeValues: Record<string, any>,
    nameCounter: { count: number },
    valueCounter: { count: number },
    valueMap: Map<any, string>,
  ): string | undefined {
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
      if (valueMap.has(value)) {
        return valueMap.get(value)!;
      }
      const name = `:v${valueCounter.count++}`;
      ExpressionAttributeValues[name] = value;
      valueMap.set(value, name);
      return name;
    };

    if (this.filterConditions.length === 0) return undefined;

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
        const v = getValue(f.value);
        expr = `${f.operator}(${name}, ${v})`;
      } else if (f.operator === "IN") {
        const values = (f.value as InferSchema<S>[typeof f.key][]).map((v) =>
          getValue(v),
        );
        expr = `${name} IN (${values.join(", ")})`;
      } else {
        const v = getValue(f.value);
        expr = `${name} ${f.operator} ${v}`;
      }
      filterExpr.push(i > 0 ? `${f.join} ${expr}` : expr);
    });
    return filterExpr.join(" ");
  }

  private buildProjectionExpression(
    ExpressionAttributeNames: Record<string, string>,
    nameCounter: { count: number },
  ): string | undefined {
    if (!this.ProjectionAttributes || this.ProjectionAttributes.length === 0) {
      return undefined;
    }
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
    if (!this.partitionKeyCondition) {
      throw new Error("Partition key must be specified via wherePK()");
    }
    if (this.indexName) this.validateIndexKeys();

    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    const nameCounter = { count: 0 };
    const valueCounter = { count: 0 };
    const valueMap = new Map<any, string>();

    const KeyConditionExpression = this.buildKeyExpression(
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      nameCounter,
      valueCounter,
      valueMap,
    );
    const FilterExpression = this.buildFilterExpression(
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      nameCounter,
      valueCounter,
      valueMap,
    );
    const ProjectionExpression = this.buildProjectionExpression(
      ExpressionAttributeNames,
      nameCounter,
    );

    return {
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues),
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

    const command = new QueryCommand({
      TableName: this.model.tableName,
      IndexName: this.indexName ? String(this.indexName) : undefined,
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ProjectionExpression,
      Limit: this.limitCount,
      ScanIndexForward: this.scanIndexForward,
      ExclusiveStartKey: this.ExclusiveStartKey,
      ConsistentRead: this.consistent,
    });

    const result = await this.model.sendCommand<QueryCommandOutput>(command);
    return {
      items: result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [],
      lastKey: result.LastEvaluatedKey,
    };
  }

  async *paginate(): AsyncGenerator<InferSchema<S>[], void, unknown> {
    let nextKey = this.ExclusiveStartKey;
    const queryBuilder = this.clone();

    do {
      if (nextKey) {
        queryBuilder.startKey(nextKey);
      }
      const { items, lastKey } = await queryBuilder.exec();
      if (items.length > 0) {
        yield items;
      }
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
