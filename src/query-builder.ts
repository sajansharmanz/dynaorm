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

export class QueryBuilder<S extends Schema<any, any, any, any, any>> {
  private model: Model<S>;
  private partitionKeyCondition?: KeyCondition<S, S["partitionKey"]>;
  private sortKeyCondition?: KeyCondition<S, NonNullable<S["sortKey"]>>;
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

  /** Set partition key condition (required) */
  where(
    partitionKey: S["partitionKey"],
    value: InferSchema<S>[S["partitionKey"]],
    sortKey?: {
      key: NonNullable<S["sortKey"]>;
      operator: Exclude<KeyOperators, "=">;
      value:
        | InferSchema<S>[NonNullable<S["sortKey"]>]
        | [
            InferSchema<S>[NonNullable<S["sortKey"]>],
            InferSchema<S>[NonNullable<S["sortKey"]>],
          ];
    },
  ) {
    this.partitionKeyCondition = { key: partitionKey, operator: "=", value };
    if (sortKey) {
      this.sortKeyCondition = {
        key: sortKey.key,
        operator: sortKey.operator,
        value: sortKey.value,
      };
    }
    return this;
  }

  /** Add filter conditions */
  filter<K extends keyof InferSchema<S>>(
    key: K,
    operator: FilterOperators,
    value?: InferSchema<S>[K] | InferSchema<S>[K][],
    join: "AND" | "OR" = "AND",
  ) {
    if (operator === "IN" && !Array.isArray(value)) {
      throw new Error("IN operator requires an array of values");
    }
    this.filterConditions.push({ key, operator, value, join });
    return this;
  }

  /** Set limit */
  limit(count: number) {
    this.limitCount = count;
    return this;
  }

  /** Set scan order */
  orderBy(asc: boolean) {
    this.scanIndexForward = asc;
    return this;
  }

  /** Start key for pagination */
  startKey(key: Record<string, any>) {
    this.ExclusiveStartKey = key;
    return this;
  }

  /** Projection attributes */
  project(attrs: Array<keyof InferSchema<S>>) {
    this.ProjectionAttributes = attrs;
    return this;
  }

  /** Use a secondary index */
  onIndex(
    index: keyof S["globalSecondaryIndexes"] | keyof S["localSecondaryIndexes"],
  ) {
    this.indexName = index;
    return this;
  }

  /** Consistent read */
  consistentRead(isConsistent = true) {
    this.consistent = isConsistent;
    return this;
  }

  /** Build DynamoDB expressions */
  private buildExpression() {
    if (!this.partitionKeyCondition)
      throw new Error("Partition key must be specified via where()");

    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    let nameCounter = 0;
    let valueCounter = 0;

    const getName = (key: string) => {
      const existing = Object.entries(ExpressionAttributeNames).find(
        ([, v]) => v === key,
      );
      if (existing) return existing[0];
      const name = `#a${nameCounter++}`;
      ExpressionAttributeNames[name] = key;
      return name;
    };

    // --- Key conditions ---
    const keyConditions: string[] = [];
    const pk = this.partitionKeyCondition;
    const pkName = getName(pk.key as string);
    const pkValue = `:v${valueCounter++}`;
    ExpressionAttributeValues[pkValue] = pk.value;
    keyConditions.push(`${pkName} = ${pkValue}`);

    if (this.sortKeyCondition) {
      const sk = this.sortKeyCondition;
      const skName = getName(sk.key as string);
      if (sk.operator === "BETWEEN" && Array.isArray(sk.value)) {
        const v1 = `:v${valueCounter++}`;
        const v2 = `:v${valueCounter++}`;
        ExpressionAttributeValues[v1] = sk.value[0];
        ExpressionAttributeValues[v2] = sk.value[1];
        keyConditions.push(`${skName} BETWEEN ${v1} AND ${v2}`);
      } else if (sk.operator === "begins_with") {
        const v = `:v${valueCounter++}`;
        ExpressionAttributeValues[v] = sk.value;
        keyConditions.push(`begins_with(${skName}, ${v})`);
      } else {
        const v = `:v${valueCounter++}`;
        ExpressionAttributeValues[v] = sk.value;
        keyConditions.push(`${skName} ${sk.operator} ${v}`);
      }
    }

    const KeyConditionExpression = keyConditions.join(" AND ");

    // --- Filter expressions ---
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
        const v = `:v${valueCounter++}`;
        ExpressionAttributeValues[v] = f.value;
        expr = `${f.operator}(${name}, ${v})`;
      } else if (f.operator === "IN") {
        const values = (f.value as any[]).map((v) => {
          const p = `:v${valueCounter++}`;
          ExpressionAttributeValues[p] = v;
          return p;
        });
        expr = `${name} IN (${values.join(", ")})`;
      } else {
        const v = `:v${valueCounter++}`;
        ExpressionAttributeValues[v] = f.value;
        expr = `${name} ${f.operator} ${v}`;
      }
      filterExpr.push(i > 0 ? `${f.join} ${expr}` : expr);
    });

    const ProjectionExpression = this.ProjectionAttributes?.length
      ? this.ProjectionAttributes.map((a) => getName(a as string)).join(", ")
      : undefined;

    return {
      KeyConditionExpression,
      FilterExpression: filterExpr.length ? filterExpr.join(" ") : undefined,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues),
      ProjectionExpression,
    };
  }

  /** Execute query */
  async exec(): Promise<InferSchema<S>[]> {
    const {
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ProjectionExpression,
    } = this.buildExpression();

    const command = new QueryCommand({
      TableName: this.model.tableName,
      IndexName: this.indexName as string,
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      Limit: this.limitCount,
      ScanIndexForward: this.scanIndexForward,
      ExclusiveStartKey: this.ExclusiveStartKey,
      ConsistentRead: this.consistent,
      ProjectionExpression,
    });

    const result = await this.model.sendCommand<QueryCommandOutput>(command);
    return result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
  }
}
