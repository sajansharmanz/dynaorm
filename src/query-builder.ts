import {
  QueryCommand,
  type QueryCommandOutput,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

import type { InferSchema, Schema } from "./schema.js";

import { Model, type IndexName } from "./model.js";

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
  | "attribute_not_exists";

interface KeyCondition {
  key: string;
  operator: KeyOperators;
  value: any | [any, any];
}

interface FilterCondition {
  key: string;
  operator: FilterOperators;
  value?: any;
  join?: "AND" | "OR";
}

export class QueryBuilder<
  S extends Schema<any, any, any, any, any>,
  I extends IndexName<S> | undefined = undefined,
> {
  private model: Model<S>;
  private indexName?: I;
  private keyConditions: KeyCondition[] = [];
  private filterConditions: FilterCondition[] = [];
  private limitCount?: number;
  private scanIndexForward?: boolean;
  private ExclusiveStartKey?: Record<string, any>;

  constructor(model: Model<S>) {
    this.model = model;
  }

  // --------------------
  // Configuration Methods
  // --------------------
  index<J extends IndexName<S>>(name: J): QueryBuilder<S, J> {
    const builder = new QueryBuilder<S, J>(this.model);
    builder.indexName = name;
    return builder;
  }

  where(
    key: keyof InferSchema<S>,
    operator: KeyOperators,
    value: any | [any, any],
  ): this {
    this.keyConditions.push({ key: String(key), operator, value });
    return this;
  }

  filter(
    key: keyof InferSchema<S>,
    operator: FilterOperators,
    value?: any,
    join: "AND" | "OR" = "AND",
  ): this {
    this.filterConditions.push({ key: String(key), operator, value, join });
    return this;
  }

  limit(count: number): this {
    this.limitCount = count;
    return this;
  }

  orderBy(asc: boolean): this {
    this.scanIndexForward = asc;
    return this;
  }

  startKey(key: Record<string, any>): this {
    this.ExclusiveStartKey = key;
    return this;
  }

  // --------------------
  // Expression Builder
  // --------------------
  private buildExpression() {
    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};

    const KeyConditionExpression = this.keyConditions
      .map((cond, i) => {
        const attr = `#k${i}`;
        ExpressionAttributeNames[attr] = cond.key;

        if (cond.operator === "BETWEEN") {
          ExpressionAttributeValues[`:v${i}a`] = cond.value[0];
          ExpressionAttributeValues[`:v${i}b`] = cond.value[1];
          return `${attr} BETWEEN :v${i}a AND :v${i}b`;
        }

        if (cond.operator === "begins_with") {
          ExpressionAttributeValues[`:v${i}`] = cond.value;
          return `begins_with(${attr}, :v${i})`;
        }

        ExpressionAttributeValues[`:v${i}`] = cond.value;
        return `${attr} ${cond.operator} :v${i}`;
      })
      .join(" AND ");

    const FilterExpression = this.filterConditions
      .map((cond, i) => {
        const attr = `#f${i}`;
        ExpressionAttributeNames[attr] = cond.key;
        let expr = "";

        if (
          cond.operator === "attribute_exists" ||
          cond.operator === "attribute_not_exists"
        ) {
          expr = `${cond.operator}(${attr})`;
        } else if (
          cond.operator === "begins_with" ||
          cond.operator === "contains"
        ) {
          ExpressionAttributeValues[`:f${i}`] = cond.value;
          expr = `${cond.operator}(${attr}, :f${i})`;
        } else {
          ExpressionAttributeValues[`:f${i}`] = cond.value;
          expr = `${attr} ${cond.operator} :f${i}`;
        }

        return cond.join ? `${cond.join} ${expr}` : expr;
      })
      .join(" ");

    return {
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      KeyConditionExpression,
      FilterExpression,
    };
  }

  // --------------------
  // Execution
  // --------------------
  async exec(): Promise<InferSchema<S>[]> {
    const {
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      KeyConditionExpression,
      FilterExpression,
    } = this.buildExpression();

    const command = new QueryCommand({
      TableName: this.model.tableName,
      IndexName: this.indexName ? String(this.indexName) : undefined,
      KeyConditionExpression,
      FilterExpression: FilterExpression || undefined,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues),
      Limit: this.limitCount,
      ScanIndexForward: this.scanIndexForward,
      ExclusiveStartKey: this.ExclusiveStartKey,
    });

    const result = await this.model.sendCommand<QueryCommandOutput>(command);
    return result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
  }
}
