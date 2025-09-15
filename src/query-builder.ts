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

export class QueryBuilder<S extends Schema<any, any, any, any, any>> {
  private model: Model<S>;
  private partitionKeyCondition?: KeyCondition;
  private sortKeyCondition?: KeyCondition;
  private filterConditions: FilterCondition[] = [];
  private limitCount?: number;
  private scanIndexForward?: boolean;
  private ExclusiveStartKey?: Record<string, any>;
  private ProjectionAttributes?: Array<keyof InferSchema<S>>;
  private indexName?: string;
  private consistent?: boolean;
  private selectMode?:
    | "ALL_ATTRIBUTES"
    | "ALL_PROJECTED_ATTRIBUTES"
    | "COUNT"
    | "SPECIFIC_ATTRIBUTES";
  private returnConsumed?: "INDEXES" | "TOTAL" | "NONE";

  constructor(model: Model<S>) {
    this.model = model;
  }

  where(
    partitionKey: S["partitionKey"],
    operator: "=",
    value: any,
    sortKey?: S["sortKey"] extends undefined
      ? never
      : {
          key: S["sortKey"];
          operator: Exclude<KeyOperators, "=">;
          value: any | [any, any];
        },
  ) {
    if (operator !== "=")
      throw new Error("Partition key condition must use '=' operator.");
    this.partitionKeyCondition = { key: String(partitionKey), operator, value };

    if (sortKey) {
      const allowedSortOperators: KeyOperators[] = [
        "<",
        "<=",
        ">",
        ">=",
        "<>",
        "BETWEEN",
        "begins_with",
      ];
      if (!allowedSortOperators.includes(sortKey.operator)) {
        throw new Error(`Invalid operator for sort key: ${sortKey.operator}`);
      }
      this.sortKeyCondition = {
        key: String(sortKey.key),
        operator: sortKey.operator,
        value: sortKey.value,
      };
    }

    return this;
  }

  filter(
    key: keyof InferSchema<S>,
    operator: FilterOperators,
    value?: any,
    join: "AND" | "OR" = "AND",
  ) {
    this.filterConditions.push({ key: String(key), operator, value, join });
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

  startKey(key: Record<string, any>) {
    this.ExclusiveStartKey = key;
    return this;
  }

  project(attrs: Array<keyof InferSchema<S>>) {
    this.ProjectionAttributes = attrs;
    this.selectMode = "SPECIFIC_ATTRIBUTES";
    return this;
  }

  onIndex(indexName: string) {
    this.indexName = indexName;
    return this;
  }

  consistentRead(isConsistent: boolean = true) {
    this.consistent = isConsistent;
    return this;
  }

  select(
    mode:
      | "ALL_ATTRIBUTES"
      | "ALL_PROJECTED_ATTRIBUTES"
      | "COUNT"
      | "SPECIFIC_ATTRIBUTES",
  ) {
    this.selectMode = mode;
    return this;
  }

  returnConsumedCapacity(mode: "INDEXES" | "TOTAL" | "NONE") {
    this.returnConsumed = mode;
    return this;
  }

  private buildExpression() {
    if (!this.partitionKeyCondition) {
      throw new Error("Partition key must be specified via where()");
    }

    const ExpressionAttributeNames: Record<string, string> = {};
    const ExpressionAttributeValues: Record<string, any> = {};
    let nameCounter = 0;
    let valueCounter = 0;
    const getName = (key: string) => {
      const existing = Object.entries(ExpressionAttributeNames).find(
        ([_, v]) => v === key,
      );
      if (existing) return existing[0];
      const name = `#a${nameCounter++}`;
      ExpressionAttributeNames[name] = key;
      return name;
    };

    const keyConditions: string[] = [];
    const pk = this.partitionKeyCondition;
    const pkName = getName(pk.key);
    const pkValue = `:v${valueCounter++}`;
    ExpressionAttributeValues[pkValue] = pk.value;
    keyConditions.push(`${pkName} ${pk.operator} ${pkValue}`);

    if (this.sortKeyCondition) {
      const sk = this.sortKeyCondition;
      const skName = getName(sk.key);
      if (sk.operator === "BETWEEN") {
        const v1 = `:v${valueCounter++}`;
        const v2 = `:v${valueCounter++}`;
        ExpressionAttributeValues[v1] = (sk.value as any[])[0];
        ExpressionAttributeValues[v2] = (sk.value as any[])[1];
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

    const filterExpr: string[] = [];
    this.filterConditions.forEach((f, i) => {
      const name = getName(f.key);
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
        expr = `${name} IN (${(f.value as any[])
          .map((v) => {
            const p = `:v${valueCounter++}`;
            ExpressionAttributeValues[p] = v;
            return p;
          })
          .join(", ")})`;
      } else {
        const v = `:v${valueCounter++}`;
        ExpressionAttributeValues[v] = f.value;
        expr = `${name} ${f.operator} ${v}`;
      }
      filterExpr.push(i > 0 ? `${f.join} ${expr}` : expr);
    });

    const ProjectionExpression =
      this.selectMode === "SPECIFIC_ATTRIBUTES" &&
      this.ProjectionAttributes?.length
        ? this.ProjectionAttributes.map((a) => getName(a as string)).join(", ")
        : undefined;

    const finalProjection = [
      "ALL_ATTRIBUTES",
      "ALL_PROJECTED_ATTRIBUTES",
      "COUNT",
    ].includes(this.selectMode || "")
      ? undefined
      : ProjectionExpression;

    return {
      KeyConditionExpression,
      FilterExpression: filterExpr.join(" ") || undefined,
      ExpressionAttributeNames,
      ExpressionAttributeValues: marshall(ExpressionAttributeValues),
      ProjectionExpression: finalProjection,
    };
  }

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
      IndexName: this.indexName,
      KeyConditionExpression,
      FilterExpression,
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      Limit: this.limitCount,
      ScanIndexForward: this.scanIndexForward,
      ExclusiveStartKey: this.ExclusiveStartKey,
      ConsistentRead: this.consistent,
      Select: this.selectMode,
      ReturnConsumedCapacity: this.returnConsumed,
      ProjectionExpression,
    });

    const result = await this.model.sendCommand<QueryCommandOutput>(command);
    return result.Items?.map((i) => unmarshall(i) as InferSchema<S>) || [];
  }
}
