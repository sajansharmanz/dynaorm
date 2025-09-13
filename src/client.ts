import {
  DynamoDBClient,
  type DynamoDBClientConfig,
} from "@aws-sdk/client-dynamodb";

import type { Schema } from "./schema.js";

import { Model, type ModelOptions } from "./model.js";

// --------------------
// Types
// --------------------
export interface CreateClientOptions {
  config: DynamoDBClientConfig;
  modelOptions?: ModelOptions;
  perModelOptions?: Record<string, ModelOptions>;
}

export type Client<
  Schemas extends Record<string, Schema<any, any, any, any, any>>,
> = {
  client: DynamoDBClient;
} & {
  [K in keyof Schemas]: Model<Schemas[K]>;
};

// --------------------
// Factory Function
// --------------------
export function createClient<
  Schemas extends Record<string, Schema<any, any, any, any, any>>,
>(schemas: Schemas, options: CreateClientOptions): Client<Schemas> {
  const dynamoClient = new DynamoDBClient(options.config);

  const models: { [K in keyof Schemas]: Model<Schemas[K]> } = {} as any;
  for (const name in schemas) {
    const schema = schemas[name];
    if (schema) {
      const modelOpts = options.perModelOptions?.[name] ?? options.modelOptions;
      models[name] = new Model(schema, dynamoClient, modelOpts);
    }
  }

  return new Proxy(
    { client: dynamoClient },
    {
      get(_, prop: string | symbol) {
        if (prop === "client") return dynamoClient;
        if (typeof prop === "string" && prop in models) return models[prop];
        throw new Error(`Model "${String(prop)}" does not exist.`);
      },
      ownKeys() {
        return ["client", ...Object.keys(models)];
      },
      getOwnPropertyDescriptor(_, key: string | symbol) {
        if (key === "client") {
          return { configurable: true, enumerable: true, value: dynamoClient };
        }
        if (typeof key === "string" && key in models) {
          return { configurable: true, enumerable: true, value: models[key] };
        }
        return undefined;
      },
    },
  ) as Client<Schemas>;
}
