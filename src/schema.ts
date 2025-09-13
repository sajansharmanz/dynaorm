import { z } from "zod";

// --------------------
// Utility Types
// --------------------
export type Keys<T extends z.ZodRawShape> = keyof z.infer<z.ZodObject<T>>;

export type Projection<T extends z.ZodRawShape> =
  | { type: "ALL" }
  | { type: "KEYS_ONLY" }
  | { type: "INCLUDE"; attributes: Keys<T>[] };

// --------------------
// Index Types
// --------------------
export interface GlobalSecondaryIndex<T extends z.ZodRawShape> {
  partitionKey: Keys<T>;
  sortKey?: Keys<T>;
  projection?: Projection<T>;
}

export type GlobalSecondaryIndexes<T extends z.ZodRawShape> = Record<
  string,
  GlobalSecondaryIndex<T>
>;

export interface LocalSecondaryIndex<T extends z.ZodRawShape> {
  sortKey: Keys<T>;
  projection?: Projection<T>;
}

export type LocalSecondaryIndexes<T extends z.ZodRawShape> = Record<
  string,
  LocalSecondaryIndex<T>
>;

// --------------------
// Schema Type
// --------------------
export interface Schema<
  T extends z.ZodRawShape,
  PK extends Keys<T>,
  SK extends Exclude<Keys<T>, PK> | undefined = undefined,
  GSI extends Record<string, GlobalSecondaryIndex<T>> = {},
  LSI extends Record<string, LocalSecondaryIndex<T>> = {},
> {
  tableName: string;
  fields: z.ZodObject<T>;
  partitionKey: PK;
  sortKey?: SK;
  globalSecondaryIndexes: GSI;
  localSecondaryIndexes: LSI;
}

// Infer Type from Schema
export type InferSchema<S extends Schema<any, any, any, any, any>> = z.infer<
  S["fields"]
>;

// --------------------
// Schema Definition Helper
// --------------------
export function defineSchema<
  const T extends z.ZodRawShape,
  const PK extends Keys<T>,
  const SK extends Exclude<Keys<T>, PK> | undefined = undefined,
  const GSI extends Record<string, GlobalSecondaryIndex<T>> = {},
  const LSI extends Record<string, LocalSecondaryIndex<T>> = {},
>(schema: {
  tableName: string;
  fields: z.ZodObject<T>;
  partitionKey: PK;
  sortKey?: SK;
  globalSecondaryIndexes?: GSI;
  localSecondaryIndexes?: LSI;
}): Schema<T, PK, SK, GSI, LSI> {
  const gsiNames = Object.keys(schema.globalSecondaryIndexes ?? {});
  const lsiNames = Object.keys(schema.localSecondaryIndexes ?? {});

  // Ensure no duplicate index names between GSIs and LSIs
  const duplicates = gsiNames.filter((name) => lsiNames.includes(name));
  if (duplicates.length) {
    throw new Error(
      `Index names must be unique across GSIs and LSIs. Duplicates: ${duplicates.join(
        ", ",
      )}`,
    );
  }

  // Sort key cannot be the same as partition key
  if (
    schema.sortKey &&
    String(schema.sortKey) === String(schema.partitionKey)
  ) {
    throw new Error("Table sortKey cannot be the same as partitionKey");
  }

  return {
    ...schema,
    globalSecondaryIndexes: schema.globalSecondaryIndexes ?? ({} as GSI),
    localSecondaryIndexes: schema.localSecondaryIndexes ?? ({} as LSI),
  } as Schema<T, PK, SK, GSI, LSI>;
}
