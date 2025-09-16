```ts
// src/types.ts
import {
  MongoClient,
  Db,
  Collection,
  ObjectId,
  ClientSession,
  MongoClientOptions,
} from "mongodb";

// ========================== BASIC MONGODB TYPES ==========================
export interface MongoConnection {
  client: MongoClient;
  db: Db;
  isConnected: boolean;
}

export interface MongoResult {
  rows: any[];
  rowsAffected: number;
  lastInsertId?: ObjectId | string;
  insertedIds?: ObjectId[];
}

export interface MongoAdapter {
  disconnect(connectionString: string, databaseName: string): unknown;
  connect(
    connectionString: string,
    dbName: string,
    options?: MongoClientOptions
  ): Promise<MongoConnection>;
  isSupported(): boolean;
}

export interface MongoQueryOptions {
  sort?: Record<string, 1 | -1>;
  limit?: number;
  skip?: number;
  projection?: Record<string, 0 | 1>;
}

export interface MongoWhereClause {
  [key: string]: any;
}

// ========================== SHARED SCHEMA TYPES (Compatible with SQLite) ==========================

// Enhanced schema types based on SQLiteDAO but adapted for MongoDB
export interface TypeMappingConfig {
  type_mapping: {
    [targetType: string]: {
      [sourceType: string]: string;
    };
  };
}

export interface ColumnDefinition {
  name: string;
  type: string;
  precision?: number;
  scale?: number;
  option_key?: string;
  description?: string;
  nullable?: boolean;
  default?: any;
  primary_key?: boolean;
  auto_increment?: boolean;
  enum?: string[] | number[];
  unique?: boolean;
  constraints?: string;
  length?: number;
}

export interface Column {
  name: string;
  value?: any;
}

export interface WhereClause {
  name: string;
  value: any;
  operator?: string;
}

export interface OrderByClause {
  name: string;
  direction?: "ASC" | "DESC";
}

export interface LimitOffset {
  limit?: number;
  offset?: number;
}

export interface QueryTable {
  name: string;
  cols: Column[];
  wheres?: WhereClause[];
  orderbys?: OrderByClause[];
  limitOffset?: LimitOffset;
}

export interface JoinClause {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  on: string;
}

export interface IndexDefinition {
  name: string;
  columns: string[];
  unique?: boolean;
  description?: string;
}

export type ForeignKeyAction =
  | "CASCADE"
  | "RESTRICT"
  | "SET NULL"
  | "NO ACTION"
  | undefined;

export interface ForeignKeyDefinition {
  name: string;
  column: string;
  references: {
    table: string;
    column: string;
  };
  on_delete?: string | ForeignKeyAction;
  on_update?: string | ForeignKeyAction;
  description?: string;
}

export interface TableDefinition {
  name: string;
  cols: ColumnDefinition[];
  description?: string;
  indexes?: IndexDefinition[];
  foreign_keys?: ForeignKeyDefinition[];
}

// Main DatabaseSchema interface - Compatible with SQLite but adapted for MongoDB
export interface DatabaseSchema {
  version: string;
  database_name: string;
  description?: string;
  type_mapping?: TypeMappingConfig["type_mapping"];
  schemas: Record<
    string,
    {
      description?: string;
      cols: ColumnDefinition[];
      indexes?: IndexDefinition[];
      foreign_keys?: ForeignKeyDefinition[]; // Will be ignored in MongoDB but kept for compatibility
    }
  >;
}

// ========================== MONGODB SPECIFIC TYPES ==========================

export interface MongoIndexDefinition {
  name: string;
  keys: Record<string, 1 | -1 | "text" | "2dsphere">;
  options?: {
    unique?: boolean;
    sparse?: boolean;
    background?: boolean;
    expireAfterSeconds?: number;
  };
}

export interface MongoCollection {
  name: string;
  schema?: Record<string, any>;
  indexes?: MongoIndexDefinition[];
  validation?: Record<string, any>;
}

export interface MongoDatabaseSchema {
  version: string;
  database_name: string;
  description?: string;
  collections: Record<string, MongoCollection>;
}

// ========================== TRANSACTION TYPES ==========================
export interface TransactionOperation {
  type: "insert" | "update" | "delete" | "select";
  table: QueryTable;
}

// ========================== IMPORT/EXPORT TYPES ==========================
export interface ImportOptions {
  tableName: string;
  data: Record<string, any>[];
  batchSize?: number;
  onProgress?: (processed: number, total: number) => void;
  onError?: (
    error: Error,
    rowIndex: number,
    rowData: Record<string, any>
  ) => void;
  skipErrors?: boolean;
  validateData?: boolean;
  updateOnConflict?: boolean;
  conflictColumns?: string[];
  includeAutoIncrementPK?: boolean;
}

export interface ImportResult {
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{
    rowIndex: number;
    error: string;
    rowData: Record<string, any>;
  }>;
  executionTime: number;
}

// ========================== COLUMN MAPPING TYPES ==========================
export interface ColumnMapping {
  sourceColumn: string;
  targetColumn: string;
  transform?: (value: any) => any;
}

// ========================== DATABASE FACTORY OPTIONS ==========================
export interface DbFactoryOptions {
  config?: DatabaseSchema; // Option 1: Provide a config object directly
  configAsset?: any; // Option 3: Provide a required JSON asset
  dbDirectory?: string; // Optional: Directory to store the .db file (not used in MongoDB)
  adapter?: MongoAdapter; // Optional: Specific adapter to use
}

// ========================== HEALTH CHECK TYPES ==========================
export interface ServiceStatus {
  schemaName: string;
  collectionName: string; // Changed from tableName
  isOpened: boolean;
  isInitialized: boolean;
  hasDao: boolean;
}

export interface HealthCheckResult {
  healthy: boolean;
  schemaName: string;
  recordCount?: number;
  error?: string;
  timestamp: string;
}

// ========================== MONGODB TYPE MAPPING ==========================
export const MONGODB_TYPE_MAPPING = {
  mongodb: {
    // String types
    string: "String",
    varchar: "String",
    char: "String",
    text: "String",
    email: "String",
    url: "String",
    uuid: "String",

    // Numeric types
    integer: "Number",
    int: "Number",
    bigint: "Number",
    smallint: "Number",
    tinyint: "Number",
    decimal: "Number",
    numeric: "Number",
    float: "Number",
    double: "Number",

    // Boolean
    boolean: "Boolean",
    bool: "Boolean",

    // Date/Time types
    timestamp: "Date",
    datetime: "Date",
    date: "Date",
    time: "Date",

    // Complex types
    json: "Object",
    array: "Array",
    object: "Object",

    // Binary types
    blob: "Buffer",
    binary: "Buffer",

    // MongoDB specific
    objectid: "ObjectId",
    mixed: "Mixed",
    // Add index signature here
    // [key: string]: string;
  },
};

// ========================== UTILITY TYPES ==========================

// Row type for MongoDB documents
export interface MongoRow {
  [key: string]: any;
  _id?: ObjectId;
}

// Enhanced result type for MongoDB operations
export interface MongoOperationResult extends MongoResult {
  acknowledged?: boolean;
  insertedCount?: number;
  matchedCount?: number;
  modifiedCount?: number;
  deletedCount?: number;
  upsertedCount?: number;
  upsertedId?: ObjectId;
}

// Aggregation pipeline types
export interface AggregationPipeline {
  $match?: Record<string, any>;
  $group?: Record<string, any>;
  $sort?: Record<string, 1 | -1>;
  $limit?: number;
  $skip?: number;
  $project?: Record<string, 0 | 1>;
  $unwind?: string | Record<string, any>;
  $lookup?: {
    from: string;
    localField: string;
    foreignField: string;
    as: string;
  };
  [key: string]: any;
}

// Update operation types
export interface UpdateOperation {
  $set?: Record<string, any>;
  $unset?: Record<string, any>;
  $inc?: Record<string, number>;
  $push?: Record<string, any>;
  $pull?: Record<string, any>;
  $addToSet?: Record<string, any>;
  [key: string]: any;
}

// ========================== MIGRATION HELPER TYPES ==========================

// Interface for database migration between SQLite and MongoDB
export interface MigrationMapping {
  sourceTable: string;
  targetCollection: string;
  columnMappings: ColumnMapping[];
  transformRules?: {
    [columnName: string]: (value: any) => any;
  };
  ignoreColumns?: string[];
  customFields?: Record<string, any>;
}

export interface MigrationConfig {
  sourceType: "sqlite" | "mongodb";
  targetType: "sqlite" | "mongodb";
  mappings: MigrationMapping[];
  batchSize?: number;
  onProgress?: (processed: number, total: number) => void;
  onError?: (error: Error, record: any) => void;
}

// ========================== VALIDATION TYPES ==========================

export interface ValidationRule {
  required?: boolean;
  type?: string;
  min?: number;
  max?: number;
  pattern?: string | RegExp;
  enum?: (string | number)[];
  custom?: (value: any) => boolean | string;
}

export interface SchemaValidation {
  [fieldName: string]: ValidationRule;
}
```

```ts
// ./src/adapter/base-adapter.ts
import { MongoClientOptions, MongoClient } from "mongodb";
import { MongoAdapter, MongoConnection } from "../types";

// ========================== MONGODB ADAPTER ==========================
export class BaseMongoAdapter implements MongoAdapter {
  private connections: Map<string, MongoConnection> = new Map();

  isSupported(): boolean {
    try {
      require("mongodb");
      return true;
    } catch {
      return false;
    }
  }

  async connect(
    connectionString: string,
    dbName: string,
    options?: MongoClientOptions
  ): Promise<MongoConnection> {
    const connectionKey = `${connectionString}_${dbName}`;

    // Return existing connection if available
    const existingConnection = this.connections.get(connectionKey);
    if (existingConnection?.isConnected) {
      return existingConnection;
    }

    try {
      const client = new MongoClient(connectionString, {
        ...options,
        serverSelectionTimeoutMS: 5000,
        connectTimeoutMS: 10000,
      });

      await client.connect();
      const db = client.db(dbName);

      // Test connection
      await db.admin().ping();

      const connection: MongoConnection = {
        client,
        db,
        isConnected: true,
      };

      this.connections.set(connectionKey, connection);
      return connection;
    } catch (error) {
      throw new Error(
        `Failed to connect to MongoDB: ${(error as Error).message}`
      );
    }
  }

  async disconnect(connectionString: string, dbName: string): Promise<void> {
    const connectionKey = `${connectionString}_${dbName}`;
    const connection = this.connections.get(connectionKey);

    if (connection?.client) {
      await connection.client.close();
      connection.isConnected = false;
      this.connections.delete(connectionKey);
    }
  }

  async disconnectAll(): Promise<void> {
    const disconnectPromises = Array.from(this.connections.values()).map(
      async (connection) => {
        if (connection.client && connection.isConnected) {
          await connection.client.close();
        }
      }
    );

    await Promise.all(disconnectPromises);
    this.connections.clear();
  }
}
```

```ts
// src/core/base-service.ts

import { ObjectId } from "mongodb";
import {
  MongoQueryOptions,
  WhereClause,
  OrderByClause,
  LimitOffset,
  AggregationPipeline,
  UpdateOperation,
  ImportOptions,
  ImportResult,
} from "../types";
import { MongoUniversalDAO } from "./universal-dao";

// ========================== BASE SERVICE FOR MONGODB ==========================
export abstract class MongoBaseService<T = any> {
  protected dao: MongoUniversalDAO;
  protected collectionName: string;
  protected isInitialized: boolean = false;

  constructor(dao: MongoUniversalDAO, collectionName: string) {
    this.dao = dao;
    this.collectionName = collectionName;
  }

  async init(): Promise<this> {
    if (!this.isInitialized) {
      await this.dao.connect();
      this.isInitialized = true;
    }
    return this;
  }

  // ========================== BASIC CRUD OPERATIONS ==========================

  async create(data: Partial<T>): Promise<T> {
    await this.init();
    const result = await this.dao.insert(
      this.collectionName,
      data as Record<string, any>
    );
    return result.rows[0] as T;
  }

  async createMany(dataArray: Partial<T>[]): Promise<T[]> {
    await this.init();
    const result = await this.dao.insertMany(
      this.collectionName,
      dataArray as Record<string, any>[]
    );
    return result.rows as T[];
  }

  async findById(id: string | ObjectId): Promise<T | null> {
    await this.init();
    const objectId = typeof id === "string" ? new ObjectId(id) : id;
    return (await this.dao.findOne(this.collectionName, {
      _id: objectId,
    })) as T | null;
  }

  async findOne(filter: Record<string, any> = {}): Promise<T | null> {
    await this.init();
    return (await this.dao.findOne(this.collectionName, filter)) as T | null;
  }

  async findMany(
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<T[]> {
    await this.init();
    return (await this.dao.find(this.collectionName, filter, options)) as T[];
  }

  async updateById(
    id: string | ObjectId,
    update: Partial<T>
  ): Promise<boolean> {
    await this.init();
    const objectId = typeof id === "string" ? new ObjectId(id) : id;
    const result = await this.dao.update(
      this.collectionName,
      { _id: objectId },
      update as Record<string, any>
    );
    return result.rowsAffected > 0;
  }

  async updateMany(
    filter: Record<string, any>,
    update: Partial<T>
  ): Promise<number> {
    await this.init();
    const result = await this.dao.update(
      this.collectionName,
      filter,
      update as Record<string, any>,
      { multi: true }
    );
    return result.rowsAffected;
  }

  async deleteById(id: string | ObjectId): Promise<boolean> {
    await this.init();
    const objectId = typeof id === "string" ? new ObjectId(id) : id;
    const result = await this.dao.delete(this.collectionName, {
      _id: objectId,
    });
    return result.rowsAffected > 0;
  }

  async deleteMany(filter: Record<string, any>): Promise<number> {
    await this.init();
    const result = await this.dao.delete(this.collectionName, filter, {
      multi: true,
    });
    return result.rowsAffected;
  }

  async count(filter: Record<string, any> = {}): Promise<number> {
    await this.init();
    return await this.dao.count(this.collectionName, filter);
  }

  async exists(filter: Record<string, any>): Promise<boolean> {
    const count = await this.count(filter);
    return count > 0;
  }

  // ========================== ADVANCED QUERY METHODS ==========================

  /**
   * Find with SQLite-compatible where clauses
   */
  async findWithWhere(
    wheres: WhereClause[],
    orderBys?: OrderByClause[],
    limitOffset?: LimitOffset
  ): Promise<T[]> {
    await this.init();

    const filter = this.dao.buildMongoQuery(wheres);
    const options: MongoQueryOptions = {};

    if (orderBys && orderBys.length > 0) {
      options.sort = this.dao.buildMongoSort(orderBys);
    }

    if (limitOffset?.limit) {
      options.limit = limitOffset.limit;
    }

    if (limitOffset?.offset) {
      options.skip = limitOffset.offset;
    }

    return (await this.dao.find(this.collectionName, filter, options)) as T[];
  }

  /**
   * Find with pagination
   */
  async findWithPagination(
    filter: Record<string, any> = {},
    page: number = 1,
    pageSize: number = 10,
    sort?: Record<string, 1 | -1>
  ): Promise<{
    data: T[];
    pagination: {
      page: number;
      pageSize: number;
      total: number;
      totalPages: number;
      hasNext: boolean;
      hasPrev: boolean;
    };
  }> {
    await this.init();

    const skip = (page - 1) * pageSize;
    const total = await this.dao.count(this.collectionName, filter);
    const totalPages = Math.ceil(total / pageSize);

    const options: MongoQueryOptions = {
      limit: pageSize,
      skip: skip,
    };

    if (sort) {
      options.sort = sort;
    }

    const data = (await this.dao.find(
      this.collectionName,
      filter,
      options
    )) as T[];

    return {
      data,
      pagination: {
        page,
        pageSize,
        total,
        totalPages,
        hasNext: page < totalPages,
        hasPrev: page > 1,
      },
    };
  }

  /**
   * Find with text search
   */
  async search(
    searchText: string,
    searchFields: string[],
    additionalFilter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<T[]> {
    await this.init();

    const searchFilter: Record<string, any> = {
      $or: searchFields.map((field) => ({
        [field]: { $regex: searchText, $options: "i" },
      })),
    };

    const combinedFilter = {
      ...additionalFilter,
      ...searchFilter,
    };

    return (await this.dao.find(
      this.collectionName,
      combinedFilter,
      options
    )) as T[];
  }

  // ========================== AGGREGATION OPERATIONS ==========================

  async aggregate(pipeline: AggregationPipeline[]): Promise<any[]> {
    await this.init();
    return await this.dao.aggregate(this.collectionName, pipeline);
  }

  /**
   * Group by field with count
   */
  async groupByCount(
    groupField: string,
    filter: Record<string, any> = {}
  ): Promise<Array<{ _id: any; count: number }>> {
    const pipeline: AggregationPipeline[] = [];

    if (Object.keys(filter).length > 0) {
      pipeline.push({ $match: filter });
    }

    pipeline.push(
      { $group: { _id: `$${groupField}`, count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    );

    return await this.aggregate(pipeline);
  }

  /**
   * Get statistics for numeric field
   */
  async getFieldStats(
    numericField: string,
    filter: Record<string, any> = {}
  ): Promise<{
    count: number;
    sum: number;
    avg: number;
    min: number;
    max: number;
  }> {
    const pipeline: AggregationPipeline[] = [];

    if (Object.keys(filter).length > 0) {
      pipeline.push({ $match: filter });
    }

    pipeline.push({
      $group: {
        _id: null,
        count: { $sum: 1 },
        sum: { $sum: `$${numericField}` },
        avg: { $avg: `$${numericField}` },
        min: { $min: `$${numericField}` },
        max: { $max: `$${numericField}` },
      },
    });

    const result = await this.aggregate(pipeline);

    return (
      result[0] || {
        count: 0,
        sum: 0,
        avg: 0,
        min: 0,
        max: 0,
      }
    );
  }

  // ========================== BULK OPERATIONS ==========================

  /**
   * Bulk insert with batch processing
   */
  async bulkInsert(
    documents: Partial<T>[],
    batchSize: number = 1000
  ): Promise<ImportResult> {
    await this.init();

    const startTime = Date.now();
    let successCount = 0;
    let errorCount = 0;
    const errors: Array<{
      rowIndex: number;
      error: string;
      rowData: Record<string, any>;
    }> = [];

    try {
      const result = await this.dao.bulkInsert(
        this.collectionName,
        documents as Record<string, any>[],
        batchSize
      );

      successCount = result.rowsAffected;
    } catch (error) {
      errorCount = documents.length;
      errors.push({
        rowIndex: 0,
        error: (error as Error).message,
        rowData: {},
      });
    }

    return {
      totalRows: documents.length,
      successRows: successCount,
      errorRows: errorCount,
      errors,
      executionTime: Date.now() - startTime,
    };
  }

  /**
   * Bulk update with filter
   */
  async bulkUpdate(
    filter: Record<string, any>,
    update: UpdateOperation,
    options?: { upsert?: boolean }
  ): Promise<number> {
    await this.init();

    const result = await this.dao.update(this.collectionName, filter, update, {
      multi: true,
      upsert: options?.upsert,
    });

    return result.rowsAffected;
  }

  /**
   * Bulk delete with filter
   */
  async bulkDelete(filter: Record<string, any>): Promise<number> {
    await this.init();

    const result = await this.dao.delete(this.collectionName, filter, {
      multi: true,
    });
    return result.rowsAffected;
  }

  // ========================== TRANSACTION SUPPORT ==========================

  async executeTransaction<R>(callback: () => Promise<R>): Promise<R> {
    await this.init();
    await this.dao.beginTransaction();

    try {
      const result = await callback();
      await this.dao.commitTransaction();
      return result;
    } catch (error) {
      await this.dao.rollbackTransaction();
      throw error;
    }
  }

  /**
   * Execute multiple operations in transaction
   */
  async executeMultipleInTransaction<R>(
    operations: Array<() => Promise<R>>
  ): Promise<R[]> {
    return await this.executeTransaction(async () => {
      const results: R[] = [];
      for (const operation of operations) {
        const result = await operation();
        results.push(result);
      }
      return results;
    });
  }

  // ========================== DATA MIGRATION HELPERS ==========================

  /**
   * Import data from SQLite format
   */
  async importFromSQLite(
    records: Record<string, any>[],
    options?: {
      batchSize?: number;
      transformRecord?: (record: Record<string, any>) => Record<string, any>;
      onProgress?: (processed: number, total: number) => void;
      skipErrors?: boolean;
    }
  ): Promise<ImportResult> {
    await this.init();

    const batchSize = options?.batchSize || 1000;
    const startTime = Date.now();
    let successCount = 0;
    let errorCount = 0;
    const errors: Array<{
      rowIndex: number;
      error: string;
      rowData: Record<string, any>;
    }> = [];

    // Transform records from SQLite to MongoDB format
    const transformedRecords = records
      .map((record, index) => {
        try {
          let transformed = this.dao.sqliteToMongoFormat(record);

          if (options?.transformRecord) {
            transformed = options.transformRecord(transformed);
          }

          return transformed;
        } catch (error) {
          if (!options?.skipErrors) {
            throw error;
          }

          errors.push({
            rowIndex: index,
            error: (error as Error).message,
            rowData: record,
          });
          errorCount++;
          return null;
        }
      })
      .filter((record) => record !== null);

    // Batch insert
    for (let i = 0; i < transformedRecords.length; i += batchSize) {
      try {
        const batch = transformedRecords.slice(i, i + batchSize);
        const result = await this.dao.bulkInsert(
          this.collectionName,
          batch,
          batchSize
        );
        successCount += result.rowsAffected;

        if (options?.onProgress) {
          options.onProgress(
            Math.min(i + batchSize, transformedRecords.length),
            records.length
          );
        }
      } catch (error) {
        const batchStart = i;
        const batchEnd = Math.min(i + batchSize, transformedRecords.length);

        for (let j = batchStart; j < batchEnd; j++) {
          errors.push({
            rowIndex: j,
            error: (error as Error).message,
            rowData: transformedRecords[j],
          });
          errorCount++;
        }

        if (!options?.skipErrors) {
          break;
        }
      }
    }

    return {
      totalRows: records.length,
      successRows: successCount,
      errorRows: errorCount,
      errors,
      executionTime: Date.now() - startTime,
    };
  }

  /**
   * Export data to SQLite format
   */
  async exportToSQLite(
    filter: Record<string, any> = {},
    options?: {
      limit?: number;
      sort?: Record<string, 1 | -1>;
      transformRecord?: (record: any) => Record<string, any>;
    }
  ): Promise<Record<string, any>[]> {
    await this.init();

    const queryOptions: MongoQueryOptions = {};

    if (options?.limit) {
      queryOptions.limit = options.limit;
    }

    if (options?.sort) {
      queryOptions.sort = options.sort;
    }

    const records = await this.dao.find(
      this.collectionName,
      filter,
      queryOptions
    );

    return records.map((record) => {
      let transformed = this.dao.mongoToSQLiteFormat(record);

      if (options?.transformRecord) {
        transformed = options.transformRecord(transformed);
      }

      return transformed;
    });
  }

  // ========================== UTILITY METHODS ==========================

  /**
   * Get collection statistics
   */
  async getCollectionStats(): Promise<{
    name: string;
    count: number;
    averageSize: number;
    totalSize: number;
    indexes: any[];
  }> {
    await this.init();

    const count = await this.dao.count(this.collectionName);
    const collectionInfo = await this.dao.getCollectionInfo(
      this.collectionName
    );

    return {
      name: this.collectionName,
      count,
      averageSize: 0, // MongoDB doesn't provide this directly
      totalSize: 0, // MongoDB doesn't provide this directly
      indexes: collectionInfo.indexes,
    };
  }

  /**
   * Create index on collection
   */
  async createIndex(
    keys: Record<string, 1 | -1 | "text" | "2dsphere">,
    options?: {
      name?: string;
      unique?: boolean;
      sparse?: boolean;
      background?: boolean;
      expireAfterSeconds?: number;
    }
  ): Promise<void> {
    await this.init();

    const collection = this.dao["getCollection"](this.collectionName);
    await collection.createIndex(keys, options);
  }

  /**
   * Drop index from collection
   */
  async dropIndex(indexName: string): Promise<void> {
    await this.init();

    const collection = this.dao["getCollection"](this.collectionName);
    await collection.dropIndex(indexName);
  }

  /**
   * Get distinct values for a field
   */
  async distinct(
    field: string,
    filter: Record<string, any> = {}
  ): Promise<any[]> {
    await this.init();

    const collection = this.dao["getCollection"](this.collectionName);
    return await collection.distinct(field, filter);
  }

  /**
   * Check if collection is empty
   */
  async isEmpty(): Promise<boolean> {
    const count = await this.count();
    return count === 0;
  }

  /**
   * Get first document
   */
  async getFirst(sort?: Record<string, 1 | -1>): Promise<T | null> {
    await this.init();

    const options: MongoQueryOptions = { limit: 1 };
    if (sort) {
      options.sort = sort;
    }

    const results = await this.dao.find(this.collectionName, {}, options);
    return (results[0] as T) || null;
  }

  /**
   * Get last document
   */
  async getLast(sort?: Record<string, 1 | -1>): Promise<T | null> {
    await this.init();

    const defaultSort = sort || { _id: -1 };
    const options: MongoQueryOptions = {
      limit: 1,
      sort: defaultSort,
    };

    const results = await this.dao.find(this.collectionName, {}, options);
    return (results[0] as T) || null;
  }

  /**
   * Validate document against schema rules
   */
  protected validateDocument(document: Partial<T>): {
    isValid: boolean;
    errors: string[];
  } {
    const errors: string[] = [];

    // Basic validation - can be overridden in subclasses
    if (!document || typeof document !== "object") {
      errors.push("Document must be an object");
    }

    return {
      isValid: errors.length === 0,
      errors,
    };
  }

  /**
   * Create document with validation
   */
  async createValidated(data: Partial<T>): Promise<T> {
    const validation = this.validateDocument(data);

    if (!validation.isValid) {
      throw new Error(`Validation failed: ${validation.errors.join(", ")}`);
    }

    return await this.create(data);
  }

  /**
   * Update document with validation
   */
  async updateValidated(
    id: string | ObjectId,
    update: Partial<T>
  ): Promise<boolean> {
    const validation = this.validateDocument(update);

    if (!validation.isValid) {
      throw new Error(`Validation failed: ${validation.errors.join(", ")}`);
    }

    return await this.updateById(id, update);
  }

  // ========================== CLEANUP METHODS ==========================

  /**
   * Clear all documents in collection
   */
  async clear(): Promise<number> {
    await this.init();
    return await this.deleteMany({});
  }

  /**
   * Drop the entire collection
   */
  async drop(): Promise<void> {
    await this.init();
    await this.dao.dropCollection(this.collectionName);
  }

  /**
   * Cleanup expired documents (requires expiration field)
   */
  async cleanupExpired(expirationField: string = "expiresAt"): Promise<number> {
    const filter = {
      [expirationField]: { $lt: new Date() },
    };

    return await this.deleteMany(filter);
  }
}
```

```ts
// src/core/database-manager.ts

import {
  DatabaseSchema,
  ImportOptions,
  ImportResult,
  ColumnMapping,
  MongoClientOptions,
} from "../types";
import { MongoDatabaseFactory } from "./database-factory";
import { MongoUniversalDAO } from "./universal-dao";

export type MongoDatabaseConnections = {
  [key: string]: MongoUniversalDAO;
};

export interface MongoRoleConfig {
  roleName: string;
  requiredDatabases: string[];
  optionalDatabases?: string[];
  priority?: number;
}

export type MongoRoleRegistry = {
  [roleName: string]: MongoRoleConfig;
};

export interface MongoDatabaseImportConfig {
  databaseKey: string;
  collectionName: string;
  data: Record<string, any>[];
  options?: Partial<ImportOptions>;
  columnMappings?: ColumnMapping[];
}

export interface MongoBulkImportResult {
  totalDatabases: number;
  successDatabases: number;
  results: Record<string, ImportResult>;
  errors: Record<string, Error>;
  executionTime: number;
}

export interface MongoSchemaManager {
  getSchema(key: string): DatabaseSchema | undefined;
  registerSchema(key: string, schema: DatabaseSchema): void;
  getAllSchemaKeys(): string[];
  hasSchema(key: string): boolean;
}

export class MongoDatabaseManager {
  private static maxConnections = 10;
  private static connections: MongoDatabaseConnections = {};
  private static isInitialized = false;
  private static roleRegistry: MongoRoleRegistry = {};
  private static currentRole: string | null = null;
  private static currentUserRoles: string[] = [];
  private static activeDatabases: Set<string> = new Set();
  private static isClosingConnections = false;

  // Connection configuration
  private static defaultConnectionString = "mongodb://localhost:27017";
  private static connectionOptions: MongoClientOptions = {};

  // Schema management
  private static schemaConfigurations: Record<string, DatabaseSchema> = {};
  private static schemaManager: MongoSchemaManager | null = null;

  // Event system for database reconnection
  private static eventListeners: Map<
    string,
    Array<(dao: MongoUniversalDAO) => void>
  > = new Map();

  // ========================== CONNECTION CONFIGURATION ==========================

  /**
   * Set default MongoDB connection string
   */
  public static setConnectionString(connectionString: string): void {
    this.defaultConnectionString = connectionString;
  }

  /**
   * Set MongoDB connection options
   */
  public static setConnectionOptions(options: MongoClientOptions): void {
    this.connectionOptions = { ...options };
  }

  /**
   * Get current connection string
   */
  public static getConnectionString(): string {
    return this.defaultConnectionString;
  }

  /**
   * Get the maximum number of allowed database connections
   */
  public static getMaxConnections(): number {
    return this.maxConnections;
  }

  /**
   * Set the maximum number of allowed database connections
   */
  public static setMaxConnections(maxConnections: number): void {
    if (maxConnections <= 0) {
      throw new Error("Maximum connections must be a positive number");
    }

    const currentConnectionCount = Object.keys(this.connections).length;
    if (currentConnectionCount > maxConnections) {
      throw new Error(
        `Cannot set maximum connections to ${maxConnections}. ` +
          `Current active connections (${currentConnectionCount}) exceed the new limit. ` +
          `Please close some connections first.`
      );
    }

    this.maxConnections = maxConnections;
  }

  // ========================== SCHEMA MANAGEMENT ==========================

  /**
   * Set a schema manager for dynamic schema handling
   */
  public static setSchemaManager(manager: MongoSchemaManager): void {
    this.schemaManager = manager;
  }

  /**
   * Register a schema configuration dynamically
   */
  public static registerSchema(key: string, schema: DatabaseSchema): void {
    this.schemaConfigurations[key] = schema;
  }

  /**
   * Register multiple schemas at once
   */
  public static registerSchemas(schemas: Record<string, DatabaseSchema>): void {
    Object.entries(schemas).forEach(([key, schema]) => {
      this.registerSchema(key, schema);
    });
  }

  /**
   * Get schema from internal store or external manager
   */
  private static getSchema(key: string): DatabaseSchema | undefined {
    // Try internal schemas first
    if (this.schemaConfigurations[key]) {
      return this.schemaConfigurations[key];
    }

    // Try external schema manager
    if (this.schemaManager) {
      const schema = this.schemaManager.getSchema(key);
      if (schema) {
        return schema;
      }
    }

    return undefined;
  }

  /**
   * Get all available schema keys
   */
  public static getAvailableSchemas(): string[] {
    const internalKeys = Object.keys(this.schemaConfigurations);
    const externalKeys = this.schemaManager?.getAllSchemaKeys() || [];
    return [...new Set([...internalKeys, ...externalKeys])];
  }

  // ========================== ROLE MANAGEMENT ==========================

  /**
   * Register a role configuration
   */
  public static registerRole(roleConfig: MongoRoleConfig): void {
    this.roleRegistry[roleConfig.roleName] = roleConfig;
  }

  /**
   * Register multiple roles
   */
  public static registerRoles(roleConfigs: MongoRoleConfig[]): void {
    roleConfigs.forEach((config) => this.registerRole(config));
  }

  /**
   * Get all registered roles
   */
  public static getRegisteredRoles(): MongoRoleRegistry {
    return { ...this.roleRegistry };
  }

  /**
   * Get databases for a specific role
   */
  public static getRoleDatabases(roleName: string): string[] {
    const roleConfig = this.roleRegistry[roleName];
    if (!roleConfig) {
      throw new Error(`Role '${roleName}' is not registered.`);
    }

    return [
      ...roleConfig.requiredDatabases,
      ...(roleConfig.optionalDatabases || []),
    ];
  }

  /**
   * Get databases for current user roles
   */
  public static getCurrentUserDatabases(): string[] {
    const allDatabases = new Set<string>();
    allDatabases.add("core"); // Core database is always included

    for (const roleName of this.currentUserRoles) {
      const roleConfig = this.roleRegistry[roleName];
      if (roleConfig) {
        roleConfig.requiredDatabases.forEach((db) => allDatabases.add(db));
        if (roleConfig.optionalDatabases) {
          roleConfig.optionalDatabases.forEach((db) => allDatabases.add(db));
        }
      }
    }

    return Array.from(allDatabases);
  }

  // ========================== CONNECTION MANAGEMENT ==========================

  /**
   * Initialize core database connection
   */
  public static async initializeCoreConnection(): Promise<void> {
    if (this.connections["core"]) {
      return;
    }

    try {
      const coreSchema = this.getSchema("core");
      if (!coreSchema) {
        throw new Error("Core database schema not found.");
      }

      const dao = await MongoDatabaseFactory.createFromSchema(
        coreSchema,
        this.defaultConnectionString,
        this.connectionOptions
      );

      this.connections["core"] = dao;
    } catch (error) {
      throw new Error(
        `Error initializing core database: ${(error as Error).message}`
      );
    }
  }

  /**
   * Set current user roles and initialize connections
   */
  public static async setCurrentUserRoles(
    userRoles: string[],
    primaryRole?: string
  ): Promise<void> {
    // Validate roles exist
    for (const roleName of userRoles) {
      if (!this.roleRegistry[roleName]) {
        throw new Error(
          `Role '${roleName}' is not registered. Please register it first.`
        );
      }
    }

    const previousRoles = [...this.currentUserRoles];
    this.currentUserRoles = userRoles;
    this.currentRole = primaryRole || userRoles[0] || null;

    try {
      await this.initializeUserRoleConnections();
      await this.cleanupUnusedConnections(previousRoles);
    } catch (error) {
      throw error;
    }
  }

  /**
   * Get current user roles
   */
  public static getCurrentUserRoles(): string[] {
    return [...this.currentUserRoles];
  }

  /**
   * Get current primary role
   */
  public static getCurrentRole(): string | null {
    return this.currentRole;
  }

  /**
   * Initialize connections for current user roles
   */
  private static async initializeUserRoleConnections(): Promise<void> {
    const requiredDatabases = this.getCurrentUserDatabases();
    const failedInitializations: { key: string; error: Error }[] = [];

    const initPromises = requiredDatabases.map(async (dbKey) => {
      if (this.connections[dbKey]) {
        return; // Already connected
      }

      try {
        const schema = this.getSchema(dbKey);
        if (!schema) {
          throw new Error(
            `Database key '${dbKey}' not found in schema configurations.`
          );
        }

        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );

        this.connections[dbKey] = dao;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));

        // Check if database is required for any role
        const isRequired = this.currentUserRoles.some((roleName) => {
          const roleConfig = this.roleRegistry[roleName];
          return roleConfig && roleConfig.requiredDatabases.includes(dbKey);
        });

        if (isRequired) {
          failedInitializations.push({ key: dbKey, error: err });
        }
        // Optional databases that fail are ignored
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      throw new Error(
        `Failed to initialize required databases for user roles:\n${errorSummary}`
      );
    }
  }

  /**
   * Cleanup unused connections
   */
  private static async cleanupUnusedConnections(
    previousRoles: string[]
  ): Promise<void> {
    const previousDatabases = new Set<string>();
    previousDatabases.add("core");

    for (const roleName of previousRoles) {
      const roleConfig = this.roleRegistry[roleName];
      if (roleConfig) {
        roleConfig.requiredDatabases.forEach((db) => previousDatabases.add(db));
        if (roleConfig.optionalDatabases) {
          roleConfig.optionalDatabases.forEach((db) =>
            previousDatabases.add(db)
          );
        }
      }
    }

    const currentDatabases = new Set(this.getCurrentUserDatabases());
    const databasesToClose = Array.from(previousDatabases).filter(
      (db) => !currentDatabases.has(db)
    );

    if (databasesToClose.length > 0) {
      for (const dbKey of databasesToClose) {
        if (this.connections[dbKey]) {
          try {
            await this.connections[dbKey].disconnect();
            delete this.connections[dbKey];
          } catch (error) {
            // Log error but continue cleanup
          }
        }
      }
    }
  }

  /**
   * Check if current user has access to database
   */
  public static hasAccessToDatabase(dbKey: string): boolean {
    return this.getSchema(dbKey) !== undefined;
  }

  /**
   * Get database connection
   */
  public static get(key: string): MongoUniversalDAO {
    if (!this.hasAccessToDatabase(key)) {
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    const dao = this.connections[key];
    if (!dao) {
      throw new Error(
        `Database '${key}' is not connected. Please ensure it's initialized.`
      );
    }

    return dao;
  }

  // ========================== EVENT MANAGEMENT ==========================

  /**
   * Register event listener for database reconnection
   */
  public static onDatabaseReconnect(
    schemaName: string,
    callback: (dao: MongoUniversalDAO) => void
  ): void {
    if (!this.eventListeners.has(schemaName)) {
      this.eventListeners.set(schemaName, []);
    }
    this.eventListeners.get(schemaName)!.push(callback);
  }

  /**
   * Remove event listener for database reconnection
   */
  public static offDatabaseReconnect(
    schemaName: string,
    callback: (dao: MongoUniversalDAO) => void
  ): void {
    const listeners = this.eventListeners.get(schemaName);
    if (listeners) {
      const index = listeners.indexOf(callback);
      if (index > -1) {
        listeners.splice(index, 1);
      }
    }
  }

  /**
   * Notify listeners of database reconnection
   */
  private static notifyDatabaseReconnect(
    schemaName: string,
    dao: MongoUniversalDAO
  ): void {
    const listeners = this.eventListeners.get(schemaName);
    if (listeners) {
      listeners.forEach((callback) => {
        try {
          callback(dao);
        } catch (error) {
          // Handle callback errors gracefully
        }
      });
    }
  }

  // ========================== DATABASE OPERATIONS ==========================

  /**
   * Close all connections
   */
  private static async closeAllConnections(): Promise<void> {
    if (this.isClosingConnections) {
      return;
    }

    this.isClosingConnections = true;
    try {
      // Save active databases
      const currentActiveDb = Object.keys(this.connections);
      currentActiveDb.forEach((dbKey) => this.activeDatabases.add(dbKey));

      const closePromises = Object.entries(this.connections).map(
        async ([dbKey, dao]) => {
          try {
            await dao.disconnect();
          } catch (error) {
            // Log error but continue closing
          }
        }
      );

      await Promise.all(closePromises);
      this.connections = {};
    } finally {
      this.isClosingConnections = false;
    }
  }

  /**
   * Reopen connections
   */
  public static async reopenConnections(): Promise<void> {
    try {
      await this.initializeCoreConnection();

      if (this.currentUserRoles.length > 0) {
        await this.initializeUserRoleConnections();
      }

      // Reinitialize previously active databases
      const activeDbArray = Array.from(this.activeDatabases);

      if (activeDbArray.length > 0) {
        for (const dbKey of activeDbArray) {
          if (!this.connections[dbKey]) {
            const schema = this.getSchema(dbKey);
            if (schema) {
              try {
                const dao = await MongoDatabaseFactory.createFromSchema(
                  schema,
                  this.defaultConnectionString,
                  this.connectionOptions
                );
                this.connections[dbKey] = dao;
                this.notifyDatabaseReconnect(dbKey, dao);
              } catch (error) {
                // Log error but continue
              }
            }
          } else if (this.connections[dbKey]) {
            // Database exists, notify services
            this.notifyDatabaseReconnect(dbKey, this.connections[dbKey]);
          }
        }
      }
    } catch (error) {
      throw error;
    }
  }

  /**
   * Ensure database connection exists and is active
   */
  public static async ensureDatabaseConnection(
    key: string
  ): Promise<MongoUniversalDAO> {
    this.activeDatabases.add(key);

    if (!this.hasAccessToDatabase(key)) {
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    if (this.connections[key]) {
      try {
        const isConnected = this.connections[key].isConnectionOpen();
        if (isConnected) {
          return this.connections[key];
        } else {
          // Clean up inactive connection
          try {
            await this.connections[key].disconnect().catch(() => {});
          } catch (error) {
            // Ignore cleanup errors
          }
          delete this.connections[key];
        }
      } catch (error) {
        delete this.connections[key];
      }
    }

    // Create new connection
    return await this.getLazyLoading(key);
  }

  /**
   * Get all connections
   */
  public static getConnections(): MongoDatabaseConnections {
    return { ...this.connections };
  }

  /**
   * Open all existing databases
   */
  public static async openAllExisting(
    databaseKeys: string[]
  ): Promise<boolean> {
    const failedOpens: { key: string; error: Error }[] = [];

    for (const key of databaseKeys) {
      try {
        const schema = this.getSchema(key);
        if (!schema) {
          throw new Error(`Invalid database key: ${key}. Schema not found.`);
        }

        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );

        this.connections[key] = dao;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        failedOpens.push({ key, error: err });
      }
    }

    if (failedOpens.length > 0) {
      const errorSummary = failedOpens
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      throw new Error(`Failed to open one or more databases:\n${errorSummary}`);
    }

    this.isInitialized = true;
    return true;
  }

  /**
   * Initialize databases lazily
   */
  public static async initLazySchema(databaseKeys: string[]): Promise<boolean> {
    const invalidKeys = databaseKeys.filter((key) => !this.getSchema(key));
    if (invalidKeys.length > 0) {
      throw new Error(
        `Invalid database keys: ${invalidKeys.join(", ")}. Schemas not found.`
      );
    }

    const newConnectionsCount = databaseKeys.filter(
      (key) => !this.connections[key]
    ).length;
    const currentConnectionsCount = Object.keys(this.connections).length;

    if (currentConnectionsCount + newConnectionsCount > this.maxConnections) {
      throw new Error(
        `Cannot initialize ${newConnectionsCount} new connections. Would exceed maximum of ${this.maxConnections} connections. Current: ${currentConnectionsCount}`
      );
    }

    const failedInitializations: { key: string; error: Error }[] = [];
    const initPromises = databaseKeys.map(async (key) => {
      if (this.connections[key]) {
        return; // Already initialized
      }

      try {
        const schema = this.getSchema(key)!;
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );
        this.connections[key] = dao;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        failedInitializations.push({ key, error: err });
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      throw new Error(
        `Failed to initialize one or more databases:\n${errorSummary}`
      );
    }

    if (Object.keys(this.connections).length > 0) {
      this.isInitialized = true;
    }

    return true;
  }

  /**
   * Initialize all available databases
   */
  public static async initializeAll(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    const availableSchemas = this.getAvailableSchemas();
    const failedInitializations: { key: string; error: Error }[] = [];

    const initPromises = availableSchemas.map(async (key) => {
      try {
        const schema = this.getSchema(key)!;
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );
        this.connections[key] = dao;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        failedInitializations.push({ key, error: err });
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      this.isInitialized = false;
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      throw new Error(
        `Failed to initialize one or more databases:\n${errorSummary}`
      );
    }

    this.isInitialized = true;
  }

  /**
   * Get database with lazy loading
   */
  public static async getLazyLoading(key: string): Promise<MongoUniversalDAO> {
    this.activeDatabases.add(key);

    if (!this.hasAccessToDatabase(key)) {
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    if (!this.connections[key]) {
      const schema = this.getSchema(key);
      if (!schema) {
        throw new Error(`Invalid database key: ${key}. Schema not found.`);
      }

      if (Object.keys(this.connections).length >= this.maxConnections) {
        throw new Error("Maximum number of database connections reached");
      }

      const dao = await MongoDatabaseFactory.createFromSchema(
        schema,
        this.defaultConnectionString,
        this.connectionOptions
      );
      this.connections[key] = dao;
    }

    this.isInitialized = true;
    return this.connections[key];
  }

  // ========================== TRANSACTION OPERATIONS ==========================

  /**
   * Execute cross-schema transaction
   */
  public static async executeCrossSchemaTransaction(
    schemas: string[],
    callback: (daos: Record<string, MongoUniversalDAO>) => Promise<void>
  ): Promise<void> {
    for (const key of schemas) {
      if (!this.hasAccessToDatabase(key)) {
        throw new Error(`Access denied: Database '${key}' is not accessible.`);
      }
    }

    const daos = schemas.reduce((acc, key) => {
      acc[key] = this.get(key);
      return acc;
    }, {} as Record<string, MongoUniversalDAO>);

    try {
      await Promise.all(
        Object.values(daos).map((dao) => dao.beginTransaction())
      );

      await callback(daos);

      await Promise.all(
        Object.values(daos).map((dao) => dao.commitTransaction())
      );
    } catch (error) {
      await Promise.all(
        Object.values(daos).map((dao) => dao.rollbackTransaction())
      );
      throw error;
    }
  }

  // ========================== IMPORT OPERATIONS ==========================

  /**
   * Import data to collection
   */
  public static async importDataToCollection(
    databaseKey: string,
    collectionName: string,
    data: Record<string, any>[],
    options: Partial<ImportOptions> = {}
  ): Promise<ImportResult> {
    if (!this.hasAccessToDatabase(databaseKey)) {
      throw new Error(
        `Access denied: Database '${databaseKey}' is not accessible.`
      );
    }

    const dao = this.get(databaseKey);
    try {
      const result = await dao.bulkInsert(collectionName, data);
      return {
        totalRows: data.length,
        successRows: result.rowsAffected,
        errorRows: data.length - result.rowsAffected,
        errors: [],
        executionTime: 0,
      };
    } catch (error) {
      throw error;
    }
  }

  /**
   * Bulk import data
   */
  public static async bulkImport(
    importConfigs: MongoDatabaseImportConfig[]
  ): Promise<MongoBulkImportResult> {
    const startTime = Date.now();
    const result: MongoBulkImportResult = {
      totalDatabases: importConfigs.length,
      successDatabases: 0,
      results: {},
      errors: {},
      executionTime: 0,
    };

    for (const config of importConfigs) {
      const configKey = `${config.databaseKey}.${config.collectionName}`;

      try {
        if (!this.hasAccessToDatabase(config.databaseKey)) {
          throw new Error(
            `Access denied: Database '${config.databaseKey}' is not accessible.`
          );
        }

        const importResult = await this.importDataToCollection(
          config.databaseKey,
          config.collectionName,
          config.data,
          config.options
        );

        result.results[configKey] = importResult;
        result.successDatabases++;
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        result.errors[configKey] = err;
      }
    }

    result.executionTime = Date.now() - startTime;
    return result;
  }

  // ========================== UTILITY METHODS ==========================

  /**
   * Get connection count
   */
  public static getConnectionCount(): number {
    return Object.keys(this.connections).length;
  }

  /**
   * List all active connections
   */
  public static listConnections(): string[] {
    return Object.keys(this.connections);
  }

  /**
   * Close specific connection
   */
  public static async closeConnection(dbKey: string): Promise<void> {
    const dao = this.connections[dbKey];
    if (dao) {
      try {
        await dao.disconnect();
        delete this.connections[dbKey];
      } catch (error) {
        throw error;
      }
    }
  }

  /**
   * Close all connections and reset state
   */
  public static async closeAll(): Promise<void> {
    await this.closeAllConnections();

    this.currentUserRoles = [];
    this.currentRole = null;
    this.isInitialized = false;
    this.activeDatabases.clear();
    this.eventListeners.clear();
    this.isClosingConnections = false;
  }

  /**
   * Logout user - close role-specific connections
   */
  public static async logout(): Promise<void> {
    const connectionsToClose = Object.keys(this.connections).filter(
      (key) => key !== "core"
    );

    for (const dbKey of connectionsToClose) {
      try {
        await this.connections[dbKey].disconnect();
        delete this.connections[dbKey];
      } catch (error) {
        // Log error but continue cleanup
      }
    }

    this.currentUserRoles = [];
    this.currentRole = null;
  }
}
```

```ts
// src/core/database-factory.ts

import { MongoClientOptions } from "mongodb";
import { DatabaseSchema } from "../types";
import { MongoUniversalDAO } from "./universal-dao";
import { BaseMongoAdapter } from "../adapters/base-adapter";

// ========================== DATABASE FACTORY FOR MONGODB ==========================
export class MongoDatabaseFactory {
  private static adapters: BaseMongoAdapter[] = [];

  static registerAdapter(adapter: BaseMongoAdapter): void {
    this.adapters.push(adapter);
  }

  private static getBestAdapter(): BaseMongoAdapter {
    const adapter = this.adapters.find((a) => a.isSupported());
    if (!adapter) {
      // Use default adapter if none registered
      return new BaseMongoAdapter();
    }
    return adapter;
  }

  static createDAO(
    connectionString: string,
    databaseName: string,
    options?: MongoClientOptions
  ): MongoUniversalDAO {
    const adapter = this.getBestAdapter();
    return new MongoUniversalDAO(
      adapter,
      connectionString,
      databaseName,
      options
    );
  }

  /**
   * Create DAO from SQLite-compatible DatabaseSchema
   */
  static async createFromSchema(
    schema: DatabaseSchema,
    connectionString: string,
    options?: MongoClientOptions
  ): Promise<MongoUniversalDAO> {
    const dao = this.createDAO(connectionString, schema.database_name, options);
    await dao.connect();

    // Use the new method that accepts DatabaseSchema
    await dao.initializeFromDatabaseSchema(schema);
    return dao;
  }

  /**
   * Create DAO with custom database name (different from schema)
   */
  static async createFromSchemaWithCustomDb(
    schema: DatabaseSchema,
    connectionString: string,
    customDatabaseName: string,
    options?: MongoClientOptions
  ): Promise<MongoUniversalDAO> {
    const dao = this.createDAO(connectionString, customDatabaseName, options);
    await dao.connect();

    // Create a modified schema with custom database name
    const modifiedSchema: DatabaseSchema = {
      ...schema,
      database_name: customDatabaseName,
    };

    await dao.initializeFromDatabaseSchema(modifiedSchema);
    return dao;
  }

  /**
   * Create multiple DAOs from the same schema for different databases
   */
  static async createMultipleFromSchema(
    schema: DatabaseSchema,
    connectionString: string,
    databaseNames: string[],
    options?: MongoClientOptions
  ): Promise<Record<string, MongoUniversalDAO>> {
    const daos: Record<string, MongoUniversalDAO> = {};

    for (const dbName of databaseNames) {
      const dao = await this.createFromSchemaWithCustomDb(
        schema,
        connectionString,
        dbName,
        options
      );
      daos[dbName] = dao;
    }

    return daos;
  }

  /**
   * Validate schema before creating DAO
   */
  static validateSchema(schema: DatabaseSchema): {
    isValid: boolean;
    errors: string[];
  } {
    const errors: string[] = [];

    // Check required fields
    if (!schema.version) {
      errors.push("Schema version is required");
    }

    if (!schema.database_name) {
      errors.push("Database name is required");
    }

    if (!schema.schemas || Object.keys(schema.schemas).length === 0) {
      errors.push("At least one collection schema is required");
    }

    // Validate each collection schema
    for (const [collectionName, collectionSchema] of Object.entries(
      schema.schemas || {}
    )) {
      if (!collectionSchema.cols || collectionSchema.cols.length === 0) {
        errors.push(
          `Collection '${collectionName}' must have at least one column`
        );
      }

      // Check for duplicate column names
      const columnNames = new Set();
      for (const col of collectionSchema.cols) {
        if (!col.name) {
          errors.push(
            `Collection '${collectionName}' has a column without name`
          );
          continue;
        }

        if (columnNames.has(col.name)) {
          errors.push(
            `Collection '${collectionName}' has duplicate column name: ${col.name}`
          );
        }
        columnNames.add(col.name);

        if (!col.type) {
          errors.push(
            `Column '${col.name}' in collection '${collectionName}' must have a type`
          );
        }
      }

      // Validate indexes
      if (collectionSchema.indexes) {
        for (const index of collectionSchema.indexes) {
          if (!index.name) {
            errors.push(
              `Collection '${collectionName}' has an index without name`
            );
          }

          if (!index.columns || index.columns.length === 0) {
            errors.push(
              `Index '${index.name}' in collection '${collectionName}' must have at least one column`
            );
          }

          // Check if indexed columns exist
          const availableColumns = collectionSchema.cols.map((col) => col.name);
          for (const indexColumn of index.columns || []) {
            if (!availableColumns.includes(indexColumn)) {
              errors.push(
                `Index '${index.name}' in collection '${collectionName}' references non-existent column: ${indexColumn}`
              );
            }
          }
        }
      }
    }

    return {
      isValid: errors.length === 0,
      errors,
    };
  }

  /**
   * Create DAO with schema validation
   */
  static async createFromValidatedSchema(
    schema: DatabaseSchema,
    connectionString: string,
    options?: MongoClientOptions & { throwOnValidationError?: boolean }
  ): Promise<MongoUniversalDAO> {
    const validation = this.validateSchema(schema);

    if (!validation.isValid) {
      const errorMessage = `Schema validation failed:\n${validation.errors.join(
        "\n"
      )}`;

      if (options?.throwOnValidationError !== false) {
        throw new Error(errorMessage);
      } else {
        console.warn(errorMessage);
      }
    }

    return await this.createFromSchema(schema, connectionString, options);
  }

  /**
   * Get schema information for debugging
   */
  static getSchemaInfo(schema: DatabaseSchema): {
    version: string;
    databaseName: string;
    collectionCount: number;
    totalColumns: number;
    totalIndexes: number;
    collections: Array<{
      name: string;
      columnCount: number;
      indexCount: number;
      hasValidation: boolean;
    }>;
  } {
    const collections = Object.entries(schema.schemas || {}).map(
      ([name, config]) => ({
        name,
        columnCount: config.cols?.length || 0,
        indexCount: config.indexes?.length || 0,
        hasValidation:
          config.cols?.some((col) => col.enum || !col.nullable) || false,
      })
    );

    return {
      version: schema.version,
      databaseName: schema.database_name,
      collectionCount: collections.length,
      totalColumns: collections.reduce((sum, col) => sum + col.columnCount, 0),
      totalIndexes: collections.reduce((sum, col) => sum + col.indexCount, 0),
      collections,
    };
  }

  /**
   * Create DAO with connection testing
   */
  static async createWithConnectionTest(
    schema: DatabaseSchema,
    connectionString: string,
    options?: MongoClientOptions & {
      testTimeout?: number;
      retryAttempts?: number;
      retryDelay?: number;
    }
  ): Promise<MongoUniversalDAO> {
    const testTimeout = options?.testTimeout || 5000;
    const retryAttempts = options?.retryAttempts || 3;
    const retryDelay = options?.retryDelay || 1000;

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= retryAttempts; attempt++) {
      try {
        const dao = this.createDAO(
          connectionString,
          schema.database_name,
          options
        );

        // Test connection with timeout
        const connectPromise = dao.connect();
        const timeoutPromise = new Promise<never>((_, reject) => {
          setTimeout(
            () => reject(new Error("Connection timeout")),
            testTimeout
          );
        });

        await Promise.race([connectPromise, timeoutPromise]);

        // Test database operations
        await dao.getDatabaseInfo();

        // Initialize schema
        await dao.initializeFromDatabaseSchema(schema);

        return dao;
      } catch (error) {
        lastError = error as Error;
        console.warn(
          `Connection attempt ${attempt}/${retryAttempts} failed:`,
          error
        );

        if (attempt < retryAttempts) {
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }

    throw new Error(
      `Failed to connect after ${retryAttempts} attempts. Last error: ${lastError?.message}`
    );
  }

  /**
   * Cleanup all registered adapters
   */
  static async cleanup(): Promise<void> {
    for (const adapter of this.adapters) {
      if (typeof adapter.disconnectAll === "function") {
        await adapter.disconnectAll();
      }
    }
    this.adapters = [];
  }

  /**
   * Get registered adapter information
   */
  static getAdapterInfo(): Array<{
    name: string;
    version?: string;
    isSupported: boolean;
  }> {
    return this.adapters.map((adapter) => ({
      name: adapter.constructor.name,
      version: (adapter as any).version,
      isSupported: adapter.isSupported(),
    }));
  }
}
```

```ts
// src/core/universal-dao.ts
import {
  ClientSession,
  MongoClientOptions,
  Collection,
  ObjectId,
} from "mongodb";
import {
  MongoConnection,
  MongoAdapter,
  DatabaseSchema,
  MongoDatabaseSchema,
  MongoIndexDefinition,
  MongoResult,
  MongoQueryOptions,
  ColumnDefinition,
  IndexDefinition,
  MONGODB_TYPE_MAPPING,
  ValidationRule,
  SchemaValidation,
} from "../types";

// ========================== MONGODB DAO ==========================
export class MongoUniversalDAO {
  private connection: MongoConnection | null = null;
  private isConnected: boolean = false;
  private currentSession: ClientSession | null = null;
  private inTransaction: boolean = false;
  private adapter: MongoAdapter;
  private connectionString: string;
  private databaseName: string;

  constructor(
    adapter: MongoAdapter,
    connectionString: string,
    databaseName: string,
    private options?: MongoClientOptions
  ) {
    this.adapter = adapter;
    this.connectionString = connectionString;
    this.databaseName = databaseName;
  }

  // ========================== CONNECTION MANAGEMENT ==========================
  async connect(): Promise<void> {
    if (this.isConnected && this.connection) {
      return;
    }

    this.connection = await this.adapter.connect(
      this.connectionString,
      this.databaseName,
      this.options
    );
    this.isConnected = true;
  }

  async disconnect(): Promise<void> {
    if (this.currentSession) {
      await this.currentSession.endSession();
      this.currentSession = null;
    }

    if (this.connection) {
      await this.adapter.disconnect(this.connectionString, this.databaseName);
      this.connection = null;
      this.isConnected = false;
    }
  }

  isConnectionOpen(): boolean {
    return this.isConnected && !!this.connection;
  }

  private ensureConnected(): void {
    if (!this.isConnectionOpen()) {
      throw new Error("Database connection is not open. Call connect() first.");
    }
  }

  private getCollection(collectionName: string): Collection {
    this.ensureConnected();
    return this.connection!.db.collection(collectionName);
  }

  // ========================== TRANSACTION MANAGEMENT ==========================
  async beginTransaction(): Promise<void> {
    if (this.inTransaction) {
      throw new Error("Transaction already in progress");
    }

    this.ensureConnected();
    this.currentSession = this.connection!.client.startSession();
    this.currentSession.startTransaction();
    this.inTransaction = true;
  }

  async commitTransaction(): Promise<void> {
    if (!this.inTransaction || !this.currentSession) {
      throw new Error("No transaction in progress");
    }

    await this.currentSession.commitTransaction();
    await this.currentSession.endSession();
    this.currentSession = null;
    this.inTransaction = false;
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction || !this.currentSession) {
      throw new Error("No transaction in progress");
    }

    await this.currentSession.abortTransaction();
    await this.currentSession.endSession();
    this.currentSession = null;
    this.inTransaction = false;
  }

  // ========================== SCHEMA MANAGEMENT ==========================

  /**
   * Initialize database from SQLite-compatible DatabaseSchema
   */
  async initializeFromDatabaseSchema(schema: DatabaseSchema): Promise<void> {
    this.ensureConnected();

    try {
      // Convert SQLite-style schema to MongoDB schema
      const mongoSchema = this.convertToMongoSchema(schema);

      // Initialize with converted schema
      await this.initializeFromSchema(mongoSchema);
    } catch (error) {
      throw new Error(
        `Database schema initialization failed: ${(error as Error).message}`
      );
    }
  }

  /**
   * Convert SQLite DatabaseSchema to MongoDB MongoDatabaseSchema
   */
  private convertToMongoSchema(schema: DatabaseSchema): MongoDatabaseSchema {
    const mongoSchema: MongoDatabaseSchema = {
      version: schema.version,
      database_name: schema.database_name,
      description: schema.description,
      collections: {},
    };

    // Convert each table to a collection
    for (const [tableName, tableConfig] of Object.entries(schema.schemas)) {
      const collection = {
        name: tableName,
        schema: this.buildMongoSchema(tableConfig.cols),
        indexes: this.convertIndexes(tableConfig.indexes || []),
        validation: this.buildValidationSchema(tableConfig.cols),
      };

      mongoSchema.collections[tableName] = collection;
    }

    return mongoSchema;
  }

  /**
   * Build MongoDB schema from column definitions
   */
  private buildMongoSchema(columns: ColumnDefinition[]): Record<string, any> {
    const schema: Record<string, any> = {};

    for (const col of columns) {
      const fieldSchema: Record<string, any> = {};

      // Map SQLite type to MongoDB type
      const mongoType = this.mapTypeToMongo(col.type);
      fieldSchema.type = mongoType;

      // Handle special cases
      if (col.enum) {
        fieldSchema.enum = col.enum;
      }

      if (col.default !== undefined) {
        fieldSchema.default = col.default;
      }

      if (col.description) {
        fieldSchema.description = col.description;
      }

      // Handle MongoDB-specific field naming
      const fieldName = col.name === "id" && col.primary_key ? "_id" : col.name;
      schema[fieldName] = fieldSchema;
    }

    return schema;
  }

  /**
   * Map SQLite types to MongoDB types
   */
  private mapTypeToMongo(sqliteType: string): string {
    const mapping: any = MONGODB_TYPE_MAPPING.mongodb;
    return mapping[sqliteType.toLowerCase()] || "Mixed";
  }

  /**
   * Convert SQLite indexes to MongoDB indexes
   */
  private convertIndexes(indexes: IndexDefinition[]): MongoIndexDefinition[] {
    return indexes.map((index) => {
      const keys: Record<string, 1 | -1 | "text" | "2dsphere"> = {};

      // Convert column array to keys object
      for (const column of index.columns) {
        // Default to ascending index
        keys[column] = 1;
      }

      const mongoIndex: MongoIndexDefinition = {
        name: index.name,
        keys,
        options: {
          unique: index.unique || false,
          background: true,
        },
      };

      return mongoIndex;
    });
  }

  /**
   * Build validation schema from column definitions
   */
  private buildValidationSchema(
    columns: ColumnDefinition[]
  ): Record<string, any> {
    const validationRules: Record<string, any> = {};
    const properties: Record<string, any> = {};
    const required: string[] = [];

    for (const col of columns) {
      const fieldName = col.name === "id" && col.primary_key ? "_id" : col.name;
      const fieldValidation: Record<string, any> = {};

      // Type validation
      const mongoType = this.mapTypeToMongo(col.type);
      switch (mongoType) {
        case "String":
          fieldValidation.bsonType = "string";
          if (col.length) {
            fieldValidation.maxLength = col.length;
          }
          break;
        case "Number":
          fieldValidation.bsonType = "number";
          if (col.precision) {
            // Handle precision constraints if needed
          }
          break;
        case "Boolean":
          fieldValidation.bsonType = "bool";
          break;
        case "Date":
          fieldValidation.bsonType = "date";
          break;
        case "Array":
          fieldValidation.bsonType = "array";
          break;
        case "Object":
          fieldValidation.bsonType = "object";
          break;
        case "ObjectId":
          fieldValidation.bsonType = "objectId";
          break;
        default:
          // Mixed type - no specific validation
          break;
      }

      // Enum validation
      if (col.enum) {
        fieldValidation.enum = col.enum;
      }

      // Required validation
      if (!col.nullable && col.name !== "id") {
        required.push(fieldName);
      }

      properties[fieldName] = fieldValidation;
    }

    if (Object.keys(properties).length > 0) {
      validationRules.$jsonSchema = {
        bsonType: "object",
        properties,
        required,
      };
    }

    return validationRules;
  }

  /**
   * Original method for MongoDB-specific schema initialization
   */
  async initializeFromSchema(schema: MongoDatabaseSchema): Promise<void> {
    this.ensureConnected();

    try {
      // Create collections and indexes
      for (const [collectionName, collectionConfig] of Object.entries(
        schema.collections
      )) {
        const collection = this.getCollection(collectionName);

        // Ensure collection exists
        await collection.findOne({}, { limit: 1 });

        // Create indexes if specified
        if (collectionConfig.indexes?.length) {
          await this.createIndexesForCollection(
            collectionName,
            collectionConfig.indexes
          );
        }

        // Apply validation rules if specified
        if (collectionConfig.validation) {
          await this.connection!.db.command({
            collMod: collectionName,
            validator: collectionConfig.validation,
            validationLevel: "moderate",
            validationAction: "warn",
          });
        }
      }

      // Store schema version
      await this.setSchemaVersion(schema.version);
    } catch (error) {
      throw new Error(
        `Schema initialization failed: ${(error as Error).message}`
      );
    }
  }

  private async createIndexesForCollection(
    collectionName: string,
    indexes: MongoIndexDefinition[]
  ): Promise<void> {
    const collection = this.getCollection(collectionName);

    for (const index of indexes) {
      try {
        await collection.createIndex(index.keys, {
          name: index.name,
          ...index.options,
        });
      } catch (error) {
        // Index might already exist, continue with others
        console.warn(`Warning creating index ${index.name}:`, error);
      }
    }
  }

  async getSchemaVersion(): Promise<string> {
    try {
      const collection = this.getCollection("_schema_info");
      const result = await collection.findOne({}, { sort: { applied_at: -1 } });
      return result?.version || "0";
    } catch {
      return "0";
    }
  }

  async setSchemaVersion(version: string): Promise<void> {
    const collection = this.getCollection("_schema_info");
    await collection.insertOne({
      version,
      applied_at: new Date(),
    });
  }

  // ========================== HELPER METHODS FOR SCHEMA MIGRATION ==========================

  /**
   * Transform document before insertion based on column definitions
   */
  private transformDocument(
    document: Record<string, any>,
    columns: ColumnDefinition[]
  ): Record<string, any> {
    const transformed: Record<string, any> = {};

    for (const [key, value] of Object.entries(document)) {
      const column = columns.find((col) => col.name === key);
      if (!column) {
        // Pass through unknown columns
        transformed[key] = value;
        continue;
      }

      const fieldName = key === "id" && column.primary_key ? "_id" : key;

      // Transform based on type
      if (value !== null && value !== undefined) {
        switch (this.mapTypeToMongo(column.type)) {
          case "ObjectId":
            transformed[fieldName] =
              typeof value === "string" ? new ObjectId(value) : value;
            break;
          case "Date":
            transformed[fieldName] =
              value instanceof Date ? value : new Date(value);
            break;
          case "Number":
            transformed[fieldName] =
              typeof value === "number" ? value : Number(value);
            break;
          case "Boolean":
            transformed[fieldName] =
              typeof value === "boolean" ? value : Boolean(value);
            break;
          case "Object":
          case "Array":
            transformed[fieldName] =
              typeof value === "string" ? JSON.parse(value) : value;
            break;
          default:
            transformed[fieldName] = value;
        }
      } else {
        transformed[fieldName] = value;
      }
    }

    return transformed;
  }

  /**
   * Get collection schema information
   */
  async getCollectionSchema(collectionName: string): Promise<any> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const collectionInfo: any = await this.connection!.db.listCollections({
        name: collectionName,
      }).next();

      return {
        name: collectionName,
        options: collectionInfo?.options || {},
        validator: collectionInfo?.options?.validator || null,
        indexes: await collection.indexes(),
      };
    } catch (error) {
      throw new Error(
        `Get collection schema failed: ${(error as Error).message}`
      );
    }
  }

  // ========================== CRUD OPERATIONS ==========================
  async insert(
    collectionName: string,
    document: Record<string, any>
  ): Promise<MongoResult> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.insertOne(document, {
        session: this.currentSession || undefined,
      });

      return {
        rows: [{ _id: result.insertedId, ...document }],
        rowsAffected: result.acknowledged ? 1 : 0,
        lastInsertId: result.insertedId,
      };
    } catch (error) {
      throw new Error(`Insert failed: ${(error as Error).message}`);
    }
  }

  async insertMany(
    collectionName: string,
    documents: Record<string, any>[]
  ): Promise<MongoResult> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.insertMany(documents, {
        session: this.currentSession || undefined,
      });

      return {
        rows: documents.map((doc, index) => ({
          _id: result.insertedIds[index],
          ...doc,
        })),
        rowsAffected: result.insertedCount,
        insertedIds: Object.values(result.insertedIds) as ObjectId[],
      };
    } catch (error) {
      throw new Error(`Insert many failed: ${(error as Error).message}`);
    }
  }

  async update(
    collectionName: string,
    filter: Record<string, any>,
    update: Record<string, any>,
    options?: { upsert?: boolean; multi?: boolean }
  ): Promise<MongoResult> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);

      // Prepare update document
      const updateDoc = this.prepareUpdateDocument(update);

      let result;
      if (options?.multi) {
        result = await collection.updateMany(filter, updateDoc, {
          upsert: options?.upsert,
          session: this.currentSession || undefined,
        });
      } else {
        result = await collection.updateOne(filter, updateDoc, {
          upsert: options?.upsert,
          session: this.currentSession || undefined,
        });
      }

      return {
        rows: [],
        rowsAffected: result.modifiedCount + (result.upsertedCount || 0),
        // lastInsertId: result.upsertedId,
      };
    } catch (error) {
      throw new Error(`Update failed: ${(error as Error).message}`);
    }
  }

  private prepareUpdateDocument(
    update: Record<string, any>
  ): Record<string, any> {
    // If update contains operators, return as-is
    const hasOperators = Object.keys(update).some((key) => key.startsWith("$"));
    if (hasOperators) {
      return update;
    }

    // Otherwise, wrap in $set
    return { $set: update };
  }

  async delete(
    collectionName: string,
    filter: Record<string, any>,
    options?: { multi?: boolean }
  ): Promise<MongoResult> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);

      let result;
      if (options?.multi) {
        result = await collection.deleteMany(filter, {
          session: this.currentSession || undefined,
        });
      } else {
        result = await collection.deleteOne(filter, {
          session: this.currentSession || undefined,
        });
      }

      return {
        rows: [],
        rowsAffected: result.deletedCount,
      };
    } catch (error) {
      throw new Error(`Delete failed: ${(error as Error).message}`);
    }
  }

  async findOne(
    collectionName: string,
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<any> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.findOne(filter, {
        sort: options?.sort,
        projection: options?.projection,
        session: this.currentSession || undefined,
      });

      return result;
    } catch (error) {
      throw new Error(`Find one failed: ${(error as Error).message}`);
    }
  }

  async find(
    collectionName: string,
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<any[]> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      let cursor = collection.find(filter, {
        projection: options?.projection,
        session: this.currentSession || undefined,
      });

      if (options?.sort) {
        cursor = cursor.sort(options.sort);
      }

      if (options?.skip) {
        cursor = cursor.skip(options.skip);
      }

      if (options?.limit) {
        cursor = cursor.limit(options.limit);
      }

      return await cursor.toArray();
    } catch (error) {
      throw new Error(`Find failed: ${(error as Error).message}`);
    }
  }

  async count(
    collectionName: string,
    filter: Record<string, any> = {}
  ): Promise<number> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      return await collection.countDocuments(filter, {
        session: this.currentSession || undefined,
      });
    } catch (error) {
      throw new Error(`Count failed: ${(error as Error).message}`);
    }
  }

  // ========================== AGGREGATION ==========================
  async aggregate(
    collectionName: string,
    pipeline: Record<string, any>[],
    options?: { allowDiskUse?: boolean }
  ): Promise<any[]> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const cursor = collection.aggregate(pipeline, {
        allowDiskUse: options?.allowDiskUse,
        session: this.currentSession || undefined,
      });

      return await cursor.toArray();
    } catch (error) {
      throw new Error(`Aggregation failed: ${(error as Error).message}`);
    }
  }

  // ========================== UTILITY METHODS ==========================
  async getDatabaseInfo(): Promise<any> {
    this.ensureConnected();

    try {
      const admin = this.connection!.db.admin();
      const collections = await this.connection!.db.listCollections().toArray();
      const stats = await this.connection!.db.stats();

      return {
        name: this.databaseName,
        collections: collections.map((c) => c.name),
        isConnected: this.isConnected,
        stats,
        version: await this.getSchemaVersion(),
      };
    } catch (error) {
      throw new Error(`Get database info failed: ${(error as Error).message}`);
    }
  }

  async getCollectionInfo(collectionName: string): Promise<any> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const indexes = await collection.indexes();

      return {
        name: collectionName,
        indexes,
      };
    } catch (error) {
      throw new Error(
        `Get collection info failed: ${(error as Error).message}`
      );
    }
  }

  async dropCollection(collectionName: string): Promise<void> {
    this.ensureConnected();

    try {
      await this.connection!.db.dropCollection(collectionName);
    } catch (error) {
      throw new Error(`Drop collection failed: ${(error as Error).message}`);
    }
  }

  // ========================== MIGRATION HELPER METHODS ==========================

  /**
   * Get all collections in database
   */
  async getCollectionNames(): Promise<string[]> {
    this.ensureConnected();

    try {
      const collections = await this.connection!.db.listCollections().toArray();
      return collections
        .map((c) => c.name)
        .filter((name) => !name.startsWith("_"));
    } catch (error) {
      throw new Error(
        `Get collection names failed: ${(error as Error).message}`
      );
    }
  }

  /**
   * Check if collection exists
   */
  async collectionExists(collectionName: string): Promise<boolean> {
    this.ensureConnected();

    try {
      const collections = await this.connection!.db.listCollections({
        name: collectionName,
      }).toArray();
      return collections.length > 0;
    } catch (error) {
      return false;
    }
  }

  /**
   * Create collection with validation schema
   */
  async createCollection(
    collectionName: string,
    validation?: Record<string, any>
  ): Promise<void> {
    this.ensureConnected();

    try {
      const options: any = {};
      if (validation) {
        options.validator = validation;
        options.validationLevel = "moderate";
        options.validationAction = "warn";
      }

      await this.connection!.db.createCollection(collectionName, options);
    } catch (error) {
      throw new Error(`Create collection failed: ${(error as Error).message}`);
    }
  }

  /**
   * Bulk insert with batch processing
   */
  async bulkInsert(
    collectionName: string,
    documents: Record<string, any>[],
    batchSize: number = 1000
  ): Promise<MongoResult> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const results: any[] = [];
      let totalInserted = 0;

      for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        const result = await collection.insertMany(batch, {
          session: this.currentSession || undefined,
          ordered: false,
        });

        results.push(
          ...batch.map((doc, index) => ({
            _id: result.insertedIds[index],
            ...doc,
          }))
        );

        totalInserted += result.insertedCount;
      }

      return {
        rows: results,
        rowsAffected: totalInserted,
        insertedIds: results.map((r) => r._id),
      };
    } catch (error) {
      throw new Error(`Bulk insert failed: ${(error as Error).message}`);
    }
  }

  /**
   * Export collection data
   */
  async exportCollection(
    collectionName: string,
    options?: {
      filter?: Record<string, any>;
      projection?: Record<string, 0 | 1>;
      limit?: number;
      sort?: Record<string, 1 | -1>;
    }
  ): Promise<any[]> {
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      let cursor = collection.find(options?.filter || {});

      if (options?.projection) {
        cursor = cursor.project(options.projection);
      }

      if (options?.sort) {
        cursor = cursor.sort(options.sort);
      }

      if (options?.limit) {
        cursor = cursor.limit(options.limit);
      }

      return await cursor.toArray();
    } catch (error) {
      throw new Error(`Export collection failed: ${(error as Error).message}`);
    }
  }

  // ========================== HELPER METHODS ==========================
  createObjectId(id?: string): ObjectId {
    return id ? new ObjectId(id) : new ObjectId();
  }

  isValidObjectId(id: string): boolean {
    return ObjectId.isValid(id);
  }

  /**
   * Convert MongoDB document to SQLite-compatible format
   */
  mongoToSQLiteFormat(document: any): Record<string, any> {
    if (!document) return document;

    const converted = { ...document };

    // Convert _id to id
    if (converted._id) {
      converted.id = converted._id.toString();
      delete converted._id;
    }

    // Convert ObjectIds to strings
    for (const [key, value] of Object.entries(converted)) {
      if (value instanceof ObjectId) {
        converted[key] = value.toString();
      } else if (value instanceof Date) {
        converted[key] = value.toISOString();
      } else if (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
      ) {
        converted[key] = JSON.stringify(value);
      } else if (Array.isArray(value)) {
        converted[key] = JSON.stringify(value);
      }
    }

    return converted;
  }

  /**
   * Convert SQLite record to MongoDB-compatible format
   */
  sqliteToMongoFormat(record: Record<string, any>): Record<string, any> {
    if (!record) return record;

    const converted = { ...record };

    // Convert id to _id if it's a valid ObjectId
    if (converted.id) {
      if (ObjectId.isValid(converted.id)) {
        converted._id = new ObjectId(converted.id);
      } else {
        converted._id = converted.id;
      }
      delete converted.id;
    }

    // Parse JSON strings back to objects/arrays
    for (const [key, value] of Object.entries(converted)) {
      if (typeof value === "string") {
        // Try to parse as date
        if (value.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
          const date = new Date(value);
          if (!isNaN(date.getTime())) {
            converted[key] = date;
            continue;
          }
        }

        // Try to parse as JSON
        if (
          (value.startsWith("{") && value.endsWith("}")) ||
          (value.startsWith("[") && value.endsWith("]"))
        ) {
          try {
            converted[key] = JSON.parse(value);
          } catch {
            // Keep as string if parsing fails
          }
        }
      }
    }

    return converted;
  }

  /**
   * Sanitize field names for MongoDB (remove problematic characters)
   */
  sanitizeFieldName(fieldName: string): string {
    // MongoDB field names cannot contain dots, dollar signs, or null characters
    return fieldName
      .replace(/\./g, "_")
      .replace(/\$/g, "_")
      .replace(/\x00/g, "");
  }

  /**
   * Build MongoDB query from SQLite-style where clauses
   */
  buildMongoQuery(
    wheres: Array<{ name: string; value: any; operator?: string }>
  ): Record<string, any> {
    const query: Record<string, any> = {};

    for (const where of wheres) {
      const fieldName = where.name === "id" ? "_id" : where.name;
      const operator = where.operator || "=";
      let value = where.value;

      // Convert string to ObjectId if field is _id
      if (
        fieldName === "_id" &&
        typeof value === "string" &&
        ObjectId.isValid(value)
      ) {
        value = new ObjectId(value);
      }

      switch (operator.toLowerCase()) {
        case "=":
        case "==":
          query[fieldName] = value;
          break;
        case "!=":
        case "<>":
          query[fieldName] = { $ne: value };
          break;
        case ">":
          query[fieldName] = { $gt: value };
          break;
        case ">=":
          query[fieldName] = { $gte: value };
          break;
        case "<":
          query[fieldName] = { $lt: value };
          break;
        case "<=":
          query[fieldName] = { $lte: value };
          break;
        case "like":
          // Convert SQL LIKE to MongoDB regex
          const regexPattern = value.replace(/%/g, ".*").replace(/_/g, ".");
          query[fieldName] = { $regex: new RegExp(regexPattern, "i") };
          break;
        case "in":
          query[fieldName] = { $in: Array.isArray(value) ? value : [value] };
          break;
        case "not in":
          query[fieldName] = { $nin: Array.isArray(value) ? value : [value] };
          break;
        case "exists":
          query[fieldName] = { $exists: Boolean(value) };
          break;
        default:
          query[fieldName] = value;
      }
    }

    return query;
  }

  /**
   * Convert SQLite ORDER BY to MongoDB sort
   */
  buildMongoSort(
    orderBys: Array<{ name: string; direction?: "ASC" | "DESC" }>
  ): Record<string, 1 | -1> {
    const sort: Record<string, 1 | -1> = {};

    for (const orderBy of orderBys) {
      const fieldName = orderBy.name === "id" ? "_id" : orderBy.name;
      sort[fieldName] = orderBy.direction === "DESC" ? -1 : 1;
    }

    return sort;
  }
}
```

```ts
// src/logger/logger-config.ts - Alternative solution using Proxy pattern

import {
  BaseModule,
  LoggerConfigBuilder,
  createLogger,
  UniversalLogger,
} from "@dqcai/logger";

export { BaseModule };

export enum MongoModules {
  DATABASE_FACTORY = "DatabaseFactory",
  UNIVERSAL_DAO = "UniversalDAO",
  BASE_SERVICE = "BaseService",
}

interface ModuleLogger {
  trace: (message: string, ...args: any[]) => void;
  debug: (message: string, ...args: any[]) => void;
  info: (message: string, ...args: any[]) => void;
  warn: (message: string, ...args: any[]) => void;
  error: (message: string, ...args: any[]) => void;
}

/**
 * Logger Proxy - always delegates to current logger instance
 */
class LoggerProxy implements ModuleLogger {
  constructor(private moduleName: string) {}

  trace(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().trace(this.moduleName, message, ...args);
  }

  debug(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().debug(this.moduleName, message, ...args);
  }

  info(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().info(this.moduleName, message, ...args);
  }

  warn(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().warn(this.moduleName, message, ...args);
  }

  error(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().error(this.moduleName, message, ...args);
  }
}

/**
 * Enhanced Mongo Logger Configuration with automatic update support
 */
export class MongoLoggerConfig {
  private static instance: UniversalLogger | null = null;
  private static currentConfig: any = null;
  // Track proxy instances for debugging
  public static proxyInstances: Map<string, LoggerProxy> = new Map();

  static createDefaultConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("warn")
      .addModule(
        MongoModules.DATABASE_FACTORY,
        true,
        ["warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.UNIVERSAL_DAO,
        true,
        ["warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.BASE_SERVICE,
        true,
        ["warn", "error"],
        ["console"]
      )
      .build();
  }

  static initialize(customConfig?: any): UniversalLogger {
    const config = customConfig || MongoLoggerConfig.createDefaultConfig();
    MongoLoggerConfig.currentConfig = config;

    if (
      config.enabled &&
      (config.defaultLevel === "trace" || config.defaultLevel === "debug")
    ) {
      console.debug(
        `MongoLoggerConfig.initialize() with ${
          customConfig ? "CUSTOM" : "default"
        } config`
      );
    }

    MongoLoggerConfig.instance = createLogger(config);
    return MongoLoggerConfig.instance;
  }

  static getInstance(): UniversalLogger {
    if (!MongoLoggerConfig.instance) {
      return MongoLoggerConfig.initialize();
    }
    return MongoLoggerConfig.instance;
  }

  /**
   * Update configuration - proxy pattern automatically handles updates
   */
  static updateConfiguration(newConfig: any): void {
    if (
      newConfig &&
      newConfig.enabled &&
      (newConfig.defaultLevel === "trace" || newConfig.defaultLevel === "debug")
    ) {
      console.debug(
        "MongoLoggerConfig.updateConfiguration()",
        JSON.stringify(newConfig, null, 2)
      );
    }

    MongoLoggerConfig.currentConfig = newConfig;
    MongoLoggerConfig.instance = createLogger(newConfig);

    // Log update confirmation
    if (
      newConfig &&
      newConfig.enabled &&
      (newConfig.defaultLevel === "trace" ||
        newConfig.defaultLevel === "debug" ||
        newConfig.defaultLevel === "info")
    ) {
      console.log(
        "MongoLoggerConfig.updateConfiguration() - Configuration updated. Proxy loggers will use new settings automatically.",
        `Active proxies: ${Array.from(MongoLoggerConfig.proxyInstances.keys())}`
      );
    }
  }

  static setEnabled(enabled: boolean): void {
    if (MongoLoggerConfig.currentConfig) {
      MongoLoggerConfig.currentConfig.enabled = enabled;
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static enableModule(
    moduleName: string,
    levels?: string[],
    appenders?: string[]
  ): void {
    if (
      MongoLoggerConfig.currentConfig &&
      MongoLoggerConfig.currentConfig.modules
    ) {
      MongoLoggerConfig.currentConfig.modules[moduleName] = {
        enabled: true,
        levels: levels || ["debug", "info", "warn", "error"],
        appenders: appenders || ["console"],
      };
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static disableModule(moduleName: string): void {
    if (
      MongoLoggerConfig.currentConfig &&
      MongoLoggerConfig.currentConfig.modules
    ) {
      MongoLoggerConfig.currentConfig.modules[moduleName] = {
        enabled: false,
      };
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static createDebugConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("trace")
      .addModule(
        MongoModules.DATABASE_FACTORY,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.UNIVERSAL_DAO,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.BASE_SERVICE,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .build();
  }

  static createProductionConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("error")
      .addModule(MongoModules.DATABASE_FACTORY, true, ["error"], ["console"])
      .addModule(MongoModules.UNIVERSAL_DAO, true, ["error"], ["console"])
      .addModule(MongoModules.BASE_SERVICE, true, ["error"], ["console"])
      .build();
  }

  static reset(): UniversalLogger {
    return MongoLoggerConfig.initialize();
  }

  /**
   * Get active proxy modules
   */
  static getActiveProxyModules(): string[] {
    return Array.from(MongoLoggerConfig.proxyInstances.keys());
  }

  /**
   * Get current configuration (for debugging)
   */
  static getCurrentConfig(): any {
    return MongoLoggerConfig.currentConfig
      ? { ...MongoLoggerConfig.currentConfig }
      : null;
  }
}

export const mongoLogger = MongoLoggerConfig.getInstance();

/**
 * Create module logger using proxy pattern - automatically updates when configuration changes
 */
export const createModuleLogger = (moduleName: string): ModuleLogger => {
  // Check if proxy already exists for this module
  if (MongoLoggerConfig.proxyInstances.has(moduleName)) {
    return MongoLoggerConfig.proxyInstances.get(moduleName)!;
  }

  // Create new proxy
  const proxy = new LoggerProxy(moduleName);
  MongoLoggerConfig.proxyInstances.set(moduleName, proxy);

  return proxy;
};

/**
 * Utility functions for testing and debugging
 */
export const LoggerUtils = {
  /**
   * Test if a module logger responds to configuration changes
   */
  testDynamicUpdate: (moduleName: string): void => {
    const logger = createModuleLogger(moduleName);

    console.log(`\n=== Testing ${moduleName} Logger Dynamic Updates ===`);

    // Test with debug config
    console.log("1. Setting debug configuration...");
    MongoLoggerConfig.updateConfiguration(
      MongoLoggerConfig.createDebugConfig()
    );
    logger.debug("This DEBUG message should be visible");
    logger.info("This INFO message should be visible");

    // Test with production config
    console.log("2. Setting production configuration...");
    MongoLoggerConfig.updateConfiguration(
      MongoLoggerConfig.createProductionConfig()
    );
    logger.debug("This DEBUG message should be HIDDEN");
    logger.info("This INFO message should be HIDDEN");
    logger.error("This ERROR message should be visible");

    // Test module disable
    console.log("3. Disabling specific module...");
    MongoLoggerConfig.disableModule(moduleName);
    logger.error("This ERROR message should be HIDDEN (module disabled)");

    // Test module re-enable
    console.log("4. Re-enabling specific module...");
    MongoLoggerConfig.enableModule(moduleName);
    logger.error("This ERROR message should be visible again");

    console.log(`=== End test for ${moduleName} ===\n`);
  },

  /**
   * Show current logger statistics
   */
  showStats: (): void => {
    console.log("\n=== Logger Statistics ===");
    console.log(
      `Active proxy modules: ${
        MongoLoggerConfig.getActiveProxyModules().length
      }`
    );
    console.log(`Proxy modules:`, MongoLoggerConfig.getActiveProxyModules());
    console.log(
      `Current config enabled:`,
      MongoLoggerConfig.getCurrentConfig()?.enabled
    );
    console.log(
      `Current default level:`,
      MongoLoggerConfig.getCurrentConfig()?.defaultLevel
    );
    console.log("========================\n");
  },
};
```

```ts
// src/index.ts - Main exports for UniversalMongodb Library with Logger Integration
// ========================== LOGGER EXPORTS ==========================
export {
  MongoLoggerConfig,
  MongoModules,
  mongoLogger,
  createModuleLogger,
} from "./logger/logger-config";

// ========================== CORE EXPORTS ==========================
export { MongoUniversalDAO } from "./core/universal-dao";
export { MongoDatabaseFactory } from "./core/database-factory";
export { MongoBaseService } from "./core/base-service";
export { BaseMongoAdapter } from "./adapters/base-adapter";

// ========================== TYPE EXPORTS ==========================
export * from "./types";
```
