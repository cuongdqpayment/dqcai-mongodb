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
import { MongoModules, createModuleLogger } from "../logger/logger-config";
const logger = createModuleLogger(MongoModules.BASE_SERVICE);

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
