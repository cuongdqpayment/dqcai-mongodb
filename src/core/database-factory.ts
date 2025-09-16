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
      database_name: customDatabaseName
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
  static validateSchema(schema: DatabaseSchema): { isValid: boolean; errors: string[] } {
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
    for (const [collectionName, collectionSchema] of Object.entries(schema.schemas || {})) {
      if (!collectionSchema.cols || collectionSchema.cols.length === 0) {
        errors.push(`Collection '${collectionName}' must have at least one column`);
      }
      
      // Check for duplicate column names
      const columnNames = new Set();
      for (const col of collectionSchema.cols) {
        if (!col.name) {
          errors.push(`Collection '${collectionName}' has a column without name`);
          continue;
        }
        
        if (columnNames.has(col.name)) {
          errors.push(`Collection '${collectionName}' has duplicate column name: ${col.name}`);
        }
        columnNames.add(col.name);
        
        if (!col.type) {
          errors.push(`Column '${col.name}' in collection '${collectionName}' must have a type`);
        }
      }
      
      // Validate indexes
      if (collectionSchema.indexes) {
        for (const index of collectionSchema.indexes) {
          if (!index.name) {
            errors.push(`Collection '${collectionName}' has an index without name`);
          }
          
          if (!index.columns || index.columns.length === 0) {
            errors.push(`Index '${index.name}' in collection '${collectionName}' must have at least one column`);
          }
          
          // Check if indexed columns exist
          const availableColumns = collectionSchema.cols.map(col => col.name);
          for (const indexColumn of index.columns || []) {
            if (!availableColumns.includes(indexColumn)) {
              errors.push(`Index '${index.name}' in collection '${collectionName}' references non-existent column: ${indexColumn}`);
            }
          }
        }
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors
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
      const errorMessage = `Schema validation failed:\n${validation.errors.join('\n')}`;
      
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
    const collections = Object.entries(schema.schemas || {}).map(([name, config]) => ({
      name,
      columnCount: config.cols?.length || 0,
      indexCount: config.indexes?.length || 0,
      hasValidation: config.cols?.some(col => col.enum || !col.nullable) || false
    }));
    
    return {
      version: schema.version,
      databaseName: schema.database_name,
      collectionCount: collections.length,
      totalColumns: collections.reduce((sum, col) => sum + col.columnCount, 0),
      totalIndexes: collections.reduce((sum, col) => sum + col.indexCount, 0),
      collections
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
        const dao = this.createDAO(connectionString, schema.database_name, options);
        
        // Test connection with timeout
        const connectPromise = dao.connect();
        const timeoutPromise = new Promise<never>((_, reject) => {
          setTimeout(() => reject(new Error('Connection timeout')), testTimeout);
        });
        
        await Promise.race([connectPromise, timeoutPromise]);
        
        // Test database operations
        await dao.getDatabaseInfo();
        
        // Initialize schema
        await dao.initializeFromDatabaseSchema(schema);
        
        return dao;
      } catch (error) {
        lastError = error as Error;
        console.warn(`Connection attempt ${attempt}/${retryAttempts} failed:`, error);
        
        if (attempt < retryAttempts) {
          await new Promise(resolve => setTimeout(resolve, retryDelay));
        }
      }
    }
    
    throw new Error(`Failed to connect after ${retryAttempts} attempts. Last error: ${lastError?.message}`);
  }

  /**
   * Cleanup all registered adapters
   */
  static async cleanup(): Promise<void> {
    for (const adapter of this.adapters) {
      if (typeof adapter.disconnectAll === 'function') {
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
    return this.adapters.map(adapter => ({
      name: adapter.constructor.name,
      version: (adapter as any).version,
      isSupported: adapter.isSupported()
    }));
  }
}