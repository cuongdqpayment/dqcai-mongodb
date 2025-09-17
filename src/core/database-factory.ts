// src/core/database-factory.ts

import { DatabaseSchema, MongoClientOptions } from "../types";
import { MongoUniversalDAO } from "./universal-dao";
import { MongoAdapter } from "../types";
import { BaseMongoAdapter } from "../adapters/base-adapter";
import { MongoModules, createModuleLogger } from "../logger/logger-config";

const logger = createModuleLogger(MongoModules.DATABASE_FACTORY);

// ========================== DATABASE FACTORY FOR MONGODB ==========================
export class MongoDatabaseFactory {
  private static adapters: MongoAdapter[] = [];

  static registerAdapter(adapter: MongoAdapter): void {
    logger.trace("Registering new adapter", {
      adapterName: adapter.constructor.name,
    });
    this.adapters.push(adapter);
  }

  private static getBestAdapter(): MongoAdapter {
    logger.debug("Finding best adapter", { adapterCount: this.adapters.length });
    const adapter = this.adapters.find((a) => a.isSupported());
    if (!adapter) {
      logger.warn("No supported adapter found, using default BaseMongoAdapter");
      return new BaseMongoAdapter();
    }
    logger.debug("Selected adapter", { adapterName: adapter.constructor.name });
    return adapter;
  }

  static createDAO(
    connectionString: string,
    databaseName: string,
    options?: MongoClientOptions
  ): MongoUniversalDAO {
    logger.debug("Creating DAO", { databaseName, connectionString });
    const adapter = this.getBestAdapter();
    const dao = new MongoUniversalDAO(adapter, connectionString, databaseName, options);
    logger.trace("DAO created successfully", { databaseName });
    return dao;
  }

  /**
   * Create DAO from SQLite-compatible DatabaseSchema
   */
  static async createFromSchema(
    schema: DatabaseSchema,
    connectionString: string,
    options?: MongoClientOptions
  ): Promise<MongoUniversalDAO> {
    logger.info("Creating DAO from schema", {
      databaseName: schema.database_name,
      schemaVersion: schema.version,
    });
    const dao = this.createDAO(connectionString, schema.database_name, options);
    await dao.connect();
    logger.debug("Connected to database", { databaseName: schema.database_name });

    await dao.initializeFromDatabaseSchema(schema);
    logger.info("DAO initialized from schema", {
      databaseName: schema.database_name,
      schemaVersion: schema.version,
    });
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
    logger.info("Creating DAO with custom database name", {
      schemaDatabaseName: schema.database_name,
      customDatabaseName,
    });
    const dao = this.createDAO(connectionString, customDatabaseName, options);
    await dao.connect();
    logger.debug("Connected to database with custom name", { customDatabaseName });

    const modifiedSchema: DatabaseSchema = {
      ...schema,
      database_name: customDatabaseName,
    };

    await dao.initializeFromDatabaseSchema(modifiedSchema);
    logger.info("DAO initialized with modified schema", { customDatabaseName });
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
    logger.info("Creating multiple DAOs from schema", {
      databaseCount: databaseNames.length,
      schemaVersion: schema.version,
    });
    const daos: Record<string, MongoUniversalDAO> = {};

    for (const dbName of databaseNames) {
      logger.trace("Creating DAO for database", { databaseName: dbName });
      const dao = await this.createFromSchemaWithCustomDb(
        schema,
        connectionString,
        dbName,
        options
      );
      daos[dbName] = dao;
      logger.debug("DAO added for database", { databaseName: dbName });
    }

    logger.info("Multiple DAOs created successfully", {
      databaseCount: databaseNames.length,
    });
    return daos;
  }

  /**
   * Validate schema before creating DAO
   */
  static validateSchema(schema: DatabaseSchema): {
    isValid: boolean;
    errors: string[];
  } {
    logger.debug("Validating schema", { schemaVersion: schema.version });
    const errors: string[] = [];

    if (!schema.version) {
      errors.push("Schema version is required");
      logger.warn("Schema validation failed: version missing");
    }

    if (!schema.database_name) {
      errors.push("Database name is required");
      logger.warn("Schema validation failed: database name missing");
    }

    if (!schema.schemas || Object.keys(schema.schemas).length === 0) {
      errors.push("At least one collection schema is required");
      logger.warn("Schema validation failed: no collection schemas provided");
    }

    for (const [collectionName, collectionSchema] of Object.entries(
      schema.schemas || {}
    )) {
      logger.trace("Validating collection schema", { collectionName });
      if (!collectionSchema.cols || collectionSchema.cols.length === 0) {
        errors.push(
          `Collection '${collectionName}' must have at least one column`
        );
        logger.warn("Schema validation failed: no columns in collection", {
          collectionName,
        });
      }

      const columnNames = new Set();
      for (const col of collectionSchema.cols) {
        if (!col.name) {
          errors.push(
            `Collection '${collectionName}' has a column without name`
          );
          logger.warn("Schema validation failed: column without name", {
            collectionName,
          });
          continue;
        }

        if (columnNames.has(col.name)) {
          errors.push(
            `Collection '${collectionName}' has duplicate column name: ${col.name}`
          );
          logger.warn("Schema validation failed: duplicate column name", {
            collectionName,
            columnName: col.name,
          });
        }
        columnNames.add(col.name);

        if (!col.type) {
          errors.push(
            `Column '${col.name}' in collection '${collectionName}' must have a type`
          );
          logger.warn("Schema validation failed: column type missing", {
            collectionName,
            columnName: col.name,
          });
        }
      }

      if (collectionSchema.indexes) {
        for (const index of collectionSchema.indexes) {
          logger.trace("Validating index", { collectionName, indexName: index.name });
          if (!index.name) {
            errors.push(
              `Collection '${collectionName}' has an index without name`
            );
            logger.warn("Schema validation failed: index without name", {
              collectionName,
            });
          }

          if (!index.columns || index.columns.length === 0) {
            errors.push(
              `Index '${index.name}' in collection '${collectionName}' must have at least one column`
            );
            logger.warn("Schema validation failed: index without columns", {
              collectionName,
              indexName: index.name,
            });
          }

          const availableColumns = collectionSchema.cols.map((col) => col.name);
          for (const indexColumn of index.columns || []) {
            if (!availableColumns.includes(indexColumn)) {
              errors.push(
                `Index '${index.name}' in collection '${collectionName}' references non-existent column: ${indexColumn}`
              );
              logger.warn("Schema validation failed: non-existent column in index", {
                collectionName,
                indexName: index.name,
                indexColumn,
              });
            }
          }
        }
      }
    }

    logger.info("Schema validation completed", {
      isValid: errors.length === 0,
      errorCount: errors.length,
    });
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
    logger.debug("Creating DAO with schema validation", {
      schemaVersion: schema.version,
      databaseName: schema.database_name,
    });
    const validation = this.validateSchema(schema);

    if (!validation.isValid) {
      const errorMessage = `Schema validation failed:\n${validation.errors.join("\n")}`;
      logger.error("Schema validation failed", {
        databaseName: schema.database_name,
        errors: validation.errors,
      });

      if (options?.throwOnValidationError !== false) {
        throw new Error(errorMessage);
      } else {
        logger.warn(errorMessage); // Replaced console.warn
      }
    }

    const dao = await this.createFromSchema(schema, connectionString, options);
    logger.info("DAO created with validated schema", {
      databaseName: schema.database_name,
    });
    return dao;
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
    logger.debug("Retrieving schema information", {
      schemaVersion: schema.version,
      databaseName: schema.database_name,
    });
    const collections = Object.entries(schema.schemas || {}).map(
      ([name, config]) => {
        logger.trace("Processing collection for schema info", { collectionName: name });
        return {
          name,
          columnCount: config.cols?.length || 0,
          indexCount: config.indexes?.length || 0,
          hasValidation:
            config.cols?.some((col) => col.enum || !col.nullable) || false,
        };
      }
    );

    const result = {
      version: schema.version,
      databaseName: schema.database_name,
      collectionCount: collections.length,
      totalColumns: collections.reduce((sum, col) => sum + col.columnCount, 0),
      totalIndexes: collections.reduce((sum, col) => sum + col.indexCount, 0),
      collections,
    };

    logger.info("Schema information retrieved", {
      databaseName: schema.database_name,
      collectionCount: collections.length,
    });
    return result;
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

    logger.info("Creating DAO with connection test", {
      databaseName: schema.database_name,
      retryAttempts,
      testTimeout,
    });

    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= retryAttempts; attempt++) {
      try {
        logger.debug("Attempting connection", { attempt, databaseName: schema.database_name });
        const dao = this.createDAO(
          connectionString,
          schema.database_name,
          options
        );

        const connectPromise = dao.connect();
        const timeoutPromise = new Promise<never>((_, reject) => {
          setTimeout(
            () => reject(new Error("Connection timeout")),
            testTimeout
          );
        });

        await Promise.race([connectPromise, timeoutPromise]);
        logger.debug("Connection successful", { databaseName: schema.database_name });

        await dao.getDatabaseInfo();
        logger.debug("Database info retrieved successfully", {
          databaseName: schema.database_name,
        });

        await dao.initializeFromDatabaseSchema(schema);
        logger.info("DAO initialized with schema after connection test", {
          databaseName: schema.database_name,
        });

        return dao;
      } catch (error) {
        lastError = error as Error;
        logger.warn(`Connection attempt ${attempt}/${retryAttempts} failed`, {
          databaseName: schema.database_name,
          error: lastError.message,
        });

        if (attempt < retryAttempts) {
          logger.debug("Retrying after delay", { delay: retryDelay });
          await new Promise((resolve) => setTimeout(resolve, retryDelay));
        }
      }
    }

    logger.error("Failed to connect after maximum attempts", {
      databaseName: schema.database_name,
      retryAttempts,
      lastError: lastError?.message,
    });
    throw new Error(
      `Failed to connect after ${retryAttempts} attempts. Last error: ${lastError?.message}`
    );
  }

  /**
   * Cleanup all registered adapters
   */
  static async cleanup(): Promise<void> {
    logger.info("Cleaning up registered adapters", { adapterCount: this.adapters.length });
    for (const adapter of this.adapters) {
      if (typeof adapter.disconnectAll === "function") {
        logger.trace("Disconnecting adapter", { adapterName: adapter.constructor.name });
        await adapter.disconnectAll();
      }
    }
    this.adapters = [];
    logger.info("Cleanup completed", { adapterCount: 0 });
  }

  /**
   * Get registered adapter information
   */
  static getAdapterInfo(): Array<{
    name: string;
    version?: string;
    isSupported: boolean;
  }> {
    logger.debug("Retrieving adapter information", { adapterCount: this.adapters.length });
    const adapterInfo = this.adapters.map((adapter) => ({
      name: adapter.constructor.name,
      version: (adapter as any).version,
      isSupported: adapter.isSupported(),
    }));
    logger.info("Adapter information retrieved", { adapterCount: adapterInfo.length });
    return adapterInfo;
  }
}