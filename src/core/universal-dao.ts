// src/core/universal-dao.ts

import {
  ClientSession,
  MongoClientOptions,
  Collection,
  ObjectId,
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
  MongoImportOptions,
  ColumnMapping,
  ImportResult,
} from "../types";

// Import logger configuration for internal use
import { MongoModules, createModuleLogger } from "../logger/logger-config";

// ========================== MONGODB DAO ==========================
export class MongoUniversalDAO {
  private connection: MongoConnection | null = null;
  private isConnected: boolean = false;
  private currentSession: ClientSession | null = null;
  private inTransaction: boolean = false;
  private adapter: MongoAdapter;
  private connectionString: string;
  private databaseName: string;
  private logger = createModuleLogger(MongoModules.UNIVERSAL_DAO);

  constructor(
    adapter: MongoAdapter,
    connectionString: string,
    databaseName: string,
    private options?: MongoClientOptions
  ) {
    this.logger.trace("Creating MongoUniversalDAO instance", {
      databaseName,
      connectionString,
    });
    this.adapter = adapter;
    this.connectionString = connectionString;
    this.databaseName = databaseName;
    this.logger.info("MongoUniversalDAO instance created successfully", {
      databaseName,
    });
  }

  // ========================== CONNECTION MANAGEMENT ==========================
  async connect(): Promise<void> {
    this.logger.debug("Attempting to connect to database", { databaseName: this.databaseName });
    if (this.isConnected && this.connection) {
      this.logger.debug("Connection already established, skipping", { databaseName: this.databaseName });
      return;
    }

    try {
      this.connection = await this.adapter.connect(
        this.connectionString,
        this.databaseName,
        this.options
      );
      this.isConnected = true;
      this.logger.info("Database connection established", { databaseName: this.databaseName });
    } catch (error) {
      this.logger.error("Failed to connect to database", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    this.logger.debug("Attempting to disconnect from database", { databaseName: this.databaseName });
    try {
      if (this.currentSession) {
        this.logger.trace("Ending active session", { databaseName: this.databaseName });
        await this.currentSession.endSession();
        this.currentSession = null;
      }

      if (this.connection) {
        await this.adapter.disconnect(this.connectionString, this.databaseName);
        this.connection = null;
        this.isConnected = false;
        this.logger.info("Database connection closed", { databaseName: this.databaseName });
      } else {
        this.logger.debug("No connection to close", { databaseName: this.databaseName });
      }
    } catch (error) {
      this.logger.error("Error disconnecting from database", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  isConnectionOpen(): boolean {
    this.logger.trace("Checking connection status", { databaseName: this.databaseName });
    return this.isConnected && !!this.connection;
  }

  private ensureConnected(): void {
    this.logger.trace("Ensuring database connection", { databaseName: this.databaseName });
    if (!this.isConnectionOpen()) {
      this.logger.error("Database connection is not open", { databaseName: this.databaseName });
      throw new Error("Database connection is not open. Call connect() first.");
    }
  }

  private getCollection(collectionName: string): Collection {
    this.logger.trace("Retrieving collection", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();
    return this.connection!.db.collection(collectionName);
  }

  // ========================== TRANSACTION MANAGEMENT ==========================
  async beginTransaction(): Promise<void> {
    this.logger.debug("Beginning transaction", { databaseName: this.databaseName });
    if (this.inTransaction) {
      this.logger.error("Transaction already in progress", { databaseName: this.databaseName });
      throw new Error("Transaction already in progress");
    }

    this.ensureConnected();
    try {
      this.currentSession = this.connection!.client.startSession();
      this.currentSession.startTransaction();
      this.inTransaction = true;
      this.logger.info("Transaction started", { databaseName: this.databaseName });
    } catch (error) {
      this.logger.error("Failed to start transaction", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  async commitTransaction(): Promise<void> {
    this.logger.debug("Committing transaction", { databaseName: this.databaseName });
    if (!this.inTransaction || !this.currentSession) {
      this.logger.error("No transaction in progress", { databaseName: this.databaseName });
      throw new Error("No transaction in progress");
    }

    try {
      await this.currentSession.commitTransaction();
      await this.currentSession.endSession();
      this.currentSession = null;
      this.inTransaction = false;
      this.logger.info("Transaction committed successfully", { databaseName: this.databaseName });
    } catch (error) {
      this.logger.error("Failed to commit transaction", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  async rollbackTransaction(): Promise<void> {
    this.logger.debug("Rolling back transaction", { databaseName: this.databaseName });
    if (!this.inTransaction || !this.currentSession) {
      this.logger.error("No transaction in progress", { databaseName: this.databaseName });
      throw new Error("No transaction in progress");
    }

    try {
      await this.currentSession.abortTransaction();
      await this.currentSession.endSession();
      this.currentSession = null;
      this.inTransaction = false;
      this.logger.info("Transaction rolled back successfully", { databaseName: this.databaseName });
    } catch (error) {
      this.logger.error("Failed to rollback transaction", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  // ========================== SCHEMA MANAGEMENT ==========================

  /**
   * Initialize database from SQLite-compatible DatabaseSchema
   */
  async initializeFromDatabaseSchema(schema: DatabaseSchema): Promise<void> {
    this.logger.info("Initializing database from schema", {
      databaseName: this.databaseName,
      schemaVersion: schema.version,
    });
    this.ensureConnected();

    try {
      // Convert SQLite-style schema to MongoDB schema
      this.logger.debug("Converting SQLite schema to MongoDB schema", {
        databaseName: this.databaseName,
      });
      const mongoSchema = this.convertToMongoSchema(schema);

      // Initialize with converted schema
      await this.initializeFromSchema(mongoSchema);
      this.logger.info("Database schema initialized successfully", {
        databaseName: this.databaseName,
        schemaVersion: schema.version,
      });
    } catch (error) {
      this.logger.error("Database schema initialization failed", {
        databaseName: this.databaseName,
        schemaVersion: schema.version,
        error: (error as Error).message,
      });
      throw new Error(
        `Database schema initialization failed: ${(error as Error).message}`
      );
    }
  }

  /**
   * Convert SQLite DatabaseSchema to MongoDB MongoDatabaseSchema
   */
  private convertToMongoSchema(schema: DatabaseSchema): MongoDatabaseSchema {
    this.logger.trace("Converting SQLite schema to MongoDB schema", {
      databaseName: this.databaseName,
      schemaVersion: schema.version,
    });
    const mongoSchema: MongoDatabaseSchema = {
      version: schema.version,
      database_name: schema.database_name,
      description: schema.description,
      collections: {},
    };

    // Convert each table to a collection
    for (const [tableName, tableConfig] of Object.entries(schema.schemas)) {
      this.logger.trace("Processing table for collection", {
        databaseName: this.databaseName,
        tableName,
      });
      const collection = {
        name: tableName,
        schema: this.buildMongoSchema(tableConfig.cols),
        indexes: this.convertIndexes(tableConfig.indexes || []),
        validation: this.buildValidationSchema(tableConfig.cols),
      };

      mongoSchema.collections[tableName] = collection;
    }

    this.logger.debug("MongoDB schema conversion completed", {
      databaseName: this.databaseName,
      collectionCount: Object.keys(mongoSchema.collections).length,
    });
    return mongoSchema;
  }

  /**
   * Build MongoDB schema from column definitions
   */
  private buildMongoSchema(columns: ColumnDefinition[]): Record<string, any> {
    this.logger.trace("Building MongoDB schema from columns", {
      databaseName: this.databaseName,
      columnCount: columns.length,
    });
    const schema: Record<string, any> = {};

    for (const col of columns) {
      const fieldSchema: Record<string, any> = {};
      this.logger.trace("Processing column for schema", {
        databaseName: this.databaseName,
        columnName: col.name,
      });

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

    this.logger.debug("MongoDB schema built", {
      databaseName: this.databaseName,
      fieldCount: Object.keys(schema).length,
    });
    return schema;
  }

  /**
   * Map SQLite types to MongoDB types
   */
  private mapTypeToMongo(sqliteType: string): string {
    this.logger.trace("Mapping SQLite type to MongoDB type", {
      databaseName: this.databaseName,
      sqliteType,
    });
    const mapping: any = MONGODB_TYPE_MAPPING.mongodb;
    const mongoType = mapping[sqliteType.toLowerCase()] || "Mixed";
    this.logger.trace("Type mapped", {
      databaseName: this.databaseName,
      sqliteType,
      mongoType,
    });
    return mongoType;
  }

  /**
   * Convert SQLite indexes to MongoDB indexes
   */
  private convertIndexes(indexes: IndexDefinition[]): MongoIndexDefinition[] {
    this.logger.trace("Converting indexes to MongoDB format", {
      databaseName: this.databaseName,
      indexCount: indexes.length,
    });
    return indexes.map((index) => {
      const keys: Record<string, 1 | -1 | "text" | "2dsphere"> = {};

      // Convert column array to keys object
      for (const column of index.columns) {
        this.logger.trace("Processing index column", {
          databaseName: this.databaseName,
          indexName: index.name,
          column,
        });
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
    this.logger.trace("Building validation schema", {
      databaseName: this.databaseName,
      columnCount: columns.length,
    });
    const validationRules: Record<string, any> = {};
    const properties: Record<string, any> = {};
    const required: string[] = [];

    for (const col of columns) {
      const fieldName = col.name === "id" && col.primary_key ? "_id" : col.name;
      const fieldValidation: Record<string, any> = {};
      this.logger.trace("Processing column for validation", {
        databaseName: this.databaseName,
        fieldName,
      });

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
            this.logger.trace("Applying precision constraint", {
              databaseName: this.databaseName,
              fieldName,
              precision: col.precision,
            });
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
          this.logger.trace("No specific validation for Mixed type", {
            databaseName: this.databaseName,
            fieldName,
          });
          break;
      }

      // Enum validation
      if (col.enum) {
        fieldValidation.enum = col.enum;
        this.logger.trace("Applying enum validation", {
          databaseName: this.databaseName,
          fieldName,
          enumValues: col.enum,
        });
      }

      // Required validation
      if (!col.nullable && col.name !== "id") {
        required.push(fieldName);
        this.logger.trace("Marking field as required", {
          databaseName: this.databaseName,
          fieldName,
        });
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

    this.logger.debug("Validation schema built", {
      databaseName: this.databaseName,
      requiredFieldCount: required.length,
      propertyCount: Object.keys(properties).length,
    });
    return validationRules;
  }

  /**
   * Original method for MongoDB-specific schema initialization
   */
  async initializeFromSchema(schema: MongoDatabaseSchema): Promise<void> {
    this.logger.info("Initializing MongoDB schema", {
      databaseName: this.databaseName,
      schemaVersion: schema.version,
    });
    this.ensureConnected();

    try {
      // Create collections and indexes
      for (const [collectionName, collectionConfig] of Object.entries(
        schema.collections
      )) {
        this.logger.debug("Processing collection", {
          databaseName: this.databaseName,
          collectionName,
        });
        const collection = this.getCollection(collectionName);

        // Ensure collection exists
        this.logger.trace("Checking if collection exists", {
          databaseName: this.databaseName,
          collectionName,
        });
        await collection.findOne({}, { limit: 1 });

        // Create indexes if specified
        if (collectionConfig.indexes?.length) {
          this.logger.debug("Creating indexes for collection", {
            databaseName: this.databaseName,
            collectionName,
            indexCount: collectionConfig.indexes.length,
          });
          await this.createIndexesForCollection(
            collectionName,
            collectionConfig.indexes
          );
        }

        // Apply validation rules if specified
        if (collectionConfig.validation) {
          this.logger.debug("Applying validation rules", {
            databaseName: this.databaseName,
            collectionName,
          });
          await this.connection!.db.command({
            collMod: collectionName,
            validator: collectionConfig.validation,
            validationLevel: "moderate",
            validationAction: "warn",
          });
        }
      }

      // Store schema version
      this.logger.debug("Storing schema version", {
        databaseName: this.databaseName,
        version: schema.version,
      });
      await this.setSchemaVersion(schema.version);
      this.logger.info("MongoDB schema initialized successfully", {
        databaseName: this.databaseName,
        schemaVersion: schema.version,
      });
    } catch (error) {
      this.logger.error("Schema initialization failed", {
        databaseName: this.databaseName,
        schemaVersion: schema.version,
        error: (error as Error).message,
      });
      throw new Error(
        `Schema initialization failed: ${(error as Error).message}`
      );
    }
  }

  private async createIndexesForCollection(
    collectionName: string,
    indexes: MongoIndexDefinition[]
  ): Promise<void> {
    this.logger.debug("Creating indexes for collection", {
      databaseName: this.databaseName,
      collectionName,
      indexCount: indexes.length,
    });
    const collection = this.getCollection(collectionName);

    for (const index of indexes) {
      this.logger.trace("Creating index", {
        databaseName: this.databaseName,
        collectionName,
        indexName: index.name,
      });
      try {
        await collection.createIndex(index.keys, {
          name: index.name,
          ...index.options,
        });
        this.logger.trace("Index created successfully", {
          databaseName: this.databaseName,
          collectionName,
          indexName: index.name,
        });
      } catch (error) {
        // Index might already exist, continue with others
        this.logger.warn(`Failed to create index ${index.name}`, {
          databaseName: this.databaseName,
          collectionName,
          error: (error as Error).message,
        });
      }
    }
  }

  async getSchemaVersion(): Promise<string> {
    this.logger.debug("Retrieving schema version", { databaseName: this.databaseName });
    try {
      const collection = this.getCollection("_schema_info");
      const result = await collection.findOne({}, { sort: { applied_at: -1 } });
      const version = result?.version || "0";
      this.logger.trace("Schema version retrieved", {
        databaseName: this.databaseName,
        version,
      });
      return version;
    } catch (error) {
      this.logger.warn("Failed to retrieve schema version, returning default", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      return "0";
    }
  }

  async setSchemaVersion(version: string): Promise<void> {
    this.logger.debug("Setting schema version", {
      databaseName: this.databaseName,
      version,
    });
    try {
      const collection = this.getCollection("_schema_info");
      await collection.insertOne({
        version,
        applied_at: new Date(),
      });
      this.logger.trace("Schema version set", {
        databaseName: this.databaseName,
        version,
      });
    } catch (error) {
      this.logger.error("Failed to set schema version", {
        databaseName: this.databaseName,
        version,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  // ========================== HELPER METHODS FOR SCHEMA MIGRATION ==========================

  /**
   * Transform document before insertion based on column definitions
   */
  private transformDocument(
    document: Record<string, any>,
    columns: ColumnDefinition[]
  ): Record<string, any> {
    this.logger.trace("Transforming document for insertion", {
      databaseName: this.databaseName,
      columnCount: columns.length,
    });
    const transformed: Record<string, any> = {};

    for (const [key, value] of Object.entries(document)) {
      const column = columns.find((col) => col.name === key);
      this.logger.trace("Processing document field", {
        databaseName: this.databaseName,
        fieldName: key,
      });
      if (!column) {
        // Pass through unknown columns
        transformed[key] = value;
        this.logger.trace("Unknown column, passing through", {
          databaseName: this.databaseName,
          fieldName: key,
        });
        continue;
      }

      const fieldName = key === "id" && column.primary_key ? "_id" : key;

      // Transform based on type
      if (value !== null && value !== undefined) {
        switch (this.mapTypeToMongo(column.type)) {
          case "ObjectId":
            transformed[fieldName] =
              typeof value === "string" ? new ObjectId(value) : value;
            this.logger.trace("Transformed field to ObjectId", {
              databaseName: this.databaseName,
              fieldName,
            });
            break;
          case "Date":
            transformed[fieldName] =
              value instanceof Date ? value : new Date(value);
            this.logger.trace("Transformed field to Date", {
              databaseName: this.databaseName,
              fieldName,
            });
            break;
          case "Number":
            transformed[fieldName] =
              typeof value === "number" ? value : Number(value);
            this.logger.trace("Transformed field to Number", {
              databaseName: this.databaseName,
              fieldName,
            });
            break;
          case "Boolean":
            transformed[fieldName] =
              typeof value === "boolean" ? value : Boolean(value);
            this.logger.trace("Transformed field to Boolean", {
              databaseName: this.databaseName,
              fieldName,
            });
            break;
          case "Object":
          case "Array":
            transformed[fieldName] =
              typeof value === "string" ? JSON.parse(value) : value;
            this.logger.trace("Transformed field to Object/Array", {
              databaseName: this.databaseName,
              fieldName,
            });
            break;
          default:
            transformed[fieldName] = value;
            this.logger.trace("No transformation needed for field", {
              databaseName: this.databaseName,
              fieldName,
            });
        }
      } else {
        transformed[fieldName] = value;
      }
    }

    this.logger.debug("Document transformation completed", {
      databaseName: this.databaseName,
      fieldCount: Object.keys(transformed).length,
    });
    return transformed;
  }

  /**
   * Get collection schema information
   */
  async getCollectionSchema(collectionName: string): Promise<any> {
    this.logger.debug("Retrieving collection schema", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const collectionInfo: any = await this.connection!.db.listCollections({
        name: collectionName,
      }).next();

      const result = {
        name: collectionName,
        options: collectionInfo?.options || {},
        validator: collectionInfo?.options?.validator || null,
        indexes: await collection.indexes(),
      };
      this.logger.info("Collection schema retrieved successfully", {
        databaseName: this.databaseName,
        collectionName,
      });
      return result;
    } catch (error) {
      this.logger.error("Failed to retrieve collection schema", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
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
    this.logger.debug("Inserting document", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.insertOne(document, {
        session: this.currentSession || undefined,
      });

      const mongoResult = {
        rows: [{ _id: result.insertedId, ...document }],
        rowsAffected: result.acknowledged ? 1 : 0,
        lastInsertId: result.insertedId,
      };
      this.logger.info("Document inserted successfully", {
        databaseName: this.databaseName,
        collectionName,
        insertedId: result.insertedId.toString(),
      });
      return mongoResult;
    } catch (error) {
      this.logger.error("Insert failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Insert failed: ${(error as Error).message}`);
    }
  }

  async insertMany(
    collectionName: string,
    documents: Record<string, any>[]
  ): Promise<MongoResult> {
    this.logger.debug("Inserting multiple documents", {
      databaseName: this.databaseName,
      collectionName,
      documentCount: documents.length,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.insertMany(documents, {
        session: this.currentSession || undefined,
      });

      const mongoResult = {
        rows: documents.map((doc, index) => ({
          _id: result.insertedIds[index],
          ...doc,
        })),
        rowsAffected: result.insertedCount,
        insertedIds: Object.values(result.insertedIds) as ObjectId[],
      };
      this.logger.info("Multiple documents inserted successfully", {
        databaseName: this.databaseName,
        collectionName,
        insertedCount: result.insertedCount,
      });
      return mongoResult;
    } catch (error) {
      this.logger.error("Insert many failed", {
        databaseName: this.databaseName,
        collectionName,
        documentCount: documents.length,
        error: (error as Error).message,
      });
      throw new Error(`Insert many failed: ${(error as Error).message}`);
    }
  }

  async update(
    collectionName: string,
    filter: Record<string, any>,
    update: Record<string, any>,
    options?: { upsert?: boolean; multi?: boolean }
  ): Promise<MongoResult> {
    this.logger.debug("Updating documents", {
      databaseName: this.databaseName,
      collectionName,
      upsert: options?.upsert,
      multi: options?.multi,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);

      // Prepare update document
      this.logger.trace("Preparing update document", { databaseName: this.databaseName, collectionName });
      const updateDoc = this.prepareUpdateDocument(update);

      let result;
      if (options?.multi) {
        this.logger.trace("Performing multi update", { databaseName: this.databaseName, collectionName });
        result = await collection.updateMany(filter, updateDoc, {
          upsert: options?.upsert,
          session: this.currentSession || undefined,
        });
      } else {
        this.logger.trace("Performing single update", { databaseName: this.databaseName, collectionName });
        result = await collection.updateOne(filter, updateDoc, {
          upsert: options?.upsert,
          session: this.currentSession || undefined,
        });
      }

      const mongoResult = {
        rows: [],
        rowsAffected: result.modifiedCount + (result.upsertedCount || 0),
      };
      this.logger.info("Documents updated successfully", {
        databaseName: this.databaseName,
        collectionName,
        modifiedCount: result.modifiedCount,
        upsertedCount: result.upsertedCount || 0,
      });
      if (result.modifiedCount === 0 && !result.upsertedCount) {
        this.logger.warn("No documents were updated", {
          databaseName: this.databaseName,
          collectionName,
        });
      }
      return mongoResult;
    } catch (error) {
      this.logger.error("Update failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Update failed: ${(error as Error).message}`);
    }
  }

  private prepareUpdateDocument(
    update: Record<string, any>
  ): Record<string, any> {
    this.logger.trace("Preparing update document", { databaseName: this.databaseName });
    // If update contains operators, return as-is
    const hasOperators = Object.keys(update).some((key) => key.startsWith("$"));
    if (hasOperators) {
      this.logger.trace("Update document contains operators", { databaseName: this.databaseName });
      return update;
    }

    // Otherwise, wrap in $set
    this.logger.trace("Wrapping update in $set operator", { databaseName: this.databaseName });
    return { $set: update };
  }

  async delete(
    collectionName: string,
    filter: Record<string, any>,
    options?: { multi?: boolean }
  ): Promise<MongoResult> {
    this.logger.debug("Deleting documents", {
      databaseName: this.databaseName,
      collectionName,
      multi: options?.multi,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);

      let result;
      if (options?.multi) {
        this.logger.trace("Performing multi delete", { databaseName: this.databaseName, collectionName });
        result = await collection.deleteMany(filter, {
          session: this.currentSession || undefined,
        });
      } else {
        this.logger.trace("Performing single delete", { databaseName: this.databaseName, collectionName });
        result = await collection.deleteOne(filter, {
          session: this.currentSession || undefined,
        });
      }

      const mongoResult = {
        rows: [],
        rowsAffected: result.deletedCount,
      };
      this.logger.info("Documents deleted successfully", {
        databaseName: this.databaseName,
        collectionName,
        deletedCount: result.deletedCount,
      });
      if (result.deletedCount === 0) {
        this.logger.warn("No documents were deleted", {
          databaseName: this.databaseName,
          collectionName,
        });
      }
      return mongoResult;
    } catch (error) {
      this.logger.error("Delete failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Delete failed: ${(error as Error).message}`);
    }
  }

  async findOne(
    collectionName: string,
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<any> {
    this.logger.debug("Finding one document", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const result = await collection.findOne(filter, {
        sort: options?.sort,
        projection: options?.projection,
        session: this.currentSession || undefined,
      });

      this.logger.info("Find one completed", {
        databaseName: this.databaseName,
        collectionName,
        found: !!result,
      });
      return result;
    } catch (error) {
      this.logger.error("Find one failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Find one failed: ${(error as Error).message}`);
    }
  }

  async find(
    collectionName: string,
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<any[]> {
    this.logger.debug("Finding documents", {
      databaseName: this.databaseName,
      collectionName,
      skip: options?.skip,
      limit: options?.limit,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      let cursor = collection.find(filter, {
        projection: options?.projection,
        session: this.currentSession || undefined,
      });

      if (options?.sort) {
        this.logger.trace("Applying sort to query", {
          databaseName: this.databaseName,
          collectionName,
          sort: options.sort,
        });
        cursor = cursor.sort(options.sort);
      }

      if (options?.skip) {
        this.logger.trace("Applying skip to query", {
          databaseName: this.databaseName,
          collectionName,
          skip: options.skip,
        });
        cursor = cursor.skip(options.skip);
      }

      if (options?.limit) {
        this.logger.trace("Applying limit to query", {
          databaseName: this.databaseName,
          collectionName,
          limit: options.limit,
        });
        cursor = cursor.limit(options.limit);
      }

      const results = await cursor.toArray();
      this.logger.info("Find completed", {
        databaseName: this.databaseName,
        collectionName,
        resultCount: results.length,
      });
      return results;
    } catch (error) {
      this.logger.error("Find failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Find failed: ${(error as Error).message}`);
    }
  }

  async count(
    collectionName: string,
    filter: Record<string, any> = {}
  ): Promise<number> {
    this.logger.debug("Counting documents", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const count = await collection.countDocuments(filter, {
        session: this.currentSession || undefined,
      });

      this.logger.info("Count completed", {
        databaseName: this.databaseName,
        collectionName,
        count,
      });
      return count;
    } catch (error) {
      this.logger.error("Count failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Count failed: ${(error as Error).message}`);
    }
  }

  // ========================== AGGREGATION ==========================
  async aggregate(
    collectionName: string,
    pipeline: Record<string, any>[],
    options?: { allowDiskUse?: boolean }
  ): Promise<any[]> {
    this.logger.debug("Executing aggregation", {
      databaseName: this.databaseName,
      collectionName,
      pipelineLength: pipeline.length,
      allowDiskUse: options?.allowDiskUse,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const cursor = collection.aggregate(pipeline, {
        allowDiskUse: options?.allowDiskUse,
        session: this.currentSession || undefined,
      });

      const results = await cursor.toArray();
      this.logger.info("Aggregation completed", {
        databaseName: this.databaseName,
        collectionName,
        resultCount: results.length,
      });
      return results;
    } catch (error) {
      this.logger.error("Aggregation failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Aggregation failed: ${(error as Error).message}`);
    }
  }

  // ========================== UTILITY METHODS ==========================
  async getDatabaseInfo(): Promise<any> {
    this.logger.debug("Retrieving database info", { databaseName: this.databaseName });
    this.ensureConnected();

    try {
      const admin = this.connection!.db.admin();
      const collections = await this.connection!.db.listCollections().toArray();
      const stats = await this.connection!.db.stats();

      const result = {
        name: this.databaseName,
        collections: collections.map((c) => c.name),
        isConnected: this.isConnected,
        stats,
        version: await this.getSchemaVersion(),
      };
      this.logger.info("Database info retrieved successfully", {
        databaseName: this.databaseName,
        collectionCount: collections.length,
      });
      return result;
    } catch (error) {
      this.logger.error("Failed to retrieve database info", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw new Error(`Get database info failed: ${(error as Error).message}`);
    }
  }

  async getCollectionInfo(collectionName: string): Promise<any> {
    this.logger.debug("Retrieving collection info", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const indexes = await collection.indexes();

      const result = {
        name: collectionName,
        indexes,
      };
      this.logger.info("Collection info retrieved successfully", {
        databaseName: this.databaseName,
        collectionName,
        indexCount: indexes.length,
      });
      return result;
    } catch (error) {
      this.logger.error("Failed to retrieve collection info", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(
        `Get collection info failed: ${(error as Error).message}`
      );
    }
  }

  async dropCollection(collectionName: string): Promise<void> {
    this.logger.debug("Dropping collection", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      await this.connection!.db.dropCollection(collectionName);
      this.logger.info("Collection dropped successfully", {
        databaseName: this.databaseName,
        collectionName,
      });
    } catch (error) {
      this.logger.error("Failed to drop collection", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Drop collection failed: ${(error as Error).message}`);
    }
  }

  // ========================== MIGRATION HELPER METHODS ==========================

  /**
   * Get all collections in database
   */
  async getCollectionNames(): Promise<string[]> {
    this.logger.debug("Retrieving collection names", { databaseName: this.databaseName });
    this.ensureConnected();

    try {
      const collections = await this.connection!.db.listCollections().toArray();
      const collectionNames = collections
        .map((c) => c.name)
        .filter((name) => !name.startsWith("_"));
      this.logger.info("Collection names retrieved successfully", {
        databaseName: this.databaseName,
        collectionCount: collectionNames.length,
      });
      return collectionNames;
    } catch (error) {
      this.logger.error("Failed to retrieve collection names", {
        databaseName: this.databaseName,
        error: (error as Error).message,
      });
      throw new Error(
        `Get collection names failed: ${(error as Error).message}`
      );
    }
  }

  /**
   * Check if collection exists
   */
  async collectionExists(collectionName: string): Promise<boolean> {
    this.logger.debug("Checking if collection exists", {
      databaseName: this.databaseName,
      collectionName,
    });
    this.ensureConnected();

    try {
      const collections = await this.connection!.db.listCollections({
        name: collectionName,
      }).toArray();
      const exists = collections.length > 0;
      this.logger.trace("Collection existence check completed", {
        databaseName: this.databaseName,
        collectionName,
        exists,
      });
      return exists;
    } catch (error) {
      this.logger.warn("Error checking collection existence, returning false", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
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
    this.logger.debug("Creating collection", {
      databaseName: this.databaseName,
      collectionName,
      hasValidation: !!validation,
    });
    this.ensureConnected();

    try {
      const options: any = {};
      if (validation) {
        options.validator = validation;
        options.validationLevel = "moderate";
        options.validationAction = "warn";
        this.logger.trace("Applying validation schema to collection", {
          databaseName: this.databaseName,
          collectionName,
        });
      }

      await this.connection!.db.createCollection(collectionName, options);
      this.logger.info("Collection created successfully", {
        databaseName: this.databaseName,
        collectionName,
      });
    } catch (error) {
      this.logger.error("Failed to create collection", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
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
    this.logger.debug("Performing bulk insert", {
      databaseName: this.databaseName,
      collectionName,
      documentCount: documents.length,
      batchSize,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      const results: any[] = [];
      let totalInserted = 0;

      for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        this.logger.trace("Processing bulk insert batch", {
          databaseName: this.databaseName,
          collectionName,
          batchNumber: Math.floor(i / batchSize) + 1,
          batchSize: batch.length,
        });
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
        this.logger.trace("Batch inserted", {
          databaseName: this.databaseName,
          collectionName,
          batchNumber: Math.floor(i / batchSize) + 1,
          insertedCount: result.insertedCount,
        });
      }

      const mongoResult = {
        rows: results,
        rowsAffected: totalInserted,
        insertedIds: results.map((r) => r._id),
      };
      this.logger.info("Bulk insert completed successfully", {
        databaseName: this.databaseName,
        collectionName,
        totalInserted,
      });
      return mongoResult;
    } catch (error) {
      this.logger.error("Bulk insert failed", {
        databaseName: this.databaseName,
        collectionName,
        documentCount: documents.length,
        error: (error as Error).message,
      });
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
    this.logger.debug("Exporting collection data", {
      databaseName: this.databaseName,
      collectionName,
      limit: options?.limit,
    });
    this.ensureConnected();

    try {
      const collection = this.getCollection(collectionName);
      let cursor = collection.find(options?.filter || {});

      if (options?.projection) {
        this.logger.trace("Applying projection to export", {
          databaseName: this.databaseName,
          collectionName,
          projection: options.projection,
        });
        cursor = cursor.project(options.projection);
      }

      if (options?.sort) {
        this.logger.trace("Applying sort to export", {
          databaseName: this.databaseName,
          collectionName,
          sort: options.sort,
        });
        cursor = cursor.sort(options.sort);
      }

      if (options?.limit) {
        this.logger.trace("Applying limit to export", {
          databaseName: this.databaseName,
          collectionName,
          limit: options.limit,
        });
        cursor = cursor.limit(options.limit);
      }

      const results = await cursor.toArray();
      this.logger.info("Collection data exported successfully", {
        databaseName: this.databaseName,
        collectionName,
        exportedCount: results.length,
      });
      return results;
    } catch (error) {
      this.logger.error("Export collection failed", {
        databaseName: this.databaseName,
        collectionName,
        error: (error as Error).message,
      });
      throw new Error(`Export collection failed: ${(error as Error).message}`);
    }
  }

  // ========================== HELPER METHODS ==========================
  createObjectId(id?: string): ObjectId {
    this.logger.trace("Creating ObjectId", {
      databaseName: this.databaseName,
      idProvided: !!id,
    });
    return id ? new ObjectId(id) : new ObjectId();
  }

  isValidObjectId(id: string): boolean {
    this.logger.trace("Checking ObjectId validity", {
      databaseName: this.databaseName,
      id,
    });
    const isValid = ObjectId.isValid(id);
    this.logger.trace("ObjectId validity check result", {
      databaseName: this.databaseName,
      id,
      isValid,
    });
    return isValid;
  }

  /**
   * Convert MongoDB document to SQLite-compatible format
   */
  mongoToSQLiteFormat(document: any): Record<string, any> {
    this.logger.trace("Converting MongoDB document to SQLite format", {
      databaseName: this.databaseName,
    });
    if (!document) {
      this.logger.debug("No document to convert", { databaseName: this.databaseName });
      return document;
    }

    const converted = { ...document };

    // Convert _id to id
    if (converted._id) {
      converted.id = converted._id.toString();
      delete converted._id;
      this.logger.trace("Converted _id to id", { databaseName: this.databaseName });
    }

    // Convert ObjectIds to strings
    for (const [key, value] of Object.entries(converted)) {
      if (value instanceof ObjectId) {
        converted[key] = value.toString();
        this.logger.trace("Converted ObjectId to string", {
          databaseName: this.databaseName,
          fieldName: key,
        });
      } else if (value instanceof Date) {
        converted[key] = value.toISOString();
        this.logger.trace("Converted Date to ISO string", {
          databaseName: this.databaseName,
          fieldName: key,
        });
      } else if (
        typeof value === "object" &&
        value !== null &&
        !Array.isArray(value)
      ) {
        converted[key] = JSON.stringify(value);
        this.logger.trace("Converted object to JSON string", {
          databaseName: this.databaseName,
          fieldName: key,
        });
      } else if (Array.isArray(value)) {
        converted[key] = JSON.stringify(value);
        this.logger.trace("Converted array to JSON string", {
          databaseName: this.databaseName,
          fieldName: key,
        });
      }
    }

    this.logger.debug("MongoDB to SQLite conversion completed", {
      databaseName: this.databaseName,
      fieldCount: Object.keys(converted).length,
    });
    return converted;
  }

  /**
   * Convert SQLite record to MongoDB-compatible format
   */
  sqliteToMongoFormat(record: Record<string, any>): Record<string, any> {
    this.logger.trace("Converting SQLite record to MongoDB format", {
      databaseName: this.databaseName,
    });
    if (!record) {
      this.logger.debug("No record to convert", { databaseName: this.databaseName });
      return record;
    }

    const converted = { ...record };

    // Convert id to _id if it's a valid ObjectId
    if (converted.id) {
      if (ObjectId.isValid(converted.id)) {
        converted._id = new ObjectId(converted.id);
        this.logger.trace("Converted id to _id (ObjectId)", {
          databaseName: this.databaseName,
        });
      } else {
        converted._id = converted.id;
        this.logger.trace("Converted id to _id (non-ObjectId)", {
          databaseName: this.databaseName,
        });
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
            this.logger.trace("Parsed string to Date", {
              databaseName: this.databaseName,
              fieldName: key,
            });
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
            this.logger.trace("Parsed string to JSON", {
              databaseName: this.databaseName,
              fieldName: key,
            });
          } catch {
            // Keep as string if parsing fails
            this.logger.trace("Failed to parse JSON string, keeping as string", {
              databaseName: this.databaseName,
              fieldName: key,
            });
          }
        }
      }
    }

    this.logger.debug("SQLite to MongoDB conversion completed", {
      databaseName: this.databaseName,
      fieldCount: Object.keys(converted).length,
    });
    return converted;
  }

  /**
   * Sanitize field names for MongoDB (remove problematic characters)
   */
  sanitizeFieldName(fieldName: string): string {
    this.logger.trace("Sanitizing field name", {
      databaseName: this.databaseName,
      fieldName,
    });
    // MongoDB field names cannot contain dots, dollar signs, or null characters
    const sanitized = fieldName
      .replace(/\./g, "_")
      .replace(/\$/g, "_")
      .replace(/\x00/g, "");
    this.logger.trace("Field name sanitized", {
      databaseName: this.databaseName,
      original: fieldName,
      sanitized,
    });
    return sanitized;
  }

  /**
   * Build MongoDB query from SQLite-style where clauses
   */
  buildMongoQuery(
    wheres: Array<{ name: string; value: any; operator?: string }>
  ): Record<string, any> {
    this.logger.trace("Building MongoDB query from where clauses", {
      databaseName: this.databaseName,
      whereCount: wheres.length,
    });
    const query: Record<string, any> = {};

    for (const where of wheres) {
      const fieldName = where.name === "id" ? "_id" : where.name;
      const operator = where.operator || "=";
      let value = where.value;

      this.logger.trace("Processing where clause", {
        databaseName: this.databaseName,
        fieldName,
        operator,
      });

      // Convert string to ObjectId if field is _id
      if (
        fieldName === "_id" &&
        typeof value === "string" &&
        ObjectId.isValid(value)
      ) {
        value = new ObjectId(value);
        this.logger.trace("Converted value to ObjectId for _id", {
          databaseName: this.databaseName,
          fieldName,
        });
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
          this.logger.trace("Converted LIKE to regex", {
            databaseName: this.databaseName,
            fieldName,
            regexPattern,
          });
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
          this.logger.trace("Applied default query operator", {
            databaseName: this.databaseName,
            fieldName,
          });
      }
    }

    this.logger.debug("MongoDB query built", {
      databaseName: this.databaseName,
      queryFieldCount: Object.keys(query).length,
    });
    return query;
  }

  /**
   * Convert SQLite ORDER BY to MongoDB sort
   */
  buildMongoSort(
    orderBys: Array<{ name: string; direction?: "ASC" | "DESC" }>
  ): Record<string, 1 | -1> {
    this.logger.trace("Building MongoDB sort from order by clauses", {
      databaseName: this.databaseName,
      orderByCount: orderBys.length,
    });
    const sort: Record<string, 1 | -1> = {};

    for (const orderBy of orderBys) {
      const fieldName = orderBy.name === "id" ? "_id" : orderBy.name;
      sort[fieldName] = orderBy.direction === "DESC" ? -1 : 1;
      this.logger.trace("Processed sort field", {
        databaseName: this.databaseName,
        fieldName,
        direction: orderBy.direction || "ASC",
      });
    }

    this.logger.debug("MongoDB sort built", {
      databaseName: this.databaseName,
      sortFieldCount: Object.keys(sort).length,
    });
    return sort;
  }

  // Enhanced Data Import functionality for MongoDB
  async importData(options: MongoImportOptions): Promise<ImportResult> {
    this.logger.info("Starting MongoDB data import operation", {
      databaseName: this.databaseName,
      collectionName: options.collectionName,
      totalRows: options.data.length,
      batchSize: options.batchSize || 1000,
      validateData: options.validateData,
      updateOnConflict: options.updateOnConflict || false,
      skipErrors: options.skipErrors || false,
    });

    const startTime = Date.now();
    const result: ImportResult = {
      totalRows: options.data.length,
      successRows: 0,
      errorRows: 0,
      errors: [],
      executionTime: 0,
    };

    if (!this.isConnected) {
      const error = new Error("Database is not connected");
      this.logger.error("Import failed - database not connected", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
      });
      throw error;
    }

    if (!options.data || options.data.length === 0) {
      this.logger.warn("No data provided for import, returning empty result", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
      });
      result.executionTime = Date.now() - startTime;
      return result;
    }

    // Check if collection exists
    this.logger.debug("Checking if collection exists", {
      databaseName: this.databaseName,
      collectionName: options.collectionName,
    });
    const collectionExists = await this.collectionExists(
      options.collectionName
    );
    if (!collectionExists) {
      this.logger.info("Collection does not exist, creating it", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
      });
      await this.createCollection(options.collectionName);
    }

    const batchSize = options.batchSize || 1000;
    let processedCount = 0;

    try {
      if (options.useTransaction !== false) {
        this.logger.debug("Starting transaction for import", {
          databaseName: this.databaseName,
          collectionName: options.collectionName,
        });
        await this.beginTransaction();
      }

      for (let i = 0; i < options.data.length; i += batchSize) {
        const batch = options.data.slice(i, i + batchSize);
        this.logger.debug("Processing import batch", {
          databaseName: this.databaseName,
          collectionName: options.collectionName,
          batchNumber: Math.floor(i / batchSize) + 1,
          batchSize: batch.length,
          totalBatches: Math.ceil(options.data.length / batchSize),
        });

        for (let j = 0; j < batch.length; j++) {
          const rowIndex = i + j;
          const rowData = batch[j];

          try {
            this.logger.trace("Processing row", {
              databaseName: this.databaseName,
              collectionName: options.collectionName,
              rowIndex,
            });
            let processedData = options.validateData
              ? this.validateAndTransformMongoRow(
                  rowData,
                  options.collectionName
                )
              : this.transformMongoRowData(rowData);

            // Convert SQLite format to MongoDB format if needed
            this.logger.trace("Converting row to MongoDB format", {
              databaseName: this.databaseName,
              collectionName: options.collectionName,
              rowIndex,
            });
            processedData = this.sqliteToMongoFormat(processedData);

            if (options.updateOnConflict && options.conflictColumns) {
              this.logger.trace("Attempting insert or update", {
                databaseName: this.databaseName,
                collectionName: options.collectionName,
                rowIndex,
              });
              await this.insertOrUpdateMongo(
                options.collectionName,
                processedData,
                options.conflictColumns
              );
            } else {
              await this.insert(options.collectionName, processedData);
            }

            result.successRows++;
            this.logger.trace("Row imported successfully", {
              databaseName: this.databaseName,
              collectionName: options.collectionName,
              rowIndex,
            });
          } catch (error) {
            result.errorRows++;
            const errorInfo = {
              rowIndex,
              error: error instanceof Error ? error.message : String(error),
              rowData,
            };
            result.errors.push(errorInfo);

            this.logger.warn("Row import failed", {
              databaseName: this.databaseName,
              collectionName: options.collectionName,
              rowIndex,
              error: error instanceof Error ? error.message : String(error),
            });

            if (options.onError) {
              this.logger.trace("Invoking onError callback", {
                databaseName: this.databaseName,
                collectionName: options.collectionName,
                rowIndex,
              });
              options.onError(
                error instanceof Error ? error : new Error(String(error)),
                rowIndex,
                rowData
              );
            }

            if (!options.skipErrors) {
              this.logger.error("Import operation stopped due to error and skipErrors=false", {
                databaseName: this.databaseName,
                collectionName: options.collectionName,
                rowIndex,
              });
              throw error;
            }
          }

          processedCount++;
          if (options.onProgress && processedCount % 100 === 0) {
            this.logger.trace("Reporting progress", {
              databaseName: this.databaseName,
              collectionName: options.collectionName,
              processedCount,
              totalCount: options.data.length,
            });
            options.onProgress(processedCount, options.data.length);
          }
        }
      }

      if (options.useTransaction !== false) {
        this.logger.debug("Committing import transaction", {
          databaseName: this.databaseName,
          collectionName: options.collectionName,
        });
        await this.commitTransaction();
      }

      this.logger.info("MongoDB data import completed successfully", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
        totalRows: result.totalRows,
        successRows: result.successRows,
        errorRows: result.errorRows,
        executionTime: Date.now() - startTime,
      });
    } catch (error) {
      this.logger.error("Import operation failed, rolling back transaction", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
        processedCount,
        error: error instanceof Error ? error.message : String(error),
      });

      if (options.useTransaction !== false) {
        await this.rollbackTransaction();
      }
      throw error;
    }

    if (options.onProgress) {
      this.logger.trace("Reporting final progress", {
        databaseName: this.databaseName,
        collectionName: options.collectionName,
        processedCount,
        totalCount: options.data.length,
      });
      options.onProgress(processedCount, options.data.length);
    }

    result.executionTime = Date.now() - startTime;
    return result;
  }

  // Import with column mapping for MongoDB
  async importDataWithMapping(
    collectionName: string,
    data: Record<string, any>[],
    columnMappings: ColumnMapping[],
    options: Partial<MongoImportOptions> = {}
  ): Promise<ImportResult> {
    this.logger.info("Starting MongoDB data import with column mapping", {
      databaseName: this.databaseName,
      collectionName,
      dataRows: data.length,
      mappingCount: columnMappings.length,
    });

    const transformedData = data.map((row, index) => {
      this.logger.trace("Transforming row with column mappings", {
        databaseName: this.databaseName,
        collectionName,
        rowIndex: index,
      });
      const newRow: Record<string, any> = {};

      columnMappings.forEach((mapping) => {
        if (row.hasOwnProperty(mapping.sourceColumn)) {
          let value = row[mapping.sourceColumn];

          if (mapping.transform) {
            this.logger.trace("Applying transformation to column", {
              databaseName: this.databaseName,
              collectionName,
              rowIndex: index,
              sourceColumn: mapping.sourceColumn,
            });
            try {
              value = mapping.transform(value);
            } catch (error) {
              this.logger.warn("Column transformation failed", {
                databaseName: this.databaseName,
                collectionName,
                rowIndex: index,
                sourceColumn: mapping.sourceColumn,
                targetColumn: mapping.targetColumn,
                error: error instanceof Error ? error.message : String(error),
              });
            }
          }

          newRow[mapping.targetColumn] = value;
        }
      });

      return newRow;
    });

    this.logger.debug("Data transformation completed", {
      databaseName: this.databaseName,
      collectionName,
      originalRowCount: data.length,
      transformedRowCount: transformedData.length,
    });

    return await this.importData({
      collectionName,
      data: transformedData,
      ...options,
    });
  }

  // Import from CSV for MongoDB
  async importFromCSV(
    collectionName: string,
    csvData: string,
    options: {
      delimiter?: string;
      hasHeader?: boolean;
      columnMappings?: ColumnMapping[];
    } & Partial<MongoImportOptions> = {}
  ): Promise<ImportResult> {
    this.logger.info("Starting MongoDB CSV import", {
      databaseName: this.databaseName,
      collectionName,
      csvLength: csvData.length,
      delimiter: options.delimiter || ",",
      hasHeader: options.hasHeader !== false,
    });

    const delimiter = options.delimiter || ",";
    const hasHeader = options.hasHeader !== false;

    const lines = csvData.split("\n").filter((line) => line.trim());
    if (lines.length === 0) {
      const error = new Error("CSV data is empty");
      this.logger.error("CSV import failed - empty data", {
        databaseName: this.databaseName,
        collectionName,
      });
      throw error;
    }

    let headers: string[] = [];
    let dataStartIndex = 0;

    if (hasHeader) {
      headers = lines[0]
        .split(delimiter)
        .map((h) => h.trim().replace(/^["']|["']$/g, ""));
      dataStartIndex = 1;
      this.logger.debug("CSV headers extracted", {
        databaseName: this.databaseName,
        collectionName,
        headers,
        headerCount: headers.length,
      });
    } else {
      const firstRowCols = lines[0].split(delimiter).length;
      headers = Array.from(
        { length: firstRowCols },
        (_, i) => `column_${i + 1}`
      );
      this.logger.debug("Generated column headers for headerless CSV", {
        databaseName: this.databaseName,
        collectionName,
        columnCount: firstRowCols,
        headers,
      });
    }

    const data: Record<string, any>[] = [];
    for (let i = dataStartIndex; i < lines.length; i++) {
      const values = lines[i]
        .split(delimiter)
        .map((v) => v.trim().replace(/^["']|["']$/g, ""));
      const row: Record<string, any> = {};

      this.logger.trace("Processing CSV row", {
        databaseName: this.databaseName,
        collectionName,
        rowIndex: i,
      });
      headers.forEach((header, index) => {
        let value: any = values[index] || null;

        // Auto-detect and convert data types
        if (value !== null && value !== "") {
          // Try to parse as number
          if (!isNaN(Number(value)) && value.toString().trim() !== "") {
            value = Number(value);
            this.logger.trace("Converted CSV value to Number", {
              databaseName: this.databaseName,
              collectionName,
              header,
              rowIndex: i,
            });
          }
          // Try to parse as boolean
          else if (
            value.toLowerCase() === "true" ||
            value.toLowerCase() === "false"
          ) {
            value = value.toLowerCase() === "true";
            this.logger.trace("Converted CSV value to Boolean", {
              databaseName: this.databaseName,
              collectionName,
              header,
              rowIndex: i,
            });
          }
          // Try to parse as date
          else if (
            value.match(/^\d{4}-\d{2}-\d{2}/) ||
            value.match(/^\d{2}\/\d{2}\/\d{4}/)
          ) {
            const dateValue = new Date(value);
            if (!isNaN(dateValue.getTime())) {
              value = dateValue;
              this.logger.trace("Converted CSV value to Date", {
                databaseName: this.databaseName,
                collectionName,
                header,
                rowIndex: i,
              });
            }
          }
        }

        // Sanitize field names for MongoDB
        const sanitizedHeader = this.sanitizeFieldName(header);
        row[sanitizedHeader] = value;
      });

      data.push(row);
    }

    this.logger.debug("CSV data parsed", {
      databaseName: this.databaseName,
      collectionName,
      totalLines: lines.length,
      dataRows: data.length,
      skipHeader: hasHeader,
    });

    if (options.columnMappings) {
      this.logger.debug("Using column mappings for CSV import", {
        databaseName: this.databaseName,
        collectionName,
        mappingCount: options.columnMappings.length,
      });
      return await this.importDataWithMapping(
        collectionName,
        data,
        options.columnMappings,
        options
      );
    } else {
      return await this.importData({
        collectionName,
        data,
        ...options,
      });
    }
  }

  // Helper methods for MongoDB import

  private validateAndTransformMongoRow(
    rowData: Record<string, any>,
    collectionName: string
  ): Record<string, any> {
    this.logger.trace("Validating and transforming MongoDB row data", {
      databaseName: this.databaseName,
      collectionName,
    });

    const processedRow: Record<string, any> = {};

    for (const [key, value] of Object.entries(rowData)) {
      const sanitizedKey = this.sanitizeFieldName(key);
      this.logger.trace("Processing row field", {
        databaseName: this.databaseName,
        collectionName,
        fieldName: key,
      });

      if (value !== null && value !== undefined) {
        try {
          processedRow[sanitizedKey] = this.convertValueForMongo(value, key);
          this.logger.trace("Field converted successfully", {
            databaseName: this.databaseName,
            collectionName,
            fieldName: key,
          });
        } catch (error) {
          this.logger.error("Value conversion failed during validation", {
            databaseName: this.databaseName,
            collectionName,
            fieldName: key,
            value,
            error: error instanceof Error ? error.message : String(error),
          });
          throw error;
        }
      } else {
        processedRow[sanitizedKey] = value;
      }
    }

    this.logger.debug("Row validation and transformation completed", {
      databaseName: this.databaseName,
      collectionName,
      fieldCount: Object.keys(processedRow).length,
    });
    return processedRow;
  }

  private transformMongoRowData(
    rowData: Record<string, any>
  ): Record<string, any> {
    this.logger.trace("Transforming MongoDB row data without validation", {
      databaseName: this.databaseName,
    });

    const processedRow: Record<string, any> = {};

    for (const [key, value] of Object.entries(rowData)) {
      const sanitizedKey = this.sanitizeFieldName(key);
      this.logger.trace("Processing row field without validation", {
        databaseName: this.databaseName,
        fieldName: key,
      });

      if (value !== null && value !== undefined) {
        try {
          processedRow[sanitizedKey] = this.convertValueForMongo(value, key);
          this.logger.trace("Field converted successfully", {
            databaseName: this.databaseName,
            fieldName: key,
          });
        } catch (error) {
          this.logger.warn("Value conversion failed during transformation", {
            databaseName: this.databaseName,
            fieldName: key,
            value,
            error: error instanceof Error ? error.message : String(error),
          });
          // Continue processing other columns
          processedRow[sanitizedKey] = value;
        }
      } else {
        processedRow[sanitizedKey] = value;
      }
    }

    this.logger.debug("Row transformation completed", {
      databaseName: this.databaseName,
      fieldCount: Object.keys(processedRow).length,
    });
    return processedRow;
  }

  private convertValueForMongo(value: any, fieldName: string): any {
    this.logger.trace("Converting value for MongoDB", {
      databaseName: this.databaseName,
      fieldName,
    });
    if (value === null || value === undefined) {
      this.logger.trace("Value is null or undefined, returning null", {
        databaseName: this.databaseName,
        fieldName,
      });
      return null;
    }

    // Handle ObjectId fields
    if (fieldName === "id" || fieldName === "_id") {
      if (typeof value === "string" && this.isValidObjectId(value)) {
        this.logger.trace("Converted value to ObjectId", {
          databaseName: this.databaseName,
          fieldName,
        });
        return new ObjectId(value);
      }
      this.logger.trace("Value not converted to ObjectId", {
        databaseName: this.databaseName,
        fieldName,
      });
      return value;
    }

    // Handle dates
    if (value instanceof Date) {
      this.logger.trace("Value is already a Date", {
        databaseName: this.databaseName,
        fieldName,
      });
      return value;
    }

    if (typeof value === "string") {
      // Try to parse as date
      if (value.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)) {
        const date = new Date(value);
        if (!isNaN(date.getTime())) {
          this.logger.trace("Parsed string to Date", {
            databaseName: this.databaseName,
            fieldName,
          });
          return date;
        }
      }

      // Try to parse JSON strings
      if (
        (value.startsWith("{") && value.endsWith("}")) ||
        (value.startsWith("[") && value.endsWith("]"))
      ) {
        try {
          const parsed = JSON.parse(value);
          this.logger.trace("Parsed JSON string", {
            databaseName: this.databaseName,
            fieldName,
          });
          return parsed;
        } catch {
          // Keep as string if parsing fails
          this.logger.trace("Failed to parse JSON string, keeping as string", {
            databaseName: this.databaseName,
            fieldName,
          });
          return value;
        }
      }
    }

    // Handle arrays and objects
    if (Array.isArray(value) || (typeof value === "object" && value !== null)) {
      this.logger.trace("Value is array or object, returning as-is", {
        databaseName: this.databaseName,
        fieldName,
      });
      return value;
    }

    this.logger.trace("No conversion needed for value", {
      databaseName: this.databaseName,
      fieldName,
    });
    return value;
  }

  private async insertOrUpdateMongo(
    collectionName: string,
    data: Record<string, any>,
    conflictColumns: string[]
  ): Promise<void> {
    this.logger.trace("Attempting MongoDB insert or update", {
      databaseName: this.databaseName,
      collectionName,
      conflictColumns,
    });

    try {
      await this.insert(collectionName, data);
      this.logger.trace("Insert successful", {
        databaseName: this.databaseName,
        collectionName,
      });
    } catch (error) {
      if (this.isMongoConflictError(error)) {
        this.logger.debug("Insert conflict detected, attempting update", {
          databaseName: this.databaseName,
          collectionName,
        });
        await this.updateByColumns(collectionName, data, conflictColumns);
      } else {
        this.logger.error("Insert failed", {
          databaseName: this.databaseName,
          collectionName,
          error: error instanceof Error ? error.message : String(error),
        });
        throw error;
      }
    }
  }

  private async updateByColumns(
    collectionName: string,
    data: Record<string, any>,
    conflictColumns: string[]
  ): Promise<void> {
    this.logger.debug("Updating by columns", {
      databaseName: this.databaseName,
      collectionName,
      conflictColumns,
    });
    const filter: Record<string, any> = {};
    const updateData: Record<string, any> = {};

    // Build filter from conflict columns
    for (const col of conflictColumns) {
      const fieldName = col === "id" ? "_id" : col;
      if (data.hasOwnProperty(col)) {
        filter[fieldName] = data[col];
        this.logger.trace("Added filter field", {
          databaseName: this.databaseName,
          collectionName,
          fieldName,
        });
      }
    }

    // Build update data (exclude conflict columns)
    for (const [key, value] of Object.entries(data)) {
      if (!conflictColumns.includes(key)) {
        updateData[key] = value;
        this.logger.trace("Added update field", {
          databaseName: this.databaseName,
          collectionName,
          fieldName: key,
        });
      }
    }

    if (Object.keys(updateData).length === 0) {
      this.logger.debug("No columns to update, skipping update operation", {
        databaseName: this.databaseName,
        collectionName,
      });
      return;
    }

    try {
      await this.update(collectionName, filter, updateData);
      this.logger.info("Update by columns completed", {
        databaseName: this.databaseName,
        collectionName,
        filter,
      });
    } catch (error) {
      this.logger.error("Update by columns failed", {
        databaseName: this.databaseName,
        collectionName,
        filter,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  private isMongoConflictError(error: any): boolean {
    this.logger.trace("Checking for MongoDB conflict error", {
      databaseName: this.databaseName,
    });
    const isConflict =
      error.code === 11000 || // Duplicate key error
      (error.message && error.message.includes("duplicate key")) ||
      (error.message && error.message.includes("E11000"));
    this.logger.trace("Conflict error check result", {
      databaseName: this.databaseName,
      isConflict,
    });
    return isConflict;
  }
}