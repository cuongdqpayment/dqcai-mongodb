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
