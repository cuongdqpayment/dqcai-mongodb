// src/core/universal-dao.ts
import { ClientSession, MongoClientOptions, Collection, ObjectId } from "mongodb";
import { MongoConnection, BaseAdapter, MongoDatabaseSchema, MongoIndexDefinition, MongoResult, MongoQueryOptions } from "../types";

// ========================== MONGODB DAO ==========================
export class MongoUniversalDAO {
  private connection: MongoConnection | null = null;
  private isConnected: boolean = false;
  private currentSession: ClientSession | null = null;
  private inTransaction: boolean = false;
  private adapter: BaseAdapter;
  private connectionString: string;
  private databaseName: string;

  constructor(
    adapter: BaseAdapter,
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
      throw new Error('Database connection is not open. Call connect() first.');
    }
  }

  private getCollection(collectionName: string): Collection {
    this.ensureConnected();
    return this.connection!.db.collection(collectionName);
  }

  // ========================== TRANSACTION MANAGEMENT ==========================
  async beginTransaction(): Promise<void> {
    if (this.inTransaction) {
      throw new Error('Transaction already in progress');
    }

    this.ensureConnected();
    this.currentSession = this.connection!.client.startSession();
    this.currentSession.startTransaction();
    this.inTransaction = true;
  }

  async commitTransaction(): Promise<void> {
    if (!this.inTransaction || !this.currentSession) {
      throw new Error('No transaction in progress');
    }

    await this.currentSession.commitTransaction();
    await this.currentSession.endSession();
    this.currentSession = null;
    this.inTransaction = false;
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.inTransaction || !this.currentSession) {
      throw new Error('No transaction in progress');
    }

    await this.currentSession.abortTransaction();
    await this.currentSession.endSession();
    this.currentSession = null;
    this.inTransaction = false;
  }

  // ========================== SCHEMA MANAGEMENT ==========================
  async initializeFromSchema(schema: MongoDatabaseSchema): Promise<void> {
    this.ensureConnected();

    try {
      // Create collections and indexes
      for (const [collectionName, collectionConfig] of Object.entries(schema.collections)) {
        const collection = this.getCollection(collectionName);
        
        // Ensure collection exists
        await collection.findOne({}, { limit: 1 });

        // Create indexes if specified
        if (collectionConfig.indexes?.length) {
          await this.createIndexesForCollection(collectionName, collectionConfig.indexes);
        }

        // Apply validation rules if specified
        if (collectionConfig.validation) {
          await this.connection!.db.command({
            collMod: collectionName,
            validator: collectionConfig.validation,
            validationLevel: 'moderate',
            validationAction: 'warn'
          });
        }
      }

      // Store schema version
      await this.setSchemaVersion(schema.version);
    } catch (error) {
      throw new Error(`Schema initialization failed: ${(error as Error).message}`);
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
      const collection = this.getCollection('_schema_info');
      const result = await collection.findOne({}, { sort: { applied_at: -1 } });
      return result?.version || '0';
    } catch {
      return '0';
    }
  }

  async setSchemaVersion(version: string): Promise<void> {
    const collection = this.getCollection('_schema_info');
    await collection.insertOne({
      version,
      applied_at: new Date(),
    });
  }

  // ========================== CRUD OPERATIONS ==========================
  async insert(collectionName: string, document: Record<string, any>): Promise<MongoResult> {
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

  private prepareUpdateDocument(update: Record<string, any>): Record<string, any> {
    // If update contains operators, return as-is
    const hasOperators = Object.keys(update).some(key => key.startsWith('$'));
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
        collections: collections.map(c => c.name),
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
      throw new Error(`Get collection info failed: ${(error as Error).message}`);
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

  // ========================== HELPER METHODS ==========================
  createObjectId(id?: string): ObjectId {
    return id ? new ObjectId(id) : new ObjectId();
  }

  isValidObjectId(id: string): boolean {
    return ObjectId.isValid(id);
  }
}