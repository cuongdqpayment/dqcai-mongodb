// src/types.ts
import { MongoClient, Db, Collection, ObjectId, ClientSession, MongoClientOptions } from 'mongodb';

// MongoDB-specific types extending the base interfaces
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

export interface BaseAdapter {
  disconnect(connectionString: string, databaseName: string): unknown;
  connect(connectionString: string, dbName: string, options?: MongoClientOptions): Promise<MongoConnection>;
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

export interface MongoCollection {
  name: string;
  schema?: Record<string, any>;
  indexes?: MongoIndexDefinition[];
  validation?: Record<string, any>;
}

export interface MongoIndexDefinition {
  name: string;
  keys: Record<string, 1 | -1 | 'text' | '2dsphere'>;
  options?: {
    unique?: boolean;
    sparse?: boolean;
    background?: boolean;
    expireAfterSeconds?: number;
  };
}

export interface MongoDatabaseSchema {
  version: string;
  database_name: string;
  description?: string;
  collections: Record<string, MongoCollection>;
}