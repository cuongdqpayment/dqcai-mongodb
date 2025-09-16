// ./src/adapter/base-adapter.ts
import { MongoClientOptions, MongoClient } from "mongodb";
import { MongoAdapter, MongoConnection } from "../types";

// ========================== MONGODB ADAPTER ==========================
export class BaseMongoAdapter implements MongoAdapter {
  private connections: Map<string, MongoConnection> = new Map();

  isSupported(): boolean {
    try {
      require('mongodb');
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
      throw new Error(`Failed to connect to MongoDB: ${(error as Error).message}`);
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