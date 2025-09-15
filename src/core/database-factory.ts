// src/core/database-factory.ts

import { MongoClientOptions } from "mongodb";
import { MongoDatabaseSchema } from "../types";
import { MongoUniversalDAO } from "./universal-dao";
import { MongoAdapter } from "../adapters/mongo-adapter";

// ========================== DATABASE FACTORY FOR MONGODB ==========================
export class MongoDatabaseFactory {
  private static adapters: MongoAdapter[] = [];

  static registerAdapter(adapter: MongoAdapter): void {
    this.adapters.push(adapter);
  }

  private static getBestAdapter(): MongoAdapter {
    const adapter = this.adapters.find((a) => a.isSupported());
    if (!adapter) {
      // Use default adapter if none registered
      return new MongoAdapter();
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

  static async createFromSchema(
    schema: MongoDatabaseSchema,
    connectionString: string,
    options?: MongoClientOptions
  ): Promise<MongoUniversalDAO> {
    const dao = this.createDAO(connectionString, schema.database_name, options);
    await dao.connect();
    await dao.initializeFromSchema(schema);
    return dao;
  }
}
