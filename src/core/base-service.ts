// src/core/base-service.ts

import { ObjectId } from "mongodb";
import { MongoQueryOptions } from "../types";
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

  // CRUD operations with MongoDB-specific features
  async create(data: Partial<T>): Promise<T> {
    await this.init();
    const result = await this.dao.insert(this.collectionName, data as Record<string, any>);
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
    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
    return await this.dao.findOne(this.collectionName, { _id: objectId }) as T | null;
  }

  async findOne(filter: Record<string, any> = {}): Promise<T | null> {
    await this.init();
    return await this.dao.findOne(this.collectionName, filter) as T | null;
  }

  async findMany(
    filter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<T[]> {
    await this.init();
    return await this.dao.find(this.collectionName, filter, options) as T[];
  }

  async updateById(
    id: string | ObjectId,
    update: Partial<T>
  ): Promise<boolean> {
    await this.init();
    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
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
    const objectId = typeof id === 'string' ? new ObjectId(id) : id;
    const result = await this.dao.delete(this.collectionName, { _id: objectId });
    return result.rowsAffected > 0;
  }

  async deleteMany(filter: Record<string, any>): Promise<number> {
    await this.init();
    const result = await this.dao.delete(this.collectionName, filter, { multi: true });
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

  // MongoDB-specific aggregation support
  async aggregate(pipeline: Record<string, any>[]): Promise<any[]> {
    await this.init();
    return await this.dao.aggregate(this.collectionName, pipeline);
  }

  // Transaction support
  async executeTransaction<R>(
    callback: () => Promise<R>
  ): Promise<R> {
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
}
