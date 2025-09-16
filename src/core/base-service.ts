// src/core/base-service.ts

import { 
  MongoQueryOptions, 
  WhereClause, 
  OrderByClause, 
  LimitOffset,
  AggregationPipeline,
  UpdateOperation,
  ObjectId,
  ImportOptions,
  ImportResult,
  ServiceStatus,
  HealthCheckResult
} from "../types";
import { MongoUniversalDAO } from "./universal-dao";
import { MongoDatabaseManager } from "./database-manager";
import { createModuleLogger, MongoModules } from "../logger/logger-config";

const logger = createModuleLogger(MongoModules.BASE_SERVICE);

export interface MongoFindOptions {
  where?: WhereClause[];
  orderBy?: OrderByClause[];
  limit?: number;
  offset?: number;
  columns?: string[];
  sort?: Record<string, 1 | -1>;
}

export type ErrorHandler = (error: Error) => void;
export type EventHandler = (data: any) => void;

/**
 * MongoDB BaseService - Enhanced abstract base class for MongoDB operations
 * using DatabaseManager for connection management and role-based access control
 */
export abstract class MongoBaseService<T = any> {
  protected dao: MongoUniversalDAO | null = null;
  protected schemaName: string;
  protected collectionName: string;
  protected isOpened: boolean = false;
  protected isInitialized: boolean = false;
  protected errorHandlers: Map<string, ErrorHandler> = new Map();
  protected eventListeners: Map<string, EventHandler[]> = new Map();
  protected primaryKeyFields: string[] = ["_id"];
  private cache: Map<string, any> = new Map();
  private reconnectHandler: (dao: MongoUniversalDAO) => void;

  constructor(schemaName: string, collectionName?: string) {
    this.schemaName = schemaName;
    this.collectionName = collectionName || schemaName;

    logger.debug("Creating MongoBaseService instance", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      primaryKeyFields: this.primaryKeyFields
    });

    // Register reconnect listener for database reconnection
    this.reconnectHandler = (newDao: MongoUniversalDAO) => {
      logger.info("Database reconnected for service", {
        schemaName: this.schemaName,
        collectionName: this.collectionName
      });
      
      this.dao = newDao;
      this._emit("daoReconnected", { schemaName: this.schemaName });
    };

    MongoDatabaseManager.onDatabaseReconnect(schemaName, this.reconnectHandler);
    this.bindMethods();
    
    logger.trace("MongoBaseService instance created successfully", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });
  }

  private bindMethods(): void {
    logger.trace("Binding service methods", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    const methods = Object.getOwnPropertyNames(Object.getPrototypeOf(this));
    methods.forEach((method) => {
      if (
        typeof (this as any)[method] === "function" &&
        method !== "constructor"
      ) {
        (this as any)[method] = (this as any)[method].bind(this);
      }
    });
  }

  /**
   * Set primary key fields for the service
   */
  setPrimaryKeyFields(fields: string[]): this {
    logger.debug("Setting primary key fields", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      previousFields: this.primaryKeyFields,
      newFields: fields
    });

    this.primaryKeyFields = fields;
    return this;
  }

  /**
   * Initialize the service and establish database connection
   */
  async init(): Promise<this> {
    logger.info("Initializing MongoBaseService", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      isInitialized: this.isInitialized
    });

    try {
      if (this.isInitialized) {
        logger.debug("Service already initialized, skipping", {
          schemaName: this.schemaName
        });
        return this;
      }

      logger.debug("Getting DAO from MongoDatabaseManager", {
        schemaName: this.schemaName
      });

      this.dao = await MongoDatabaseManager.getLazyLoading(this.schemaName);

      if (!this.dao) {
        const errorMsg = `Failed to initialize DAO for schema: ${this.schemaName}`;
        logger.error(errorMsg, {
          schemaName: this.schemaName
        });
        throw new Error(errorMsg);
      }

      if (!this.dao.isConnectionOpen()) {
        logger.debug("DAO connection not open, connecting", {
          schemaName: this.schemaName
        });
        await this.dao.connect();
      }

      this.isOpened = true;
      this.isInitialized = true;
      
      logger.info("MongoBaseService initialized successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        isOpened: this.isOpened,
        isInitialized: this.isInitialized
      });

      this._emit("initialized", { schemaName: this.schemaName });

      return this;
    } catch (error) {
      logger.error("Error initializing MongoBaseService", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      this._handleError("INIT_ERROR", error as Error);
      throw error;
    }
  }

  // ========================== BASIC CRUD OPERATIONS ==========================
  
  /**
   * Create a new document
   */
  async create(data: Partial<T>): Promise<T> {
    logger.debug("Creating new document", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      hasData: !!data,
      dataKeys: data ? Object.keys(data) : []
    });

    await this._ensureInitialized();
    await this.ensureValidConnection();

    try {
      this._validateData(data);

      logger.trace("Inserting document", {
        schemaName: this.schemaName,
        collectionName: this.collectionName
      });

      const result = await this.dao!.insert(this.collectionName, data as Record<string, any>);
      const createdDocument = result.rows[0] as T;

      logger.info("Document created successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        documentId: (createdDocument as any)?._id
      });

      this._emit("dataCreated", { operation: "create", data: createdDocument });
      return createdDocument;
    } catch (error) {
      logger.error("Error creating document", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      this._handleError("CREATE_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Create many documents
   */
  async createMany(dataArray: Partial<T>[]): Promise<T[]> {
    logger.debug("Creating multiple documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      count: dataArray.length
    });

    await this._ensureInitialized();
    await this.ensureValidConnection();

    try {
      if (!Array.isArray(dataArray) || dataArray.length === 0) {
        const errorMsg = "Data must be a non-empty array";
        logger.error(errorMsg, {
          schemaName: this.schemaName,
          collectionName: this.collectionName
        });
        throw new Error(errorMsg);
      }

      dataArray.forEach(data => this._validateData(data));

      const result = await this.dao!.insertMany(
        this.collectionName,
        dataArray as Record<string, any>[]
      );

      const createdDocuments = result.rows as T[];

      logger.info("Multiple documents created successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        count: createdDocuments.length
      });

      this._emit("dataBulkCreated", { 
        operation: "createMany", 
        count: createdDocuments.length 
      });

      return createdDocuments;
    } catch (error) {
      logger.error("Error creating multiple documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        count: dataArray.length,
        error: (error as Error).message
      });

      this._handleError("CREATE_MANY_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Find document by ID
   */
  async findById(id: string | ObjectId): Promise<T | null> {
    logger.debug("Finding document by ID", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString()
    });

    await this._ensureInitialized();

    try {
      if (!id) {
        const errorMsg = "ID is required";
        logger.error(errorMsg, {
          schemaName: this.schemaName,
          collectionName: this.collectionName
        });
        throw new Error(errorMsg);
      }

      const objectId = typeof id === 'string' ? new ObjectId(id) : id;
      const document = await this.dao!.findOne(this.collectionName, { _id: objectId }) as T | null;

      logger.debug("Find by ID completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        id: id.toString(),
        documentFound: !!document
      });

      this._emit("dataFetched", { operation: "findById", id });
      return document;
    } catch (error) {
      logger.error("Error finding document by ID", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        id: id.toString(),
        error: (error as Error).message
      });

      this._handleError("FIND_BY_ID_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Find first document matching conditions
   */
  async findFirst(conditions: Record<string, any> = {}): Promise<T | null> {
    logger.debug("Finding first document", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      conditionsCount: Object.keys(conditions).length,
      conditions
    });

    await this._ensureInitialized();

    try {
      const document = await this.dao!.findOne(this.collectionName, conditions) as T | null;

      logger.debug("Find first completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        documentFound: !!document
      });

      this._emit("dataFetched", { operation: "findFirst" });
      return document;
    } catch (error) {
      logger.error("Error finding first document", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        conditions,
        error: (error as Error).message
      });

      this._handleError("FIND_FIRST_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Find all documents matching conditions
   */
  async findAll(
    conditions: Record<string, any> = {},
    options: MongoFindOptions = {}
  ): Promise<T[]> {
    logger.debug("Finding all documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      conditionsCount: Object.keys(conditions).length,
      hasLimit: !!options.limit,
      hasOffset: !!options.offset,
      hasSort: !!(options.sort && Object.keys(options.sort).length > 0),
      limit: options.limit,
      offset: options.offset
    });

    await this._ensureInitialized();

    try {
      const mongoOptions: MongoQueryOptions = {};

      if (options.limit !== undefined) {
        mongoOptions.limit = options.limit;
      }
      if (options.offset !== undefined) {
        mongoOptions.skip = options.offset;
      }
      if (options.sort) {
        mongoOptions.sort = options.sort;
      }

      // Build filter from conditions and where clauses
      let filter = { ...conditions };
      
      if (options.where && options.where.length > 0) {
        const whereFilter = this.dao!.buildMongoQuery(options.where);
        filter = { ...filter, ...whereFilter };
      }

      if (options.orderBy && options.orderBy.length > 0 && !options.sort) {
        mongoOptions.sort = this.dao!.buildMongoSort(options.orderBy);
      }

      const documents = await this.dao!.find(this.collectionName, filter, mongoOptions) as T[];

      logger.info("Find all completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        documentsFound: documents.length,
        conditionsCount: Object.keys(conditions).length
      });

      this._emit("dataFetched", {
        operation: "findAll",
        count: documents.length,
      });

      return documents;
    } catch (error) {
      logger.error("Error finding all documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        conditions,
        options,
        error: (error as Error).message
      });

      this._handleError("FIND_ALL_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Update document by ID
   */
  async updateById(
    id: string | ObjectId,
    update: Partial<T>
  ): Promise<boolean> {
    logger.debug("Updating document by ID", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString(),
      hasUpdate: !!update,
      updateKeys: update ? Object.keys(update) : []
    });

    await this._ensureInitialized();

    try {
      if (!id) {
        const errorMsg = "ID is required for update";
        logger.error(errorMsg, {
          schemaName: this.schemaName,
          collectionName: this.collectionName
        });
        throw new Error(errorMsg);
      }

      this._validateData(update);

      const objectId = typeof id === 'string' ? new ObjectId(id) : id;
      const result = await this.dao!.update(
        this.collectionName,
        { _id: objectId },
        update as Record<string, any>
      );

      const success = result.rowsAffected > 0;

      if (success) {
        logger.info("Document updated successfully", {
          schemaName: this.schemaName,
          collectionName: this.collectionName,
          id: id.toString(),
          rowsAffected: result.rowsAffected
        });
        this._emit("dataUpdated", { operation: "updateById", id, update });
      } else {
        logger.warn("Update operation completed but no rows affected", {
          schemaName: this.schemaName,
          collectionName: this.collectionName,
          id: id.toString()
        });
      }

      return success;
    } catch (error) {
      logger.error("Error updating document by ID", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        id: id.toString(),
        error: (error as Error).message
      });

      this._handleError("UPDATE_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Update many documents
   */
  async updateMany(
    filter: Record<string, any>,
    update: Partial<T>
  ): Promise<number> {
    logger.debug("Updating many documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      filterCount: Object.keys(filter).length,
      updateKeys: update ? Object.keys(update) : []
    });

    await this._ensureInitialized();

    try {
      this._validateData(update);

      const result = await this.dao!.update(
        this.collectionName,
        filter,
        update as Record<string, any>,
        { multi: true }
      );

      logger.info("Multiple documents updated successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        rowsAffected: result.rowsAffected
      });

      this._emit("dataBulkUpdated", { 
        operation: "updateMany", 
        count: result.rowsAffected 
      });

      return result.rowsAffected;
    } catch (error) {
      logger.error("Error updating many documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        filter,
        error: (error as Error).message
      });

      this._handleError("UPDATE_MANY_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Delete document by ID
   */
  async deleteById(id: string | ObjectId): Promise<boolean> {
    logger.debug("Deleting document by ID", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString()
    });

    await this._ensureInitialized();

    try {
      if (!id) {
        const errorMsg = "ID is required for delete";
        logger.error(errorMsg, {
          schemaName: this.schemaName,
          collectionName: this.collectionName
        });
        throw new Error(errorMsg);
      }

      const objectId = typeof id === 'string' ? new ObjectId(id) : id;
      const result = await this.dao!.delete(this.collectionName, { _id: objectId });
      const success = result.rowsAffected > 0;

      if (success) {
        logger.info("Document deleted successfully", {
          schemaName: this.schemaName,
          collectionName: this.collectionName,
          id: id.toString(),
          rowsAffected: result.rowsAffected
        });
        this._emit("dataDeleted", { operation: "deleteById", id });
      } else {
        logger.warn("Delete operation completed but no rows affected", {
          schemaName: this.schemaName,
          collectionName: this.collectionName,
          id: id.toString()
        });
      }

      return success;
    } catch (error) {
      logger.error("Error deleting document by ID", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        id: id.toString(),
        error: (error as Error).message
      });

      this._handleError("DELETE_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Delete many documents
   */
  async deleteMany(filter: Record<string, any>): Promise<number> {
    logger.debug("Deleting many documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      filterCount: Object.keys(filter).length
    });

    await this._ensureInitialized();

    try {
      const result = await this.dao!.delete(this.collectionName, filter, { multi: true });

      logger.info("Multiple documents deleted successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        rowsAffected: result.rowsAffected
      });

      this._emit("dataBulkDeleted", { 
        operation: "deleteMany", 
        count: result.rowsAffected 
      });

      return result.rowsAffected;
    } catch (error) {
      logger.error("Error deleting many documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        filter,
        error: (error as Error).message
      });

      this._handleError("DELETE_MANY_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Count documents matching conditions
   */
  async count(filter: Record<string, any> = {}): Promise<number> {
    logger.debug("Counting documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      hasFilter: !!filter && Object.keys(filter).length > 0,
      filterCount: Object.keys(filter).length
    });

    await this._ensureInitialized();

    try {
      const count = await this.dao!.count(this.collectionName, filter);

      logger.debug("Count completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        count
      });

      return count;
    } catch (error) {
      logger.error("Error counting documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        filter,
        error: (error as Error).message
      });

      this._handleError("COUNT_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Check if a document exists
   */
  async exists(filter: Record<string, any>): Promise<boolean> {
    logger.debug("Checking if document exists", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      filterCount: Object.keys(filter).length
    });

    const count = await this.count(filter);
    const exists = count > 0;

    logger.debug("Existence check completed", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      exists,
      count
    });

    return exists;
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
    logger.debug("Finding with where clauses", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      whereCount: wheres.length,
      hasOrderBy: !!(orderBys && orderBys.length > 0),
      hasLimit: !!(limitOffset?.limit)
    });

    await this._ensureInitialized();
    
    try {
      const filter = this.dao!.buildMongoQuery(wheres);
      const options: MongoQueryOptions = {};
      
      if (orderBys && orderBys.length > 0) {
        options.sort = this.dao!.buildMongoSort(orderBys);
      }
      
      if (limitOffset?.limit) {
        options.limit = limitOffset.limit;
      }
      
      if (limitOffset?.offset) {
        options.skip = limitOffset.offset;
      }
      
      const documents = await this.dao!.find(this.collectionName, filter, options) as T[];

      logger.info("Find with where completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        documentsFound: documents.length
      });

      this._emit("dataFetched", {
        operation: "findWithWhere",
        count: documents.length,
      });

      return documents;
    } catch (error) {
      logger.error("Error finding with where clauses", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        whereCount: wheres.length,
        error: (error as Error).message
      });

      this._handleError("FIND_WITH_WHERE_ERROR", error as Error);
      throw error;
    }
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
    logger.debug("Finding with pagination", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      page,
      pageSize,
      hasSort: !!(sort && Object.keys(sort).length > 0)
    });

    await this._ensureInitialized();
    
    try {
      const skip = (page - 1) * pageSize;
      const total = await this.dao!.count(this.collectionName, filter);
      const totalPages = Math.ceil(total / pageSize);
      
      const options: MongoQueryOptions = {
        limit: pageSize,
        skip: skip
      };
      
      if (sort) {
        options.sort = sort;
      }
      
      const data = await this.dao!.find(this.collectionName, filter, options) as T[];
      
      const result = {
        data,
        pagination: {
          page,
          pageSize,
          total,
          totalPages,
          hasNext: page < totalPages,
          hasPrev: page > 1
        }
      };

      logger.info("Pagination find completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        page,
        pageSize,
        total,
        returned: data.length
      });

      this._emit("dataFetched", {
        operation: "findWithPagination",
        count: data.length,
        page,
        total
      });

      return result;
    } catch (error) {
      logger.error("Error finding with pagination", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        page,
        pageSize,
        error: (error as Error).message
      });

      this._handleError("FIND_PAGINATION_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Search documents with text search
   */
  async search(
    searchText: string,
    searchFields: string[],
    additionalFilter: Record<string, any> = {},
    options?: MongoQueryOptions
  ): Promise<T[]> {
    logger.debug("Searching documents", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      searchText,
      searchFieldsCount: searchFields.length,
      additionalFilterCount: Object.keys(additionalFilter).length
    });

    await this._ensureInitialized();
    
    try {
      const searchFilter: Record<string, any> = {
        $or: searchFields.map(field => ({
          [field]: { $regex: searchText, $options: 'i' }
        }))
      };
      
      const combinedFilter = {
        ...additionalFilter,
        ...searchFilter
      };
      
      const documents = await this.dao!.find(this.collectionName, combinedFilter, options) as T[];

      logger.info("Search completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        searchText,
        documentsFound: documents.length
      });

      this._emit("dataFetched", {
        operation: "search",
        count: documents.length,
        searchText
      });

      return documents;
    } catch (error) {
      logger.error("Error searching documents", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        searchText,
        error: (error as Error).message
      });

      this._handleError("SEARCH_ERROR", error as Error);
      throw error;
    }
  }

  // ========================== AGGREGATION OPERATIONS ==========================

  /**
   * Execute aggregation pipeline
   */
  async aggregate(pipeline: AggregationPipeline[]): Promise<any[]> {
    logger.debug("Executing aggregation", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      pipelineStages: pipeline.length
    });

    await this._ensureInitialized();

    try {
      const results = await this.dao!.aggregate(this.collectionName, pipeline);

      logger.info("Aggregation completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        resultsCount: results.length
      });

      this._emit("aggregationExecuted", {
        operation: "aggregate",
        count: results.length
      });

      return results;
    } catch (error) {
      logger.error("Error executing aggregation", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        pipelineStages: pipeline.length,
        error: (error as Error).message
      });

      this._handleError("AGGREGATION_ERROR", error as Error);
      throw error;
    }
  }

  // ========================== BULK OPERATIONS ==========================

  /**
   * Bulk insert documents
   */
  async bulkInsert(
    documents: Partial<T>[],
    batchSize: number = 1000
  ): Promise<ImportResult> {
    logger.info("Starting bulk insert", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      documentCount: documents.length,
      batchSize
    });

    await this._ensureInitialized();
    
    try {
      if (!Array.isArray(documents) || documents.length === 0) {
        const errorMsg = "Documents must be a non-empty array";
        logger.error(errorMsg, {
          schemaName: this.schemaName,
          collectionName: this.collectionName
        });
        throw new Error(errorMsg);
      }

      const startTime = Date.now();
      let successCount = 0;
      let errorCount = 0;
      const errors: Array<{ rowIndex: number; error: string; rowData: Record<string, any> }> = [];
      
      try {
        const result = await this.dao!.bulkInsert(
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
          rowData: {}
        });
      }
      
      const result: ImportResult = {
        totalRows: documents.length,
        successRows: successCount,
        errorRows: errorCount,
        errors,
        executionTime: Date.now() - startTime
      };

      logger.info("Bulk insert completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        totalRows: result.totalRows,
        successRows: result.successRows,
        errorRows: result.errorRows
      });

      this._emit("dataBulkCreated", {
        operation: "bulkInsert",
        count: result.successRows,
      });

      return result;
    } catch (error) {
      logger.error("Error during bulk insert", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        documentCount: documents.length,
        error: (error as Error).message
      });

      this._handleError("BULK_INSERT_ERROR", error as Error);
      throw error;
    }
  }

  // ========================== TRANSACTION SUPPORT ==========================

  /**
   * Execute operations within a transaction
   */
  async executeTransaction<R>(callback: () => Promise<R>): Promise<R> {
    logger.debug("Starting transaction", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this._ensureInitialized();

    try {
      logger.trace("Beginning database transaction", {
        schemaName: this.schemaName
      });

      await this.dao!.beginTransaction();
      const result = await callback();
      
      logger.trace("Committing transaction", {
        schemaName: this.schemaName
      });

      await this.dao!.commitTransaction();
      
      logger.info("Transaction completed successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName
      });

      this._emit("transactionCompleted", { operation: "transaction" });
      return result;
    } catch (error) {
      logger.error("Transaction failed, rolling back", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      try {
        await this.dao!.rollbackTransaction();
        logger.debug("Transaction rollback successful", {
          schemaName: this.schemaName
        });
      } catch (rollbackError) {
        logger.error("Error during transaction rollback", {
          schemaName: this.schemaName,
          rollbackError: (rollbackError as Error).message
        });
        this._handleError("ROLLBACK_ERROR", rollbackError as Error);
      }
      
      this._handleError("TRANSACTION_ERROR", error as Error);
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
    logger.info("Starting import from SQLite format", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      recordsCount: records.length,
      batchSize: options?.batchSize || 1000
    });

    await this._ensureInitialized();
    
    const batchSize = options?.batchSize || 1000;
    const startTime = Date.now();
    let successCount = 0;
    let errorCount = 0;
    const errors: Array<{ rowIndex: number; error: string; rowData: Record<string, any> }> = [];
    
    try {
      // Transform records from SQLite to MongoDB format
      const transformedRecords = records.map((record, index) => {
        try {
          let transformed = this.dao!.sqliteToMongoFormat(record);
          
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
            rowData: record
          });
          errorCount++;
          return null;
        }
      }).filter(record => record !== null);
      
      // Batch insert
      for (let i = 0; i < transformedRecords.length; i += batchSize) {
        try {
          const batch = transformedRecords.slice(i, i + batchSize);
          const result = await this.dao!.bulkInsert(this.collectionName, batch, batchSize);
          successCount += result.rowsAffected;
          
          if (options?.onProgress) {
            options.onProgress(Math.min(i + batchSize, transformedRecords.length), records.length);
          }
        } catch (error) {
          const batchStart = i;
          const batchEnd = Math.min(i + batchSize, transformedRecords.length);
          
          for (let j = batchStart; j < batchEnd; j++) {
            errors.push({
              rowIndex: j,
              error: (error as Error).message,
              rowData: transformedRecords[j]
            });
            errorCount++;
          }
          
          if (!options?.skipErrors) {
            break;
          }
        }
      }
      
      const result: ImportResult = {
        totalRows: records.length,
        successRows: successCount,
        errorRows: errorCount,
        errors,
        executionTime: Date.now() - startTime
      };

      logger.info("SQLite import completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        totalRows: result.totalRows,
        successRows: result.successRows,
        errorRows: result.errorRows
      });

      this._emit("dataImported", { operation: "importFromSQLite", result });
      return result;
    } catch (error) {
      logger.error("Error during SQLite import", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        recordsCount: records.length,
        error: (error as Error).message
      });

      this._handleError("IMPORT_SQLITE_ERROR", error as Error);
      throw error;
    }
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
    logger.debug("Exporting data to SQLite format", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      hasFilter: Object.keys(filter).length > 0,
      hasLimit: !!options?.limit
    });

    await this._ensureInitialized();
    
    try {
      const queryOptions: MongoQueryOptions = {};
      
      if (options?.limit) {
        queryOptions.limit = options.limit;
      }
      
      if (options?.sort) {
        queryOptions.sort = options.sort;
      }
      
      const records = await this.dao!.find(this.collectionName, filter, queryOptions);
      
      const transformedRecords = records.map(record => {
        let transformed = this.dao!.mongoToSQLiteFormat(record);
        
        if (options?.transformRecord) {
          transformed = options.transformRecord(transformed);
        }
        
        return transformed;
      });

      logger.info("SQLite export completed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        recordsExported: transformedRecords.length
      });

      return transformedRecords;
    } catch (error) {
      logger.error("Error during SQLite export", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      this._handleError("EXPORT_SQLITE_ERROR", error as Error);
      throw error;
    }
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
    logger.debug("Getting collection statistics", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this._ensureInitialized();
    
    try {
      const count = await this.dao!.count(this.collectionName);
      const collectionInfo = await this.dao!.getCollectionInfo(this.collectionName);
      
      const stats = {
        name: this.collectionName,
        count,
        averageSize: 0, // MongoDB doesn't provide this directly
        totalSize: 0,   // MongoDB doesn't provide this directly
        indexes: collectionInfo.indexes
      };

      logger.debug("Collection statistics retrieved", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        count: stats.count,
        indexesCount: stats.indexes.length
      });

      return stats;
    } catch (error) {
      logger.error("Error getting collection statistics", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      this._handleError("GET_STATS_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Get distinct values for a field
   */
  async distinct(field: string, filter: Record<string, any> = {}): Promise<any[]> {
    logger.debug("Getting distinct values", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      field,
      hasFilter: Object.keys(filter).length > 0
    });

    await this._ensureInitialized();
    
    try {
      const collection = this.dao!['getCollection'](this.collectionName);
      const values = await collection.distinct(field, filter);

      logger.debug("Distinct values retrieved", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        field,
        valuesCount: values.length
      });

      return values;
    } catch (error) {
      logger.error("Error getting distinct values", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        field,
        error: (error as Error).message
      });

      this._handleError("DISTINCT_ERROR", error as Error);
      throw error;
    }
  }

  /**
   * Check if collection is empty
   */
  async isEmpty(): Promise<boolean> {
    const count = await this.count();
    return count === 0;
  }

  /**
   * Clear all documents in collection
   */
  async clear(): Promise<number> {
    logger.warn("Clearing all documents in collection", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this._ensureInitialized();
    const result = await this.deleteMany({});

    logger.info("Collection cleared", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      deletedCount: result
    });

    this._emit("collectionCleared", { collectionName: this.collectionName });
    return result;
  }

  // ========================== EVENT SYSTEM ==========================

  on(event: string, handler: EventHandler): this {
    logger.trace("Adding event listener", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      event
    });

    if (!this.eventListeners.has(event)) {
      this.eventListeners.set(event, []);
    }
    this.eventListeners.get(event)!.push(handler);
    return this;
  }

  off(event: string, handler: EventHandler): this {
    logger.trace("Removing event listener", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      event
    });

    const handlers = this.eventListeners.get(event);
    if (handlers) {
      const index = handlers.indexOf(handler);
      if (index > -1) {
        handlers.splice(index, 1);
      }
    }
    return this;
  }

  protected _emit(event: string, data: any): void {
    logger.trace("Emitting event", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      event,
      hasData: !!data
    });

    const handlers = this.eventListeners.get(event);
    if (handlers) {
      handlers.forEach((handler) => {
        try {
          handler(data);
        } catch (error) {
          logger.error("Error in event handler", {
            schemaName: this.schemaName,
            collectionName: this.collectionName,
            event,
            error: (error as Error).message
          });
        }
      });
    }
  }

  // ========================== ERROR HANDLING ==========================

  setErrorHandler(errorType: string, handler: ErrorHandler): this {
    logger.debug("Setting error handler", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      errorType
    });

    this.errorHandlers.set(errorType, handler);
    return this;
  }

  protected _handleError(errorType: string, error: Error): void {
    logger.error("Handling service error", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      errorType,
      error: error.message
    });

    const handler = this.errorHandlers.get(errorType);
    if (handler) {
      try {
        handler(error);
      } catch (handlerError) {
        logger.error("Error in error handler", {
          schemaName: this.schemaName,
          collectionName: this.collectionName,
          errorType,
          handlerError: (handlerError as Error).message
        });
      }
    }
    this._emit("error", { errorType, error });
  }

  protected _validateData(data: any): void {
    if (!data || typeof data !== "object") {
      const errorMsg = "Data must be a valid object";
      logger.error("Data validation failed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        dataType: typeof data,
        isNull: data === null
      });
      throw new Error(errorMsg);
    }
  }

  protected async _ensureInitialized(): Promise<void> {
    if (!this.isInitialized) {
      logger.debug("Service not initialized, initializing now", {
        schemaName: this.schemaName,
        collectionName: this.collectionName
      });
      await this.init();
    }
  }

  private async ensureValidConnection(): Promise<void> {
    logger.trace("Ensuring valid database connection", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    try {
      const isConnected = this.dao?.isConnectionOpen();
      if (!isConnected) {
        logger.debug("Connection not valid, getting new connection", {
          schemaName: this.schemaName
        });
        this.dao = await MongoDatabaseManager.ensureDatabaseConnection(
          this.schemaName
        );
      }
    } catch (error) {
      logger.warn("Error checking connection, getting new connection", {
        schemaName: this.schemaName,
        error: (error as Error).message
      });
      this.dao = await MongoDatabaseManager.ensureDatabaseConnection(
        this.schemaName
      );
    }
  }

  // ========================== INFORMATION AND STATUS METHODS ==========================

  async getDatabaseInfo(): Promise<any> {
    logger.trace("Getting database info", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this._ensureInitialized();
    return await this.dao!.getDatabaseInfo();
  }

  async getCollectionInfo(): Promise<any> {
    logger.trace("Getting collection info", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this._ensureInitialized();
    return await this.dao!.getCollectionInfo(this.collectionName);
  }

  getStatus(): ServiceStatus {
    const status = {
      schemaName: this.schemaName,
      tableName: this.collectionName, // Keep tableName for compatibility
      collectionName: this.collectionName,
      isOpened: this.isOpened,
      isInitialized: this.isInitialized,
      hasDao: !!this.dao,
    };

    logger.trace("Getting service status", status);
    return status;
  }

  async healthCheck(): Promise<HealthCheckResult> {
    logger.debug("Performing health check", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    try {
      await this._ensureInitialized();
      const count = await this.count();
      
      const result = {
        healthy: true,
        schemaName: this.schemaName,
        recordCount: count,
        timestamp: new Date().toISOString(),
      };

      logger.info("Health check passed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        recordCount: count
      });

      return result;
    } catch (error) {
      const result = {
        healthy: false,
        schemaName: this.schemaName,
        error: (error as Error).message,
        timestamp: new Date().toISOString(),
      };

      logger.error("Health check failed", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      return result;
    }
  }

  // ========================== LIFECYCLE MANAGEMENT ==========================

  async close(): Promise<boolean> {
    logger.info("Closing MongoBaseService", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      isOpened: this.isOpened,
      isInitialized: this.isInitialized
    });

    try {
      if (this.dao) {
        await this.dao.disconnect();
        logger.debug("DAO disconnected successfully", {
          schemaName: this.schemaName
        });
      }

      this.isOpened = false;
      this.isInitialized = false;
      this.eventListeners.clear();
      this.errorHandlers.clear();
      this.cache.clear();

      logger.info("MongoBaseService closed successfully", {
        schemaName: this.schemaName,
        collectionName: this.collectionName
      });

      this._emit("closed", { schemaName: this.schemaName });
      return true;
    } catch (error) {
      logger.error("Error closing MongoBaseService", {
        schemaName: this.schemaName,
        collectionName: this.collectionName,
        error: (error as Error).message
      });

      this._handleError("CLOSE_ERROR", error as Error);
      throw error;
    }
  }

  public destroy(): void {
    logger.debug("Destroying MongoBaseService", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    // Remove reconnect listener
    MongoDatabaseManager.offDatabaseReconnect(
      this.schemaName,
      this.reconnectHandler
    );

    // Clear all resources
    this.eventListeners.clear();
    this.errorHandlers.clear();
    this.cache.clear();

    logger.trace("MongoBaseService destroyed", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });
  }

  // ========================== BACKWARD COMPATIBILITY ALIASES ==========================

  async getAll(
    conditions: Record<string, any> = {},
    options: MongoFindOptions = {}
  ): Promise<T[]> {
    logger.trace("Using getAll alias", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });
    return this.findAll(conditions, options);
  }

  async getById(id: string | ObjectId): Promise<T | null> {
    logger.trace("Using getById alias", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString()
    });
    return this.findById(id);
  }

  async getFirst(conditions: Record<string, any> = {}): Promise<T | null> {
    logger.trace("Using getFirst alias", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });
    return this.findFirst(conditions);
  }

  // ========================== LEGACY SQLITE-COMPATIBLE METHODS ==========================

  /**
   * Legacy method for SQLite compatibility - maps to updateById
   */
  async update(id: string | ObjectId, data: Partial<T>): Promise<T | null> {
    logger.trace("Using legacy update method", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString()
    });

    const success = await this.updateById(id, data);
    if (success) {
      return await this.findById(id);
    }
    return null;
  }

  /**
   * Legacy method for SQLite compatibility - maps to deleteById
   */
  async delete(id: string | ObjectId): Promise<boolean> {
    logger.trace("Using legacy delete method", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      id: id.toString()
    });

    return await this.deleteById(id);
  }

  /**
   * Legacy method for SQLite compatibility - maps to clear
   */
  async truncate(): Promise<void> {
    logger.warn("Using legacy truncate method - clearing collection", {
      schemaName: this.schemaName,
      collectionName: this.collectionName
    });

    await this.clear();
    this._emit("collectionTruncated", { collectionName: this.collectionName });
  }

  /**
   * Legacy method for SQLite compatibility - maps to bulkInsert
   */
  async bulkCreate(dataArray: Record<string, any>[]): Promise<T[]> {
    logger.trace("Using legacy bulkCreate method", {
      schemaName: this.schemaName,
      collectionName: this.collectionName,
      itemsCount: dataArray.length
    });

    const result = await this.bulkInsert(dataArray  as Partial<T>[]);
    
    // Return the created documents by querying recent inserts
    // This is a simplified approach - in production you might want to track inserted IDs
    if (result.successRows > 0) {
      const recentDocuments = await this.findAll({}, { 
        limit: result.successRows,
        sort: { _id: -1 }
      });
      return recentDocuments.reverse(); // Return in creation order
    }
    
    return [];
  }
}