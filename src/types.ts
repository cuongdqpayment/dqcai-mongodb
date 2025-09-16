// src/types.ts
import { MongoClient, Db, Collection, ObjectId, ClientSession, MongoClientOptions } from 'mongodb';

// ========================== BASIC MONGODB TYPES ==========================
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

export interface MongoAdapter {
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

// ========================== SHARED SCHEMA TYPES (Compatible with SQLite) ==========================

// Enhanced schema types based on SQLiteDAO but adapted for MongoDB
export interface TypeMappingConfig {
  type_mapping: {
    [targetType: string]: {
      [sourceType: string]: string;
    };
  };
}

export interface ColumnDefinition {
  name: string;
  type: string;
  precision?: number;
  scale?: number;
  option_key?: string;
  description?: string;
  nullable?: boolean;
  default?: any;
  primary_key?: boolean;
  auto_increment?: boolean;
  enum?: string[] | number[];
  unique?: boolean;
  constraints?: string;
  length?: number;
}

export interface Column {
  name: string;
  value?: any;
}

export interface WhereClause {
  name: string;
  value: any;
  operator?: string;
}

export interface OrderByClause {
  name: string;
  direction?: "ASC" | "DESC";
}

export interface LimitOffset {
  limit?: number;
  offset?: number;
}

export interface QueryTable {
  name: string;
  cols: Column[];
  wheres?: WhereClause[];
  orderbys?: OrderByClause[];
  limitOffset?: LimitOffset;
}

export interface JoinClause {
  type: "INNER" | "LEFT" | "RIGHT" | "FULL";
  table: string;
  on: string;
}

export interface IndexDefinition {
  name: string;
  columns: string[];
  unique?: boolean;
  description?: string;
}

export type ForeignKeyAction =
  | "CASCADE"
  | "RESTRICT"
  | "SET NULL"
  | "NO ACTION"
  | undefined;

export interface ForeignKeyDefinition {
  name: string;
  column: string;
  references: {
    table: string;
    column: string;
  };
  on_delete?: string | ForeignKeyAction;
  on_update?: string | ForeignKeyAction;
  description?: string;
}

export interface TableDefinition {
  name: string;
  cols: ColumnDefinition[];
  description?: string;
  indexes?: IndexDefinition[];
  foreign_keys?: ForeignKeyDefinition[];
}

// Main DatabaseSchema interface - Compatible with SQLite but adapted for MongoDB
export interface DatabaseSchema {
  version: string;
  database_name: string;
  description?: string;
  type_mapping?: TypeMappingConfig["type_mapping"];
  schemas: Record<
    string,
    {
      description?: string;
      cols: ColumnDefinition[];
      indexes?: IndexDefinition[];
      foreign_keys?: ForeignKeyDefinition[]; // Will be ignored in MongoDB but kept for compatibility
    }
  >;
}

// ========================== MONGODB SPECIFIC TYPES ==========================

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

export interface MongoCollection {
  name: string;
  schema?: Record<string, any>;
  indexes?: MongoIndexDefinition[];
  validation?: Record<string, any>;
}

export interface MongoDatabaseSchema {
  version: string;
  database_name: string;
  description?: string;
  collections: Record<string, MongoCollection>;
}

// ========================== TRANSACTION TYPES ==========================
export interface TransactionOperation {
  type: "insert" | "update" | "delete" | "select";
  table: QueryTable;
}

// ========================== IMPORT/EXPORT TYPES ==========================
export interface ImportOptions {
  tableName: string;
  data: Record<string, any>[];
  batchSize?: number;
  onProgress?: (processed: number, total: number) => void;
  onError?: (
    error: Error,
    rowIndex: number,
    rowData: Record<string, any>
  ) => void;
  skipErrors?: boolean;
  validateData?: boolean;
  updateOnConflict?: boolean;
  conflictColumns?: string[];
  includeAutoIncrementPK?: boolean;
}

export interface ImportResult {
  totalRows: number;
  successRows: number;
  errorRows: number;
  errors: Array<{
    rowIndex: number;
    error: string;
    rowData: Record<string, any>;
  }>;
  executionTime: number;
}

// ========================== COLUMN MAPPING TYPES ==========================
export interface ColumnMapping {
  sourceColumn: string;
  targetColumn: string;
  transform?: (value: any) => any;
}

// ========================== DATABASE FACTORY OPTIONS ==========================
export interface DbFactoryOptions {
  config?: DatabaseSchema; // Option 1: Provide a config object directly
  configAsset?: any; // Option 3: Provide a required JSON asset
  dbDirectory?: string; // Optional: Directory to store the .db file (not used in MongoDB)
  adapter?: MongoAdapter; // Optional: Specific adapter to use
}

// ========================== HEALTH CHECK TYPES ==========================
export interface ServiceStatus {
  schemaName: string;
  collectionName: string; // Changed from tableName
  isOpened: boolean;
  isInitialized: boolean;
  hasDao: boolean;
}

export interface HealthCheckResult {
  healthy: boolean;
  schemaName: string;
  recordCount?: number;
  error?: string;
  timestamp: string;
}

// ========================== MONGODB TYPE MAPPING ==========================
export const MONGODB_TYPE_MAPPING = {
  mongodb: {
    // String types
    string: 'String',
    varchar: 'String',
    char: 'String',
    text: 'String',
    email: 'String',
    url: 'String',
    uuid: 'String',
    
    // Numeric types
    integer: 'Number',
    int: 'Number',
    bigint: 'Number',
    smallint: 'Number',
    tinyint: 'Number',
    decimal: 'Number',
    numeric: 'Number',
    float: 'Number',
    double: 'Number',
    
    // Boolean
    boolean: 'Boolean',
    bool: 'Boolean',
    
    // Date/Time types
    timestamp: 'Date',
    datetime: 'Date',
    date: 'Date',
    time: 'Date',
    
    // Complex types
    json: 'Object',
    array: 'Array',
    object: 'Object',
    
    // Binary types
    blob: 'Buffer',
    binary: 'Buffer',
    
    // MongoDB specific
    objectid: 'ObjectId',
    mixed: 'Mixed',
    // Add index signature here
    // [key: string]: string; 
  }
};

// ========================== UTILITY TYPES ==========================

// Row type for MongoDB documents
export interface MongoRow {
  [key: string]: any;
  _id?: ObjectId;
}

// Enhanced result type for MongoDB operations
export interface MongoOperationResult extends MongoResult {
  acknowledged?: boolean;
  insertedCount?: number;
  matchedCount?: number;
  modifiedCount?: number;
  deletedCount?: number;
  upsertedCount?: number;
  upsertedId?: ObjectId;
}

// Aggregation pipeline types
export interface AggregationPipeline {
  $match?: Record<string, any>;
  $group?: Record<string, any>;
  $sort?: Record<string, 1 | -1>;
  $limit?: number;
  $skip?: number;
  $project?: Record<string, 0 | 1>;
  $unwind?: string | Record<string, any>;
  $lookup?: {
    from: string;
    localField: string;
    foreignField: string;
    as: string;
  };
  [key: string]: any;
}

// Update operation types
export interface UpdateOperation {
  $set?: Record<string, any>;
  $unset?: Record<string, any>;
  $inc?: Record<string, number>;
  $push?: Record<string, any>;
  $pull?: Record<string, any>;
  $addToSet?: Record<string, any>;
  [key: string]: any;
}

// ========================== MIGRATION HELPER TYPES ==========================

// Interface for database migration between SQLite and MongoDB
export interface MigrationMapping {
  sourceTable: string;
  targetCollection: string;
  columnMappings: ColumnMapping[];
  transformRules?: {
    [columnName: string]: (value: any) => any;
  };
  ignoreColumns?: string[];
  customFields?: Record<string, any>;
}

export interface MigrationConfig {
  sourceType: 'sqlite' | 'mongodb';
  targetType: 'sqlite' | 'mongodb';
  mappings: MigrationMapping[];
  batchSize?: number;
  onProgress?: (processed: number, total: number) => void;
  onError?: (error: Error, record: any) => void;
}

// ========================== VALIDATION TYPES ==========================

export interface ValidationRule {
  required?: boolean;
  type?: string;
  min?: number;
  max?: number;
  pattern?: string | RegExp;
  enum?: (string | number)[];
  custom?: (value: any) => boolean | string;
}

export interface SchemaValidation {
  [fieldName: string]: ValidationRule;
}