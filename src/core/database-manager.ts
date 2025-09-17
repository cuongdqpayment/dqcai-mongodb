// src/core/database-manager.ts

import {
  DatabaseSchema,
  ImportOptions,
  ImportResult,
  ColumnMapping,
  MongoClientOptions,
  MongoBulkImportResult,
  MongoDatabaseImportConfig,
  MongoRoleConfig,
  MongoRoleRegistry,
  MongoSchemaManager,
} from "../types";
import { MongoDatabaseFactory } from "./database-factory";
import { MongoUniversalDAO } from "./universal-dao";

// Import logger configuration for internal use
import { MongoModules, createModuleLogger } from "../logger/logger-config";
const logger = createModuleLogger(MongoModules.DATABASE_MANAGER);

export type MongoDatabaseConnections = {
  [key: string]: MongoUniversalDAO;
};

export class MongoDatabaseManager {
  private static maxConnections = 10;
  private static connections: MongoDatabaseConnections = {};
  private static isInitialized = false;
  private static roleRegistry: MongoRoleRegistry = {};
  private static currentRole: string | null = null;
  private static currentUserRoles: string[] = [];
  private static activeDatabases: Set<string> = new Set();
  private static isClosingConnections = false;

  // Connection configuration
  private static defaultConnectionString = "mongodb://localhost:27017";
  private static connectionOptions: MongoClientOptions = {};

  // Schema management
  private static schemaConfigurations: Record<string, DatabaseSchema> = {};
  private static schemaManager: MongoSchemaManager | null = null;

  // Event system for database reconnection
  private static eventListeners: Map<
    string,
    Array<(dao: MongoUniversalDAO) => void>
  > = new Map();

  // ========================== CONNECTION CONFIGURATION ==========================

  /**
   * Set default MongoDB connection string
   */
  public static setConnectionString(connectionString: string): void {
    logger.debug("Setting default connection string", { connectionString });
    this.defaultConnectionString = connectionString;
  }

  /**
   * Set MongoDB connection options
   */
  public static setConnectionOptions(options: MongoClientOptions): void {
    logger.debug("Setting connection options", { options });
    this.connectionOptions = { ...options };
  }

  /**
   * Get current connection string
   */
  public static getConnectionString(): string {
    logger.debug("Retrieving current connection string");
    return this.defaultConnectionString;
  }

  /**
   * Get the maximum number of allowed database connections
   */
  public static getMaxConnections(): number {
    logger.debug("Retrieving max connections", {
      maxConnections: this.maxConnections,
    });
    return this.maxConnections;
  }

  /**
   * Set the maximum number of allowed database connections
   */
  public static setMaxConnections(maxConnections: number): void {
    logger.debug("Setting max connections", { maxConnections });
    if (maxConnections <= 0) {
      logger.error("Invalid max connections value", { maxConnections });
      throw new Error("Maximum connections must be a positive number");
    }

    const currentConnectionCount = Object.keys(this.connections).length;
    if (currentConnectionCount > maxConnections) {
      logger.error("Cannot set max connections due to active connections", {
        maxConnections,
        currentConnectionCount,
      });
      throw new Error(
        `Cannot set maximum connections to ${maxConnections}. ` +
          `Current active connections (${currentConnectionCount}) exceed the new limit. ` +
          `Please close some connections first.`
      );
    }

    this.maxConnections = maxConnections;
    logger.info("Max connections updated", { maxConnections });
  }

  // ========================== SCHEMA MANAGEMENT ==========================

  /**
   * Set a schema manager for dynamic schema handling
   */
  public static setSchemaManager(manager: MongoSchemaManager): void {
    logger.info("Setting schema manager", {
      managerType: manager.constructor.name,
    });
    this.schemaManager = manager;
  }

  /**
   * Register a schema configuration dynamically
   */
  public static registerSchema(key: string, schema: DatabaseSchema): void {
    logger.debug("Registering schema", {
      schemaKey: key,
      schemaVersion: schema.version,
    });
    this.schemaConfigurations[key] = schema;
  }

  /**
   * Register multiple schemas at once
   */
  public static registerSchemas(schemas: Record<string, DatabaseSchema>): void {
    logger.info("Registering multiple schemas", {
      schemaCount: Object.keys(schemas).length,
    });
    Object.entries(schemas).forEach(([key, schema]) => {
      logger.trace("Registering individual schema", { schemaKey: key });
      this.registerSchema(key, schema);
    });
  }

  /**
   * Get schema from internal store or external manager
   */
  private static getSchema(key: string): DatabaseSchema | undefined {
    logger.debug("Retrieving schema", { schemaKey: key });
    if (this.schemaConfigurations[key]) {
      logger.trace("Schema found in internal configurations", {
        schemaKey: key,
      });
      return this.schemaConfigurations[key];
    }

    if (this.schemaManager) {
      const schema = this.schemaManager.getSchema(key);
      if (schema) {
        logger.trace("Schema found in external manager", { schemaKey: key });
        return schema;
      }
    }

    logger.warn("Schema not found", { schemaKey: key });
    return undefined;
  }

  /**
   * Get all available schema keys
   */
  public static getAvailableSchemas(): string[] {
    logger.debug("Retrieving available schema keys");
    const internalKeys = Object.keys(this.schemaConfigurations);
    const externalKeys = this.schemaManager?.getAllSchemaKeys() || [];
    const schemaKeys = [...new Set([...internalKeys, ...externalKeys])];
    logger.info("Retrieved available schema keys", {
      schemaCount: schemaKeys.length,
    });
    return schemaKeys;
  }

  // ========================== ROLE MANAGEMENT ==========================

  /**
   * Register a role configuration
   */
  public static registerRole(roleConfig: MongoRoleConfig): void {
    logger.debug("Registering role", { roleName: roleConfig.roleName });
    this.roleRegistry[roleConfig.roleName] = roleConfig;
  }

  /**
   * Register multiple roles
   */
  public static registerRoles(roleConfigs: MongoRoleConfig[]): void {
    logger.info("Registering multiple roles", {
      roleCount: roleConfigs.length,
    });
    roleConfigs.forEach((config) => {
      logger.trace("Registering individual role", {
        roleName: config.roleName,
      });
      this.registerRole(config);
    });
  }

  /**
   * Get all registered roles
   */
  public static getRegisteredRoles(): MongoRoleRegistry {
    logger.debug("Retrieving registered roles", {
      roleCount: Object.keys(this.roleRegistry).length,
    });
    return { ...this.roleRegistry };
  }

  /**
   * Get databases for a specific role
   */
  public static getRoleDatabases(roleName: string): string[] {
    logger.debug("Retrieving databases for role", { roleName });
    const roleConfig = this.roleRegistry[roleName];
    if (!roleConfig) {
      logger.error("Role not found", { roleName });
      throw new Error(`Role '${roleName}' is not registered.`);
    }

    const databases = [
      ...roleConfig.requiredDatabases,
      ...(roleConfig.optionalDatabases || []),
    ];
    logger.debug("Retrieved role databases", {
      roleName,
      databaseCount: databases.length,
    });
    return databases;
  }

  /**
   * Get databases for current user roles
   */
  public static getCurrentUserDatabases(isCore: boolean = false): string[] {
    logger.debug("Retrieving databases for current user roles", {
      roleCount: this.currentUserRoles.length,
    });
    const allDatabases = new Set<string>();
    // Chỉ sử dụng database này làm core quản trị db thì mới add nó vào tự động, không thì thôi
    if (isCore) allDatabases.add("core");

    for (const roleName of this.currentUserRoles) {
      const roleConfig = this.roleRegistry[roleName];
      if (roleConfig) {
        logger.trace("Processing role databases", { roleName });
        roleConfig.requiredDatabases.forEach((db) => allDatabases.add(db));
        if (roleConfig.optionalDatabases) {
          roleConfig.optionalDatabases.forEach((db) => allDatabases.add(db));
        }
      }
    }

    const databases = Array.from(allDatabases);
    logger.info("Retrieved current user databases", {
      databaseCount: databases.length,
    });
    return databases;
  }

  // ========================== CONNECTION MANAGEMENT ==========================

  /**
   * Initialize core database connection
   */
  public static async initializeCoreConnection(): Promise<void> {
    logger.info("Initializing core database connection");
    if (this.connections["core"]) {
      logger.debug("Core connection already initialized, skipping");
      return;
    }

    try {
      const coreSchema = this.getSchema("core");
      if (!coreSchema) {
        logger.error("Core database schema not found");
        throw new Error("Core database schema not found.");
      }

      const dao = await MongoDatabaseFactory.createFromSchema(
        coreSchema,
        this.defaultConnectionString,
        this.connectionOptions
      );

      this.connections["core"] = dao;
      logger.info("Core database connection initialized", {
        databaseName: coreSchema.database_name,
      });
    } catch (error) {
      logger.error("Error initializing core database", {
        error: (error as Error).message,
      });
      throw new Error(
        `Error initializing core database: ${(error as Error).message}`
      );
    }
  }

  /**
   * Set current user roles and initialize connections
   */
  public static async setCurrentUserRoles(
    userRoles: string[],
    primaryRole?: string
  ): Promise<void> {
    logger.info("Setting current user roles", {
      userRoles,
      primaryRole,
      previousRoleCount: this.currentUserRoles.length,
    });

    for (const roleName of userRoles) {
      if (!this.roleRegistry[roleName]) {
        logger.error("Invalid role detected", { roleName });
        throw new Error(
          `Role '${roleName}' is not registered. Please register it first.`
        );
      }
    }

    const previousRoles = [...this.currentUserRoles];
    this.currentUserRoles = userRoles;
    this.currentRole = primaryRole || userRoles[0] || null;
    logger.debug("Updated current user roles and primary role", {
      currentRoles: this.currentUserRoles,
      primaryRole: this.currentRole,
    });

    try {
      await this.initializeUserRoleConnections();
      logger.info("User role connections initialized successfully");
      await this.cleanupUnusedConnections(previousRoles);
      logger.info("Unused connections cleaned up", {
        previousRoleCount: previousRoles.length,
      });
    } catch (error) {
      logger.error("Error setting user roles or initializing connections", {
        error: (error as Error).message,
      });
      throw error;
    }
  }

  /**
   * Get current user roles
   */
  public static getCurrentUserRoles(): string[] {
    logger.debug("Retrieving current user roles", {
      roleCount: this.currentUserRoles.length,
    });
    return [...this.currentUserRoles];
  }

  /**
   * Get current primary role
   */
  public static getCurrentRole(): string | null {
    logger.debug("Retrieving current primary role", {
      primaryRole: this.currentRole,
    });
    return this.currentRole;
  }

  /**
   * Initialize connections for current user roles
   */
  private static async initializeUserRoleConnections(isCore: boolean = false): Promise<void> {
    logger.info("Initializing connections for user roles", {
      roleCount: this.currentUserRoles.length,
    });
    const requiredDatabases = this.getCurrentUserDatabases(isCore);
    const failedInitializations: { key: string; error: Error }[] = [];

    const initPromises = requiredDatabases.map(async (dbKey) => {
      if (this.connections[dbKey]) {
        logger.debug("Connection already exists, skipping", {
          databaseKey: dbKey,
        });
        return;
      }

      try {
        const schema = this.getSchema(dbKey);
        if (!schema) {
          logger.error("Schema not found for database", { databaseKey: dbKey });
          throw new Error(
            `Database key '${dbKey}' not found in schema configurations.`
          );
        }

        logger.debug("Creating connection for database", {
          databaseKey: dbKey,
        });
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );

        this.connections[dbKey] = dao;
        logger.info("Connection established for database", {
          databaseKey: dbKey,
        });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error("Failed to initialize database connection", {
          databaseKey: dbKey,
          error: err.message,
        });

        const isRequired = this.currentUserRoles.some((roleName) => {
          const roleConfig = this.roleRegistry[roleName];
          return roleConfig && roleConfig.requiredDatabases.includes(dbKey);
        });

        if (isRequired) {
          failedInitializations.push({ key: dbKey, error: err });
        }
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      logger.error("Failed to initialize required databases", {
        failedCount: failedInitializations.length,
        errorSummary,
      });
      throw new Error(
        `Failed to initialize required databases for user roles:\n${errorSummary}`
      );
    }

    logger.info("User role connections initialized successfully", {
      databaseCount: requiredDatabases.length,
    });
  }

  /**
   * Cleanup unused connections
   */
  private static async cleanupUnusedConnections(
    previousRoles: string[]
  ): Promise<void> {
    logger.debug("Cleaning up unused connections", {
      previousRoleCount: previousRoles.length,
    });
    const previousDatabases = new Set<string>();
    previousDatabases.add("core");

    for (const roleName of previousRoles) {
      const roleConfig = this.roleRegistry[roleName];
      if (roleConfig) {
        logger.trace("Processing previous role databases", { roleName });
        roleConfig.requiredDatabases.forEach((db) => previousDatabases.add(db));
        if (roleConfig.optionalDatabases) {
          roleConfig.optionalDatabases.forEach((db) =>
            previousDatabases.add(db)
          );
        }
      }
    }

    const currentDatabases = new Set(this.getCurrentUserDatabases());
    const databasesToClose = Array.from(previousDatabases).filter(
      (db) => !currentDatabases.has(db)
    );

    if (databasesToClose.length > 0) {
      logger.info("Closing unused database connections", {
        databaseCount: databasesToClose.length,
      });
      for (const dbKey of databasesToClose) {
        if (this.connections[dbKey]) {
          try {
            logger.trace("Disconnecting database", { databaseKey: dbKey });
            await this.connections[dbKey].disconnect();
            delete this.connections[dbKey];
            logger.debug("Database connection closed", { databaseKey: dbKey });
          } catch (error) {
            logger.error("Error closing database connection", {
              databaseKey: dbKey,
              error: (error as Error).message,
            });
          }
        }
      }
    }
  }

  /**
   * Check if current user has access to database
   */
  public static hasAccessToDatabase(dbKey: string): boolean {
    logger.debug("Checking access to database", { databaseKey: dbKey });
    const hasAccess = this.getSchema(dbKey) !== undefined;
    logger.trace("Database access check result", {
      databaseKey: dbKey,
      hasAccess,
    });
    return hasAccess;
  }

  /**
   * Get database connection
   */
  public static get(key: string): MongoUniversalDAO {
    logger.debug("Retrieving database connection", { databaseKey: key });
    if (!this.hasAccessToDatabase(key)) {
      logger.error("Access denied to database", { databaseKey: key });
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    const dao = this.connections[key];
    if (!dao) {
      logger.error("Database not connected", { databaseKey: key });
      throw new Error(
        `Database '${key}' is not connected. Please ensure it's initialized.`
      );
    }

    logger.trace("Database connection retrieved", { databaseKey: key });
    return dao;
  }

  // ========================== EVENT MANAGEMENT ==========================

  /**
   * Register event listener for database reconnection
   */
  public static onDatabaseReconnect(
    schemaName: string,
    callback: (dao: MongoUniversalDAO) => void
  ): void {
    logger.debug("Registering database reconnect listener", { schemaName });
    if (!this.eventListeners.has(schemaName)) {
      this.eventListeners.set(schemaName, []);
    }
    this.eventListeners.get(schemaName)!.push(callback);
    logger.trace("Listener registered", {
      schemaName,
      listenerCount: this.eventListeners.get(schemaName)!.length,
    });
  }

  /**
   * Remove event listener for database reconnection
   */
  public static offDatabaseReconnect(
    schemaName: string,
    callback: (dao: MongoUniversalDAO) => void
  ): void {
    logger.debug("Removing database reconnect listener", { schemaName });
    const listeners = this.eventListeners.get(schemaName);
    if (listeners) {
      const index = listeners.indexOf(callback);
      if (index > -1) {
        listeners.splice(index, 1);
        logger.trace("Listener removed", {
          schemaName,
          listenerCount: listeners.length,
        });
      }
    }
  }

  /**
   * Notify listeners of database reconnection
   */
  private static notifyDatabaseReconnect(
    schemaName: string,
    dao: MongoUniversalDAO
  ): void {
    logger.debug("Notifying database reconnect listeners", { schemaName });
    const listeners = this.eventListeners.get(schemaName);
    if (listeners) {
      listeners.forEach((callback) => {
        try {
          logger.trace("Invoking reconnect listener", { schemaName });
          callback(dao);
        } catch (error) {
          logger.error("Error in reconnect listener callback", {
            schemaName,
            error: (error as Error).message,
          });
        }
      });
    }
  }

  // ========================== DATABASE OPERATIONS ==========================

  /**
   * Close all connections
   */
  private static async closeAllConnections(): Promise<void> {
    logger.info("Closing all database connections", {
      connectionCount: Object.keys(this.connections).length,
    });
    if (this.isClosingConnections) {
      logger.debug("Connections already being closed, skipping");
      return;
    }

    this.isClosingConnections = true;
    try {
      const currentActiveDb = Object.keys(this.connections);
      currentActiveDb.forEach((dbKey) => this.activeDatabases.add(dbKey));
      logger.debug("Saved active databases", {
        activeDatabases: currentActiveDb,
      });

      const closePromises = Object.entries(this.connections).map(
        async ([dbKey, dao]) => {
          try {
            logger.trace("Closing connection", { databaseKey: dbKey });
            await dao.disconnect();
            logger.debug("Connection closed", { databaseKey: dbKey });
          } catch (error) {
            logger.error("Error closing connection", {
              databaseKey: dbKey,
              error: (error as Error).message,
            });
          }
        }
      );

      await Promise.all(closePromises);
      this.connections = {};
      logger.info("All connections closed successfully");
    } finally {
      this.isClosingConnections = false;
      logger.debug("Connection closing process completed");
    }
  }

  /**
   * Reopen connections
   */
  public static async reopenConnections(): Promise<void> {
    logger.info("Reopening database connections", {
      activeDatabaseCount: this.activeDatabases.size,
    });
    try {
      await this.initializeCoreConnection();
      logger.debug("Core connection reopened");

      if (this.currentUserRoles.length > 0) {
        await this.initializeUserRoleConnections();
        logger.debug("User role connections reopened");
      }

      const activeDbArray = Array.from(this.activeDatabases);
      if (activeDbArray.length > 0) {
        for (const dbKey of activeDbArray) {
          if (!this.connections[dbKey]) {
            const schema = this.getSchema(dbKey);
            if (schema) {
              try {
                logger.debug("Reopening connection", { databaseKey: dbKey });
                const dao = await MongoDatabaseFactory.createFromSchema(
                  schema,
                  this.defaultConnectionString,
                  this.connectionOptions
                );
                this.connections[dbKey] = dao;
                logger.info("Connection reopened", { databaseKey: dbKey });
                this.notifyDatabaseReconnect(dbKey, dao);
              } catch (error) {
                logger.error("Error reopening connection", {
                  databaseKey: dbKey,
                  error: (error as Error).message,
                });
              }
            }
          } else {
            logger.debug("Notifying existing connection", {
              databaseKey: dbKey,
            });
            this.notifyDatabaseReconnect(dbKey, this.connections[dbKey]);
          }
        }
      }
    } catch (error) {
      logger.error("Error reopening connections", {
        error: (error as Error).message,
      });
      throw error;
    }
  }

  /**
   * Ensure database connection exists and is active
   */
  public static async ensureDatabaseConnection(
    key: string
  ): Promise<MongoUniversalDAO> {
    logger.debug("Ensuring database connection", { databaseKey: key });
    this.activeDatabases.add(key);

    if (!this.hasAccessToDatabase(key)) {
      logger.error("Access denied to database", { databaseKey: key });
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    if (this.connections[key]) {
      try {
        const isConnected = this.connections[key].isConnectionOpen();
        logger.trace("Connection status checked", {
          databaseKey: key,
          isConnected,
        });
        if (isConnected) {
          logger.debug("Connection is active", { databaseKey: key });
          return this.connections[key];
        } else {
          logger.warn("Connection inactive, cleaning up", { databaseKey: key });
          try {
            await this.connections[key].disconnect().catch(() => {});
            logger.debug("Inactive connection disconnected", {
              databaseKey: key,
            });
          } catch (error) {
            logger.error("Error disconnecting inactive connection", {
              databaseKey: key,
              error: (error as Error).message,
            });
          }
          delete this.connections[key];
        }
      } catch (error) {
        logger.error("Error checking connection status", {
          databaseKey: key,
          error: (error as Error).message,
        });
        delete this.connections[key];
      }
    }

    logger.debug("Creating new connection via lazy loading", {
      databaseKey: key,
    });
    return await this.getLazyLoading(key);
  }

  /**
   * Get all connections
   */
  public static getConnections(): MongoDatabaseConnections {
    logger.debug("Retrieving all connections", {
      connectionCount: Object.keys(this.connections).length,
    });
    return { ...this.connections };
  }

  /**
   * Open all existing databases
   */
  public static async openAllExisting(
    databaseKeys: string[]
  ): Promise<boolean> {
    logger.info("Opening all existing databases", {
      databaseCount: databaseKeys.length,
    });
    const failedOpens: { key: string; error: Error }[] = [];

    for (const key of databaseKeys) {
      try {
        const schema = this.getSchema(key);
        if (!schema) {
          logger.error("Schema not found for database", { databaseKey: key });
          throw new Error(`Invalid database key: ${key}. Schema not found.`);
        }

        logger.debug("Opening database connection", { databaseKey: key });
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );

        this.connections[key] = dao;
        logger.info("Database connection opened", { databaseKey: key });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error("Error opening database", {
          databaseKey: key,
          error: err.message,
        });
        failedOpens.push({ key, error: err });
      }
    }

    if (failedOpens.length > 0) {
      const errorSummary = failedOpens
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      logger.error("Failed to open databases", {
        failedCount: failedOpens.length,
        errorSummary,
      });
      throw new Error(`Failed to open one or more databases:\n${errorSummary}`);
    }

    this.isInitialized = true;
    logger.info("All existing databases opened successfully", {
      databaseCount: databaseKeys.length,
    });
    return true;
  }

  /**
   * Initialize databases lazily
   */
  public static async initLazySchema(databaseKeys: string[]): Promise<boolean> {
    logger.info("Initializing lazy schemas", {
      databaseCount: databaseKeys.length,
    });
    const invalidKeys = databaseKeys.filter((key) => !this.getSchema(key));
    if (invalidKeys.length > 0) {
      logger.error("Invalid database keys found", { invalidKeys });
      throw new Error(
        `Invalid database keys: ${invalidKeys.join(", ")}. Schemas not found.`
      );
    }

    const newConnectionsCount = databaseKeys.filter(
      (key) => !this.connections[key]
    ).length;
    const currentConnectionsCount = Object.keys(this.connections).length;

    if (currentConnectionsCount + newConnectionsCount > this.maxConnections) {
      logger.error("Cannot initialize new connections due to max limit", {
        newConnectionsCount,
        currentConnectionsCount,
        maxConnections: this.maxConnections,
      });
      throw new Error(
        `Cannot initialize ${newConnectionsCount} new connections. Would exceed maximum of ${this.maxConnections} connections. Current: ${currentConnectionsCount}`
      );
    }

    const failedInitializations: { key: string; error: Error }[] = [];
    const initPromises = databaseKeys.map(async (key) => {
      if (this.connections[key]) {
        logger.debug("Connection already initialized, skipping", {
          databaseKey: key,
        });
        return;
      }

      try {
        const schema = this.getSchema(key)!;
        logger.debug("Initializing database connection", { databaseKey: key });
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );
        this.connections[key] = dao;
        logger.info("Database connection initialized", { databaseKey: key });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error("Error initializing database", {
          databaseKey: key,
          error: err.message,
        });
        failedInitializations.push({ key, error: err });
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      logger.error("Failed to initialize databases", {
        failedCount: failedInitializations.length,
        errorSummary,
      });
      throw new Error(
        `Failed to initialize one or more databases:\n${errorSummary}`
      );
    }

    if (Object.keys(this.connections).length > 0) {
      this.isInitialized = true;
      logger.info("Lazy schema initialization completed", {
        connectionCount: Object.keys(this.connections).length,
      });
    }

    return true;
  }

  /**
   * Initialize all available databases
   */
  public static async initializeAll(): Promise<void> {
    logger.info("Initializing all available databases");
    if (this.isInitialized) {
      logger.debug("Already initialized, skipping");
      return;
    }

    const availableSchemas = this.getAvailableSchemas();
    logger.debug("Retrieved available schemas", {
      schemaCount: availableSchemas.length,
    });
    const failedInitializations: { key: string; error: Error }[] = [];

    const initPromises = availableSchemas.map(async (key) => {
      try {
        const schema = this.getSchema(key)!;
        logger.debug("Initializing database connection", { databaseKey: key });
        const dao = await MongoDatabaseFactory.createFromSchema(
          schema,
          this.defaultConnectionString,
          this.connectionOptions
        );
        this.connections[key] = dao;
        logger.info("Database connection initialized", { databaseKey: key });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error("Error initializing database", {
          databaseKey: key,
          error: err.message,
        });
        failedInitializations.push({ key, error: err });
      }
    });

    await Promise.all(initPromises);

    if (failedInitializations.length > 0) {
      this.isInitialized = false;
      const errorSummary = failedInitializations
        .map((f) => `  - ${f.key}: ${f.error.message}`)
        .join("\n");
      logger.error("Failed to initialize databases", {
        failedCount: failedInitializations.length,
        errorSummary,
      });
      throw new Error(
        `Failed to initialize one or more databases:\n${errorSummary}`
      );
    }

    this.isInitialized = true;
    logger.info("All databases initialized successfully", {
      connectionCount: Object.keys(this.connections).length,
    });
  }

  /**
   * Get database with lazy loading
   */
  public static async getLazyLoading(key: string): Promise<MongoUniversalDAO> {
    logger.debug("Getting database with lazy loading", { databaseKey: key });
    this.activeDatabases.add(key);

    if (!this.hasAccessToDatabase(key)) {
      logger.error("Access denied to database", { databaseKey: key });
      throw new Error(`Access denied: Database '${key}' is not accessible.`);
    }

    if (!this.connections[key]) {
      const schema = this.getSchema(key);
      if (!schema) {
        logger.error("Schema not found for database", { databaseKey: key });
        throw new Error(`Invalid database key: ${key}. Schema not found.`);
      }

      if (Object.keys(this.connections).length >= this.maxConnections) {
        logger.error("Maximum connections reached", {
          currentConnectionCount: Object.keys(this.connections).length,
          maxConnections: this.maxConnections,
        });
        throw new Error("Maximum number of database connections reached");
      }

      logger.debug("Creating new connection", { databaseKey: key });
      const dao = await MongoDatabaseFactory.createFromSchema(
        schema,
        this.defaultConnectionString,
        this.connectionOptions
      );
      this.connections[key] = dao;
      logger.info("New connection created via lazy loading", {
        databaseKey: key,
      });
    }

    this.isInitialized = true;
    logger.trace("Returning database connection", { databaseKey: key });
    return this.connections[key];
  }

  // ========================== TRANSACTION OPERATIONS ==========================

  /**
   * Execute cross-schema transaction
   */
  public static async executeCrossSchemaTransaction(
    schemas: string[],
    callback: (daos: Record<string, MongoUniversalDAO>) => Promise<void>
  ): Promise<void> {
    logger.info("Executing cross-schema transaction", {
      schemaCount: schemas.length,
    });
    for (const key of schemas) {
      if (!this.hasAccessToDatabase(key)) {
        logger.error("Access denied to database in transaction", {
          databaseKey: key,
        });
        throw new Error(`Access denied: Database '${key}' is not accessible.`);
      }
    }

    const daos = schemas.reduce((acc, key) => {
      logger.trace("Retrieving DAO for transaction", { databaseKey: key });
      acc[key] = this.get(key);
      return acc;
    }, {} as Record<string, MongoUniversalDAO>);

    try {
      logger.debug("Beginning transaction for schemas", { schemas });
      await Promise.all(
        Object.values(daos).map((dao) => dao.beginTransaction())
      );

      logger.debug("Executing transaction callback");
      await callback(daos);

      logger.debug("Committing transaction for schemas", { schemas });
      await Promise.all(
        Object.values(daos).map((dao) => dao.commitTransaction())
      );
      logger.info("Cross-schema transaction committed successfully", {
        schemaCount: schemas.length,
      });
    } catch (error) {
      logger.error("Error in cross-schema transaction, rolling back", {
        schemas,
        error: (error as Error).message,
      });
      await Promise.all(
        Object.values(daos).map((dao) => dao.rollbackTransaction())
      );
      throw error;
    }
  }

  // ========================== IMPORT OPERATIONS ==========================

  /**
   * Import data to collection
   */
  public static async importDataToCollection(
    databaseKey: string,
    collectionName: string,
    data: Record<string, any>[],
    options: Partial<ImportOptions> = {}
  ): Promise<ImportResult> {
    logger.info("Importing data to collection", {
      databaseKey,
      collectionName,
      dataCount: data.length,
    });
    if (!this.hasAccessToDatabase(databaseKey)) {
      logger.error("Access denied to database for import", { databaseKey });
      throw new Error(
        `Access denied: Database '${databaseKey}' is not accessible.`
      );
    }

    const dao = this.get(databaseKey);
    try {
      logger.debug("Performing bulk insert", { databaseKey, collectionName });
      const result = await dao.bulkInsert(collectionName, data);
      logger.info("Data import completed", {
        databaseKey,
        collectionName,
        totalRows: data.length,
        successRows: result.rowsAffected,
      });
      return {
        totalRows: data.length,
        successRows: result.rowsAffected,
        errorRows: data.length - result.rowsAffected,
        errors: [],
        executionTime: 0,
      };
    } catch (error) {
      logger.error("Error importing data to collection", {
        databaseKey,
        collectionName,
        error: (error as Error).message,
      });
      throw error;
    }
  }

  /**
   * Bulk import data
   */
  public static async bulkImport(
    importConfigs: MongoDatabaseImportConfig[]
  ): Promise<MongoBulkImportResult> {
    logger.info("Starting bulk import", { configCount: importConfigs.length });
    const startTime = Date.now();
    const result: MongoBulkImportResult = {
      totalDatabases: importConfigs.length,
      successDatabases: 0,
      results: {},
      errors: {},
      executionTime: 0,
    };

    for (const config of importConfigs) {
      const configKey = `${config.databaseKey}.${config.collectionName}`;
      logger.debug("Processing import config", { configKey });

      try {
        if (!this.hasAccessToDatabase(config.databaseKey)) {
          logger.error("Access denied to database for bulk import", {
            databaseKey: config.databaseKey,
          });
          throw new Error(
            `Access denied: Database '${config.databaseKey}' is not accessible.`
          );
        }

        logger.debug("Importing data for config", { configKey });
        const importResult = await this.importDataToCollection(
          config.databaseKey,
          config.collectionName,
          config.data,
          config.options
        );

        result.results[configKey] = importResult;
        result.successDatabases++;
        logger.info("Import successful for config", {
          configKey,
          successRows: importResult.successRows,
        });
      } catch (error) {
        const err = error instanceof Error ? error : new Error(String(error));
        logger.error("Error in bulk import for config", {
          configKey,
          error: err.message,
        });
        result.errors[configKey] = err;
      }
    }

    result.executionTime = Date.now() - startTime;
    logger.info("Bulk import completed", {
      totalDatabases: result.totalDatabases,
      successDatabases: result.successDatabases,
      executionTime: result.executionTime,
    });
    return result;
  }

  // ========================== UTILITY METHODS ==========================

  /**
   * Get connection count
   */
  public static getConnectionCount(): number {
    const count = Object.keys(this.connections).length;
    logger.debug("Retrieving connection count", { connectionCount: count });
    return count;
  }

  /**
   * List all active connections
   */
  public static listConnections(): string[] {
    const connections = Object.keys(this.connections);
    logger.debug("Listing active connections", {
      connectionCount: connections.length,
    });
    return connections;
  }

  /**
   * Close specific connection
   */
  public static async closeConnection(dbKey: string): Promise<void> {
    logger.info("Closing specific connection", { databaseKey: dbKey });
    const dao = this.connections[dbKey];
    if (dao) {
      try {
        logger.debug("Disconnecting database", { databaseKey: dbKey });
        await dao.disconnect();
        delete this.connections[dbKey];
        logger.info("Connection closed successfully", { databaseKey: dbKey });
      } catch (error) {
        logger.error("Error closing connection", {
          databaseKey: dbKey,
          error: (error as Error).message,
        });
        throw error;
      }
    } else {
      logger.warn("No connection found to close", { databaseKey: dbKey });
    }
  }

  /**
   * Close all connections and reset state
   */
  public static async closeAll(): Promise<void> {
    logger.info("Closing all connections and resetting state");
    await this.closeAllConnections();

    this.currentUserRoles = [];
    this.currentRole = null;
    this.isInitialized = false;
    this.activeDatabases.clear();
    this.eventListeners.clear();
    this.isClosingConnections = false;
    logger.info("All connections closed and state reset");
  }

  /**
   * Logout user - close role-specific connections
   */
  public static async logout(): Promise<void> {
    logger.info("Logging out user, closing role-specific connections");
    const connectionsToClose = Object.keys(this.connections).filter(
      (key) => key !== "core"
    );

    for (const dbKey of connectionsToClose) {
      try {
        logger.debug("Closing connection", { databaseKey: dbKey });
        await this.connections[dbKey].disconnect();
        delete this.connections[dbKey];
        logger.info("Connection closed during logout", { databaseKey: dbKey });
      } catch (error) {
        logger.error("Error closing connection during logout", {
          databaseKey: dbKey,
          error: (error as Error).message,
        });
      }
    }

    this.currentUserRoles = [];
    this.currentRole = null;
    logger.info("User logout completed");
  }
}
