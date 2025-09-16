// src/index.ts - Main exports for UniversalMongodb Library with Logger Integration
// ========================== LOGGER EXPORTS ==========================
export {
  MongoLoggerConfig,
  MongoModules,
  mongoLogger,
  createModuleLogger,
} from "./logger/logger-config";

// ========================== CORE EXPORTS ==========================
export { MongoUniversalDAO } from "./core/universal-dao";
export { MongoDatabaseFactory } from "./core/database-factory";
export { MongoDatabaseManager } from "./core/database-manager";
export { MongoBaseService } from "./core/base-service";
export { BaseMongoAdapter } from "./adapters/base-adapter";

// ========================== TYPE EXPORTS ==========================
export * from "./types";


