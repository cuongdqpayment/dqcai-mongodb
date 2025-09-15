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
export { MongoBaseService } from "./core/base-service";
export { MongoAdapter } from "./adapters/mongo-adapter";

// ========================== TYPE EXPORTS ==========================
export * from "./types";


