// src/logger/logger-config.ts - Alternative solution using Proxy pattern

import {
  BaseModule,
  LoggerConfigBuilder,
  createLogger,
  UniversalLogger,
} from "@dqcai/logger";

export { BaseModule };

export enum MongoModules {
  DATABASE_FACTORY = "DatabaseFactory",
  UNIVERSAL_DAO = "UniversalDAO",
  BASE_SERVICE = "BaseService",
}

interface ModuleLogger {
  trace: (message: string, ...args: any[]) => void;
  debug: (message: string, ...args: any[]) => void;
  info: (message: string, ...args: any[]) => void;
  warn: (message: string, ...args: any[]) => void;
  error: (message: string, ...args: any[]) => void;
}

/**
 * Logger Proxy - always delegates to current logger instance
 */
class LoggerProxy implements ModuleLogger {
  constructor(private moduleName: string) {}

  trace(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().trace(this.moduleName, message, ...args);
  }

  debug(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().debug(this.moduleName, message, ...args);
  }

  info(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().info(this.moduleName, message, ...args);
  }

  warn(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().warn(this.moduleName, message, ...args);
  }

  error(message: string, ...args: any[]): void {
    MongoLoggerConfig.getInstance().error(this.moduleName, message, ...args);
  }
}

/**
 * Enhanced Mongo Logger Configuration with automatic update support
 */
export class MongoLoggerConfig {
  private static instance: UniversalLogger | null = null;
  private static currentConfig: any = null;
  // Track proxy instances for debugging
  public static proxyInstances: Map<string, LoggerProxy> = new Map();

  static createDefaultConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("warn")
      .addModule(
        MongoModules.DATABASE_FACTORY,
        true,
        ["warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.UNIVERSAL_DAO,
        true,
        ["warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.BASE_SERVICE,
        true,
        ["warn", "error"],
        ["console"]
      )
      .build();
  }

  static initialize(customConfig?: any): UniversalLogger {
    const config = customConfig || MongoLoggerConfig.createDefaultConfig();
    MongoLoggerConfig.currentConfig = config;

    if (
      config.enabled &&
      (config.defaultLevel === "trace" || config.defaultLevel === "debug")
    ) {
      console.debug(
        `MongoLoggerConfig.initialize() with ${
          customConfig ? "CUSTOM" : "default"
        } config`
      );
    }

    MongoLoggerConfig.instance = createLogger(config);
    return MongoLoggerConfig.instance;
  }

  static getInstance(): UniversalLogger {
    if (!MongoLoggerConfig.instance) {
      return MongoLoggerConfig.initialize();
    }
    return MongoLoggerConfig.instance;
  }

  /**
   * Update configuration - proxy pattern automatically handles updates
   */
  static updateConfiguration(newConfig: any): void {
    if (
      newConfig &&
      newConfig.enabled &&
      (newConfig.defaultLevel === "trace" || newConfig.defaultLevel === "debug")
    ) {
      console.debug(
        "MongoLoggerConfig.updateConfiguration()",
        JSON.stringify(newConfig, null, 2)
      );
    }

    MongoLoggerConfig.currentConfig = newConfig;
    MongoLoggerConfig.instance = createLogger(newConfig);

    // Log update confirmation
    if (
      newConfig &&
      newConfig.enabled &&
      (newConfig.defaultLevel === "trace" ||
        newConfig.defaultLevel === "debug" ||
        newConfig.defaultLevel === "info")
    ) {
      console.log(
        "MongoLoggerConfig.updateConfiguration() - Configuration updated. Proxy loggers will use new settings automatically.",
        `Active proxies: ${Array.from(
          MongoLoggerConfig.proxyInstances.keys()
        )}`
      );
    }
  }

  static setEnabled(enabled: boolean): void {
    if (MongoLoggerConfig.currentConfig) {
      MongoLoggerConfig.currentConfig.enabled = enabled;
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static enableModule(
    moduleName: string,
    levels?: string[],
    appenders?: string[]
  ): void {
    if (
      MongoLoggerConfig.currentConfig &&
      MongoLoggerConfig.currentConfig.modules
    ) {
      MongoLoggerConfig.currentConfig.modules[moduleName] = {
        enabled: true,
        levels: levels || ["debug", "info", "warn", "error"],
        appenders: appenders || ["console"],
      };
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static disableModule(moduleName: string): void {
    if (
      MongoLoggerConfig.currentConfig &&
      MongoLoggerConfig.currentConfig.modules
    ) {
      MongoLoggerConfig.currentConfig.modules[moduleName] = {
        enabled: false,
      };
      MongoLoggerConfig.updateConfiguration(MongoLoggerConfig.currentConfig);
    }
  }

  static createDebugConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("trace")
      .addModule(
        MongoModules.DATABASE_FACTORY,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.UNIVERSAL_DAO,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .addModule(
        MongoModules.BASE_SERVICE,
        true,
        ["trace", "debug", "info", "warn", "error"],
        ["console"]
      )
      .build();
  }

  static createProductionConfig() {
    return new LoggerConfigBuilder()
      .setEnabled(true)
      .setDefaultLevel("error")
      .addModule(MongoModules.DATABASE_FACTORY, true, ["error"], ["console"])
      .addModule(MongoModules.UNIVERSAL_DAO, true, ["error"], ["console"])
      .addModule(MongoModules.BASE_SERVICE, true, ["error"], ["console"])
      .build();
  }

  static reset(): UniversalLogger {
    return MongoLoggerConfig.initialize();
  }

  /**
   * Get active proxy modules
   */
  static getActiveProxyModules(): string[] {
    return Array.from(MongoLoggerConfig.proxyInstances.keys());
  }

  /**
   * Get current configuration (for debugging)
   */
  static getCurrentConfig(): any {
    return MongoLoggerConfig.currentConfig
      ? { ...MongoLoggerConfig.currentConfig }
      : null;
  }
}

export const mongoLogger = MongoLoggerConfig.getInstance();

/**
 * Create module logger using proxy pattern - automatically updates when configuration changes
 */
export const createModuleLogger = (moduleName: string): ModuleLogger => {
  // Check if proxy already exists for this module
  if (MongoLoggerConfig.proxyInstances.has(moduleName)) {
    return MongoLoggerConfig.proxyInstances.get(moduleName)!;
  }

  // Create new proxy
  const proxy = new LoggerProxy(moduleName);
  MongoLoggerConfig.proxyInstances.set(moduleName, proxy);

  return proxy;
};

/**
 * Utility functions for testing and debugging
 */
export const LoggerUtils = {
  /**
   * Test if a module logger responds to configuration changes
   */
  testDynamicUpdate: (moduleName: string): void => {
    const logger = createModuleLogger(moduleName);

    console.log(`\n=== Testing ${moduleName} Logger Dynamic Updates ===`);

    // Test with debug config
    console.log("1. Setting debug configuration...");
    MongoLoggerConfig.updateConfiguration(
      MongoLoggerConfig.createDebugConfig()
    );
    logger.debug("This DEBUG message should be visible");
    logger.info("This INFO message should be visible");

    // Test with production config
    console.log("2. Setting production configuration...");
    MongoLoggerConfig.updateConfiguration(
      MongoLoggerConfig.createProductionConfig()
    );
    logger.debug("This DEBUG message should be HIDDEN");
    logger.info("This INFO message should be HIDDEN");
    logger.error("This ERROR message should be visible");

    // Test module disable
    console.log("3. Disabling specific module...");
    MongoLoggerConfig.disableModule(moduleName);
    logger.error("This ERROR message should be HIDDEN (module disabled)");

    // Test module re-enable
    console.log("4. Re-enabling specific module...");
    MongoLoggerConfig.enableModule(moduleName);
    logger.error("This ERROR message should be visible again");

    console.log(`=== End test for ${moduleName} ===\n`);
  },

  /**
   * Show current logger statistics
   */
  showStats: (): void => {
    console.log("\n=== Logger Statistics ===");
    console.log(
      `Active proxy modules: ${
        MongoLoggerConfig.getActiveProxyModules().length
      }`
    );
    console.log(`Proxy modules:`, MongoLoggerConfig.getActiveProxyModules());
    console.log(
      `Current config enabled:`,
      MongoLoggerConfig.getCurrentConfig()?.enabled
    );
    console.log(
      `Current default level:`,
      MongoLoggerConfig.getCurrentConfig()?.defaultLevel
    );
    console.log("========================\n");
  },
};
