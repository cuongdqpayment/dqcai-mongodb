# @dqcai/mongodb - Universal MongoDB Adapter for Node.js

<div align="center">

![Universal Mongo](https://img.shields.io/badge/Mongo-Universal-blue)
![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue)
![Cross Platform](https://img.shields.io/badge/Platform-Universal-green)
![NPM Version](https://img.shields.io/npm/v/@dqcai/mongodb)
![NPM Downloads](https://img.shields.io/npm/dm/@dqcai/mongodb)

**Transform MongoDB into a SQL-like experience with modern ORM capabilities**

[📖 Documentation](#-documentation) • [🚀 Quick Start](#-quick-start) • [💡 Examples](#-examples) • [🤝 Contributing](#-contributing)

</div>

---

## 🌟 Why Choose @dqcai/mongodb?

**@dqcai/mongodb** is a revolutionary ORM library that bridges the gap between NoSQL flexibility and SQL familiarity. Built for modern Node.js applications, it transforms your MongoDB operations into an intuitive, type-safe, and highly performant experience.

### ✨ Key Features

- 🏗️ **Schema-Driven Architecture** - Define clear database structures with powerful validation
- 🔄 **SQL-like API** - Query NoSQL with familiar syntax that feels like home
- 🚀 **Full TypeScript Support** - Complete type safety from database to application
- 🔐 **Multi-Document Transactions** - ACID compliance when you need it
- 🎯 **Intelligent Connection Management** - Auto-reconnection and pooling out of the box
- ⚡ **Performance Optimized** - Built for speed with connection pooling and smart caching
- 🛠️ **Universal DAO Pattern** - Consistent CRUD operations across your entire application
- 📊 **Advanced Aggregation** - Complex queries made simple
- 🔍 **Full-Text Search** - Powerful search capabilities built-in

## 🚀 Quick Start

### Installation

```bash
npm install @dqcai/mongodb mongodb
```

### Basic Connection

```typescript
import { MongoDatabaseFactory } from '@dqcai/mongodb';

// Create DAO instance
const dao = MongoDatabaseFactory.createDAO(
  'mongodb://localhost:27017',
  'myapp'
);

// Connect and start using
await dao.connect();
const users = await dao.find('users', { status: 'active' });
console.log(users);
```

### Schema-Powered Development

```typescript
import { DatabaseSchema } from '@dqcai/mongodb';

const appSchema: DatabaseSchema = {
  version: "1.0.0",
  database_name: "myapp",
  description: "My Application Database",
  schemas: {
    users: {
      description: "User management",
      cols: [
        { name: "id", type: "string", primary_key: true },
        { name: "name", type: "string", nullable: false },
        { name: "email", type: "string", unique: true },
        { name: "age", type: "integer" },
        { name: "created_at", type: "timestamp" }
      ],
      indexes: [
        { name: "idx_email", columns: ["email"], unique: true },
        { name: "idx_name", columns: ["name"] }
      ]
    }
  }
};

// Create DAO from schema
const dao = await MongoDatabaseFactory.createFromSchema(
  appSchema,
  'mongodb://localhost:27017'
);
```

## 💡 Examples

### Modern Service Pattern

```typescript
import { MongoBaseService } from '@dqcai/mongodb';

interface User {
  _id?: string;
  name: string;
  email: string;
  age: number;
  created_at?: Date;
}

class UserService extends MongoBaseService<User> {
  constructor(dao: MongoUniversalDAO) {
    super(dao, 'users');
  }

  async findByEmail(email: string): Promise<User | null> {
    return await this.findOne({ email });
  }

  async findAdults(): Promise<User[]> {
    return await this.findMany({ age: { $gte: 18 } });
  }

  protected validateDocument(user: Partial<User>) {
    const errors: string[] = [];
    
    if (!user.name?.trim()) errors.push('Name is required');
    if (!user.email?.includes('@')) errors.push('Valid email required');
    if (user.age && (user.age < 0 || user.age > 120)) {
      errors.push('Age must be between 0 and 120');
    }
    
    return { isValid: errors.length === 0, errors };
  }
}
```

### Transaction Support

```typescript
await userService.executeTransaction(async () => {
  const user = await userService.create({
    name: 'John Doe',
    email: 'john@example.com',
    age: 30
  });
  
  const postService = new PostService(dao);
  await postService.create({
    title: 'First Post',
    content: 'Hello World!',
    user_id: user._id,
    published: true
  });
  
  // Both operations succeed or fail together
});
```

### Advanced Queries & Aggregation

```typescript
// Complex aggregation made simple
const userStats = await userService.aggregate([
  { $group: { _id: '$age', count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 10 }
]);

// Full-text search
const results = await userService.search(
  'john',
  ['name', 'email'],
  { status: 'active' },
  { limit: 20 }
);

// Bulk operations
const importResult = await userService.bulkInsert(
  largeUserArray, 
  1000 // batch size
);
```

## 🏗️ Architecture & Design Patterns

### Universal DAO Pattern
Consistent CRUD operations across all collections with intelligent type inference.

### Service Layer Architecture
Build scalable applications with our battle-tested service pattern that encourages separation of concerns.

### Schema Migration Support
Seamlessly migrate from traditional MongoDB schemas to our structured approach.

## 🔧 Advanced Configuration

### Environment Setup

```typescript
// config/database.ts
export const getDatabaseConfig = () => ({
  connectionString: process.env.MONGODB_URL || 'mongodb://localhost:27017',
  databaseName: process.env.DB_NAME || 'myapp',
  options: {
    maxPoolSize: parseInt(process.env.DB_POOL_SIZE || '10'),
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
  }
});
```

### Logging & Debugging

```typescript
import { MongoLoggerConfig, MongoModules } from '@dqcai/mongodb';

// Development mode
MongoLoggerConfig.updateConfiguration(
  MongoLoggerConfig.createDebugConfig()
);

// Production mode  
MongoLoggerConfig.updateConfiguration(
  MongoLoggerConfig.createProductionConfig()
);
```

## 📚 Documentation

### Core API Methods

| Method | Description | Example |
|--------|-------------|---------|
| `connect()` | Establish database connection | `await dao.connect()` |
| `find(collection, filter, options)` | Query documents | `await dao.find('users', { age: { $gte: 18 } })` |
| `insert(collection, doc)` | Create new document | `await dao.insert('users', userData)` |
| `update(collection, filter, update)` | Update documents | `await dao.update('users', { _id }, { $set: data })` |
| `aggregate(collection, pipeline)` | Complex queries | `await dao.aggregate('users', pipeline)` |

### Service Layer Methods

| Method | Description | Use Case |
|--------|-------------|----------|
| `create(data)` | Create with validation | User registration |
| `findWithPagination()` | Paginated results | List views |
| `bulkInsert()` | Batch operations | Data imports |
| `executeTransaction()` | ACID operations | Complex workflows |
| `search()` | Full-text search | Search functionality |

## 🎯 Best Practices

### 1. **Always Define Schemas**
Use schemas to ensure data consistency and enable powerful validation.

### 2. **Leverage Indexes**
Define indexes in your schema for frequently queried fields.

### 3. **Use Transactions Wisely**  
Apply transactions for multi-document operations that require consistency.

### 4. **Connection Management**
Implement singleton pattern for database connections in production.

### 5. **Error Handling**
Implement comprehensive error handling for robust applications.

## 🔍 Migration Guide

### From Traditional MongoDB

```typescript
// Import existing MongoDB data
await userService.importFromMongo(mongodbRecords, {
  batchSize: 500,
  transformRecord: (record) => ({
    ...record,
    migrated_at: new Date(),
    created_at: new Date(record.created_at)
  }),
  onProgress: (processed, total) => {
    console.log(`Migration progress: ${processed}/${total}`);
  }
});
```

## 🚀 Performance Tips

- Use connection pooling for production environments
- Implement proper indexing strategies
- Leverage bulk operations for large datasets
- Monitor query performance with built-in logging
- Use aggregation pipelines for complex data processing

## 🤝 Contributing

We welcome contributions from the community! Here's how you can help:

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Development Setup
```bash
git clone https://github.com/cuongdqpayment/dqcai-mongodb.git
cd dqcai-mongodb
npm install
npm test
```

### Contributing Guidelines
- Follow TypeScript best practices
- Write comprehensive tests
- Update documentation for new features
- Maintain backwards compatibility
- Follow semantic versioning

## 🌍 Community & Support

Join our growing community of developers who are building amazing applications with @dqcai/mongodb!

- **📂 GitHub**: [Repository & Issues](https://github.com/cuongdqpayment/dqcai-mongodb)
- **📦 NPM**: [Package & Versions](https://www.npmjs.com/package/@dqcai/mongodb)
- **🐛 Bug Reports**: [GitHub Issues](https://github.com/cuongdqpayment/dqcai-mongodb/issues)
- **💬 Community**: [Facebook Page](https://www.facebook.com/share/p/19esHGbaGj/)
- **📧 Contact**: [Email Support](mailto:support@dqcai.com)

### Roadmap

- [ ] GraphQL integration
- [ ] Real-time subscriptions
- [ ] Advanced caching strategies
- [ ] Multi-database support
- [ ] CLI tools for schema management

## 📄 License

MIT License - see the [LICENSE](https://github.com/cuongdqpayment/dqcai-mongodb/blob/main/LICENSE) file for details.

---

**@dqcai/mongodb** - Where MongoDB meets modern development! ✨

*Built with ❤️ by developers, for developers*

[⭐ Star us on GitHub](https://github.com/cuongdqpayment/dqcai-mongodb) • [📖 Read the Docs](#-documentation) • [🚀 Get Started](#-quick-start)
