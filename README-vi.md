# @dqcai/mongodb - Universal MongoDb adapter for Node.js with a unified API.

![Universal Mongo](https://img.shields.io/badge/Mongo-Universal-blue)
![TypeScript](https://img.shields.io/badge/TypeScript-Ready-blue)
![Cross Platform](https://img.shields.io/badge/Platform-Universal-green)
![NPM Version](https://img.shields.io/npm/v/@dqcai/mongodb)
![NPM Downloads](https://img.shields.io/npm/dm/@dqcai/mongodb)

Một thư viện ORM hiện đại cho Node.js giúp chuyển đổi MongoDB (NoSQL) thành mô hình truy vấn có cấu trúc tương tự SQL. Thư viện cung cấp khả năng định nghĩa schema mạnh mẽ, quản lý kết nối thông minh và API truy vấn trực quan.

## Đặc điểm nổi bật

- 🏗️ **Schema-driven**: Định nghĩa cấu trúc database rõ ràng với validation
- 🔄 **SQL-like API**: Truy vấn NoSQL với syntax quen thuộc
- 🚀 **Type Safety**: Hỗ trợ TypeScript đầy đủ
- 🔐 **Transaction**: Multi-document transactions
- 🎯 **Connection Management**: Tự động quản lý kết nối và reconnection
- ⚡ **Performance**: Connection pooling và optimization

## Tính năng chính

- **Universal DAO Pattern**: CRUD operations đơn giản và mạnh mẽ
- **Schema Migration**: Chuyển đổi từ Mongo schema sang MongoDB
- **Transaction Support**: Hỗ trợ MongoDB transactions
- **Logging Integration**: Tích hợp @dqcai/logger
- **Base Service Pattern**: Template cho business logic
- **Type Safety**: TypeScript support đầy đủ

## Cài đặt

```bash
npm install @dqcai/mongodb mongodb
```

## Hướng dẫn sử dụng

## Quick Start

### 1. Kết nối cơ bản

```typescript
import { MongoDatabaseFactory, MongoUniversalDAO } from '@dqcai/mongodb';

// Tạo DAO instance
const dao = MongoDatabaseFactory.createDAO(
  'mongodb://localhost:27017',
  'myapp'
);

// Kết nối
await dao.connect();

// Sử dụng
const users = await dao.find('users', { status: 'active' });
console.log(users);

// Đóng kết nối
await dao.disconnect();
```

### 2. Sử dụng với Schema

```typescript
import { DatabaseSchema } from '@dqcai/mongodb';

// Định nghĩa schema (tương thích Mongo)
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
    },
    posts: {
      cols: [
        { name: "id", type: "string", primary_key: true },
        { name: "title", type: "string", nullable: false },
        { name: "content", type: "text" },
        { name: "user_id", type: "string" },
        { name: "published", type: "boolean", default: false }
      ]
    }
  }
};

// Tạo DAO từ schema
const dao = await MongoDatabaseFactory.createFromSchema(
  appSchema,
  'mongodb://localhost:27017'
);
```

## CRUD Operations

### Insert Operations

```typescript
// Insert một document
const newUser = await dao.insert('users', {
  name: 'John Doe',
  email: 'john@example.com',
  age: 30
});

// Insert nhiều documents
const users = await dao.insertMany('users', [
  { name: 'Alice', email: 'alice@example.com', age: 25 },
  { name: 'Bob', email: 'bob@example.com', age: 35 }
]);
```

### Find Operations

```typescript
// Tìm tất cả
const allUsers = await dao.find('users');

// Tìm với filter
const activeUsers = await dao.find('users', 
  { status: 'active' },
  { sort: { created_at: -1 }, limit: 10 }
);

// Tìm một document
const user = await dao.findOne('users', { email: 'john@example.com' });

// Đếm documents
const userCount = await dao.count('users', { age: { $gte: 18 } });
```

### Update Operations

```typescript
// Update một document
await dao.update('users', 
  { email: 'john@example.com' },
  { $set: { age: 31, last_login: new Date() } }
);

// Update nhiều documents
await dao.update('users',
  { status: 'pending' },
  { $set: { status: 'active' } },
  { multi: true }
);
```

### Delete Operations

```typescript
// Xóa một document
await dao.delete('users', { email: 'john@example.com' });

// Xóa nhiều documents
await dao.delete('users', 
  { last_login: { $lt: new Date('2024-01-01') } },
  { multi: true }
);
```

## Base Service Pattern

### Tạo Service Class

```typescript
import { MongoBaseService, MongoUniversalDAO } from '@dqcai/mongodb';

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

  // Custom methods
  async findByEmail(email: string): Promise<User | null> {
    return await this.findOne({ email });
  }

  async findAdults(): Promise<User[]> {
    return await this.findMany({ age: { $gte: 18 } });
  }

  async updateLastLogin(userId: string): Promise<boolean> {
    return await this.updateById(userId, {
      last_login: new Date()
    });
  }

  // Validation override
  protected validateDocument(user: Partial<User>) {
    const errors: string[] = [];
    
    if (!user.name?.trim()) {
      errors.push('Name is required');
    }
    
    if (!user.email?.includes('@')) {
      errors.push('Valid email is required');
    }
    
    if (user.age && (user.age < 0 || user.age > 120)) {
      errors.push('Age must be between 0 and 120');
    }
    
    return {
      isValid: errors.length === 0,
      errors
    };
  }
}
```

### Sử dụng Service

```typescript
// Khởi tạo
const dao = await MongoDatabaseFactory.createFromSchema(
  appSchema, 
  'mongodb://localhost:27017'
);
const userService = new UserService(dao);

// Sử dụng built-in methods
const newUser = await userService.create({
  name: 'Jane Doe',
  email: 'jane@example.com',
  age: 28
});

const users = await userService.findWithPagination(
  { age: { $gte: 18 } },
  1, // page
  10 // page size
);

// Sử dụng custom methods
const user = await userService.findByEmail('jane@example.com');
const adults = await userService.findAdults();
```

## Transaction Support

```typescript
// Sử dụng transaction trong service
await userService.executeTransaction(async () => {
  // Tạo user mới
  const user = await userService.create({
    name: 'Transaction User',
    email: 'trans@example.com',
    age: 30
  });
  
  // Tạo post cho user
  const postService = new PostService(dao);
  await postService.create({
    title: 'First Post',
    content: 'Hello World',
    user_id: user._id,
    published: true
  });
  
  // Nếu có lỗi, cả hai operations sẽ được rollback
});

// Hoặc sử dụng trực tiếp với DAO
await dao.beginTransaction();
try {
  await dao.insert('users', userData);
  await dao.insert('posts', postData);
  await dao.commitTransaction();
} catch (error) {
  await dao.rollbackTransaction();
  throw error;
}
```

## Advanced Queries

### Aggregation

```typescript
// Group by và count
const usersByAge = await userService.aggregate([
  { $group: { _id: '$age', count: { $sum: 1 } } },
  { $sort: { count: -1 } }
]);

// Thống kê
const stats = await userService.getFieldStats('age', { status: 'active' });
console.log(stats); // { count, sum, avg, min, max }

// Lookup với collection khác
const usersWithPosts = await dao.aggregate('users', [
  {
    $lookup: {
      from: 'posts',
      localField: '_id',
      foreignField: 'user_id',
      as: 'posts'
    }
  },
  { $match: { 'posts.0': { $exists: true } } }
]);
```

### Text Search

```typescript
// Tìm kiếm text
const results = await userService.search(
  'john',
  ['name', 'email'],
  { status: 'active' },
  { limit: 20 }
);
```

### Bulk Operations

```typescript
// Bulk insert
const importResult = await userService.bulkInsert(largeUserArray, 1000);
console.log(`Inserted: ${importResult.successRows}/${importResult.totalRows}`);

// Bulk update
const updatedCount = await userService.bulkUpdate(
  { status: 'pending' },
  { $set: { status: 'verified', verified_at: new Date() } }
);
```

## Logging Configuration

```typescript
import { MongoLoggerConfig, MongoModules } from '@dqcai/mongodb';

// Debug mode
MongoLoggerConfig.updateConfiguration(
  MongoLoggerConfig.createDebugConfig()
);

// Production mode
MongoLoggerConfig.updateConfiguration(
  MongoLoggerConfig.createProductionConfig()
);

// Custom configuration
MongoLoggerConfig.enableModule(
  MongoModules.UNIVERSAL_DAO,
  ['info', 'warn', 'error'],
  ['console']
);

// Disable logging
MongoLoggerConfig.setEnabled(false);
```

## Migration từ Mongo

```typescript
// Import data từ Mongo format
await userService.importFromMongo(mongodbRecords, {
  batchSize: 500,
  transformRecord: (record) => ({
    ...record,
    migrated_at: new Date(),
    // Chuyển đổi các field cần thiết
    created_at: new Date(record.created_at)
  }),
  onProgress: (processed, total) => {
    console.log(`Migration progress: ${processed}/${total}`);
  },
  skipErrors: true
});

// Export về Mongo format
const mongodbData = await userService.exportToMongo(
  { status: 'active' },
  {
    limit: 1000,
    transformRecord: (record) => ({
      ...record,
      _id: record._id.toString(),
      created_at: record.created_at.toISOString()
    })
  }
);
```

## Error Handling

```typescript
import { MongoUniversalDAO } from '@dqcai/mongodb';

try {
  const dao = MongoDatabaseFactory.createDAO(connectionString, dbName);
  await dao.connect();
  
  // Operations...
  
} catch (error) {
  if (error.message.includes('connection')) {
    console.error('Database connection failed:', error.message);
  } else if (error.message.includes('validation')) {
    console.error('Data validation failed:', error.message);
  } else {
    console.error('Database operation failed:', error.message);
  }
} finally {
  await dao?.disconnect();
}
```

## Best Practices

### 1. Connection Management

```typescript
// Singleton pattern cho connection
class DatabaseManager {
  private static dao: MongoUniversalDAO;
  
  static async getDAO(): Promise<MongoUniversalDAO> {
    if (!this.dao) {
      this.dao = await MongoDatabaseFactory.createFromSchema(
        schema,
        process.env.MONGODB_URL || 'mongodb://localhost:27017'
      );
    }
    return this.dao;
  }
  
  static async cleanup(): Promise<void> {
    if (this.dao) {
      await this.dao.disconnect();
    }
  }
}
```

### 2. Service Organization

```typescript
// Base application service
export abstract class AppBaseService<T> extends MongoBaseService<T> {
  constructor(collectionName: string) {
    super(DatabaseManager.getDAO(), collectionName);
  }
  
  // Common business logic
  async softDelete(id: string): Promise<boolean> {
    return await this.updateById(id, {
      deleted_at: new Date(),
      status: 'deleted'
    });
  }
}

// Specific services
export class UserService extends AppBaseService<User> {
  constructor() {
    super('users');
  }
  
  // User-specific methods...
}

export class PostService extends AppBaseService<Post> {
  constructor() {
    super('posts');
  }
  
  // Post-specific methods...
}
```

### 3. Environment Configuration

```typescript
// config/database.ts
import { DatabaseSchema } from '@dqcai/mongodb';

export const getDatabaseConfig = () => ({
  connectionString: process.env.MONGODB_URL || 'mongodb://localhost:27017',
  databaseName: process.env.DB_NAME || 'myapp',
  options: {
    maxPoolSize: parseInt(process.env.DB_POOL_SIZE || '10'),
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
  }
});

export const getLoggerConfig = () => {
  const isDevelopment = process.env.NODE_ENV === 'development';
  return isDevelopment 
    ? MongoLoggerConfig.createDebugConfig()
    : MongoLoggerConfig.createProductionConfig();
};
```

## API Reference

### MongoUniversalDAO Methods

| Method | Description |
|--------|-------------|
| `connect()` | Kết nối database |
| `disconnect()` | Đóng kết nối |
| `insert(collection, doc)` | Thêm document |
| `insertMany(collection, docs)` | Thêm nhiều documents |
| `find(collection, filter, options)` | Tìm kiếm |
| `findOne(collection, filter)` | Tìm một document |
| `update(collection, filter, update, options)` | Cập nhật |
| `delete(collection, filter, options)` | Xóa |
| `count(collection, filter)` | Đếm documents |
| `aggregate(collection, pipeline)` | Aggregation |

### MongoBaseService Methods

| Method | Description |
|--------|-------------|
| `create(data)` | Tạo document mới |
| `findById(id)` | Tìm theo ID |
| `findMany(filter, options)` | Tìm nhiều documents |
| `updateById(id, update)` | Cập nhật theo ID |
| `deleteById(id)` | Xóa theo ID |
| `findWithPagination()` | Phân trang |
| `search()` | Tìm kiếm text |
| `bulkInsert()` | Thêm hàng loạt |
| `executeTransaction()` | Thực hiện transaction |

## Troubleshooting

### Connection Issues

```typescript
// Test connection với retry
const dao = await MongoDatabaseFactory.createWithConnectionTest(
  schema,
  connectionString,
  {
    testTimeout: 5000,
    retryAttempts: 3,
    retryDelay: 1000
  }
);
```

### Schema Validation

```typescript
// Validate schema trước khi sử dụng
const validation = MongoDatabaseFactory.validateSchema(schema);
if (!validation.isValid) {
  console.error('Schema errors:', validation.errors);
}
```

### Performance Monitoring

```typescript
// Enable debug logging
MongoLoggerConfig.updateConfiguration(
  MongoLoggerConfig.createDebugConfig()
);

// Monitor operations
const startTime = Date.now();
const results = await dao.find('users', filter);
console.log(`Query took: ${Date.now() - startTime}ms`);
```

## Best Practices

1. **Luôn định nghĩa schema** trước khi sử dụng
2. **Sử dụng indexes** cho các truy vấn thường xuyên
3. **Validation** dữ liệu ở cấp database
4. **Transaction** cho các operations phức tạp
5. **Connection pooling** để tối ưu hiệu suất

## Contributing

Chúng tôi hoan nghênh các đóng góp từ cộng đồng. Vui lòng đọc [CONTRIBUTING.md](CONTRIBUTING.md) để biết thêm chi tiết.

## 🤝 Community & Support

- **GitHub**: [https://github.com/cuongdqpayment/dqcai-mongodb](https://github.com/cuongdqpayment/dqcai-mongodb)
- **NPM**: [https://www.npmjs.com/package/@dqcai/mongodb](https://www.npmjs.com/package/@dqcai/mongodb)
- **Issues**: [GitHub Issues](https://github.com/cuongdqpayment/dqcai-mongodb/issues)
- **Facebook**: [Facebook Page](https://www.facebook.com/share/p/19esHGbaGj/)

## License

MIT License - see [LICENSE](https://github.com/cuongdqpayment/dqcai-mongodb/blob/main/LICENSE) file for details.

**@dqcai/mongodb** - Một thư viện, đáp ứng đủ ORM cho cơ sở dữ liệu MongoDB! 🌟