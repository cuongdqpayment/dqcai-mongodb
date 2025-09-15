# @dqcai/mongodb

Một thư viện ORM hiện đại cho Node.js giúp chuyển đổi MongoDB (NoSQL) thành mô hình truy vấn có cấu trúc tương tự SQL. Thư viện cung cấp khả năng định nghĩa schema mạnh mẽ, quản lý kết nối thông minh và API truy vấn trực quan.

## Đặc điểm nổi bật

- 🏗️ **Schema-driven**: Định nghĩa cấu trúc database rõ ràng với validation
- 🔄 **SQL-like API**: Truy vấn NoSQL với syntax quen thuộc
- 🚀 **Type Safety**: Hỗ trợ TypeScript đầy đủ
- 🔐 **Transaction**: Multi-document transactions
- 🎯 **Connection Management**: Tự động quản lý kết nối và reconnection
- ⚡ **Performance**: Connection pooling và optimization

## Cài đặt

```bash
npm install @dqcai/mongodb
```

## Kiến trúc thư viện

### 1. **MongoAdapter**
- Quản lý kết nối MongoDB với connection pooling
- Tự động reconnection khi mất kết nối
- Hỗ trợ cấu hình kết nối linh hoạt

### 2. **MongoUniversalDAO**
- Cung cấp các operation cơ bản: CRUD, transaction, schema management
- Hỗ trợ MongoDB aggregation pipeline
- API nhất quán và dễ sử dụng

### 3. **MongoBaseService**
- Lớp base cho các service cụ thể
- Tích hợp sẵn transaction support
- Hỗ trợ MongoDB query patterns và best practices

### 4. **MongoDatabaseFactory**
- Factory pattern để tạo và quản lý DAO instances
- Tự động khởi tạo schema và indexes
- Quản lý lifecycle của database connections

## Tính năng chính

### Schema Management
- Tạo và quản lý collections
- Tự động tạo indexes
- Validation rules với JSON Schema
- Migration support

### Transaction Support
- Multi-document ACID transactions
- Automatic rollback on error
- Session management

### CRUD Operations
- Insert, update, delete, find với MongoDB native syntax
- Batch operations
- Upsert support

### Aggregation Pipeline
- Hỗ trợ đầy đủ aggregation framework
- Query optimization
- Result transformation

## Hướng dẫn sử dụng

### 1. Định nghĩa Schema

```typescript
import { MongoDatabaseSchema } from '@dqcai/mongodb';

const schema: MongoDatabaseSchema = {
  version: "1.0.0",
  database_name: "my_app",
  description: "Ứng dụng quản lý người dùng",
  collections: {
    users: {
      name: "users",
      indexes: [
        {
          name: "email_unique",
          keys: { email: 1 },
          options: { unique: true }
        },
        {
          name: "name_text_search",
          keys: { name: "text" }
        }
      ],
      validation: {
        $jsonSchema: {
          bsonType: "object",
          required: ["name", "email"],
          properties: {
            name: { 
              bsonType: "string",
              minLength: 2,
              maxLength: 100
            },
            email: { 
              bsonType: "string",
              pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
            },
            age: {
              bsonType: "int",
              minimum: 0,
              maximum: 150
            }
          }
        }
      }
    },
    posts: {
      name: "posts",
      indexes: [
        {
          name: "user_id_index",
          keys: { userId: 1 }
        },
        {
          name: "content_text_search",
          keys: { title: "text", content: "text" }
        }
      ]
    }
  }
};
```

### 2. Khởi tạo Database Connection

```typescript
import { MongoDatabaseFactory } from '@dqcai/mongodb';

const connectionString = "mongodb://localhost:27017";
const dao = await MongoDatabaseFactory.createFromSchema(schema, connectionString);
```

### 3. Tạo Service Layer

```typescript
import { MongoBaseService, MongoUniversalDAO } from '@dqcai/mongodb';

interface User {
  _id?: string;
  name: string;
  email: string;
  age?: number;
  createdAt?: Date;
}

class UserService extends MongoBaseService<User> {
  constructor(dao: MongoUniversalDAO) {
    super(dao, 'users');
  }

  // Tìm user theo email
  async findByEmail(email: string): Promise<User | null> {
    return await this.findOne({ email });
  }

  // Tìm tất cả người dùng trưởng thành
  async findAdults(): Promise<User[]> {
    return await this.findMany({ age: { $gte: 18 } });
  }

  // Tìm kiếm theo tên (text search)
  async searchByName(searchTerm: string): Promise<User[]> {
    return await this.findMany({ 
      $text: { $search: searchTerm } 
    });
  }

  // Cập nhật tuổi của user
  async updateAge(userId: string, newAge: number): Promise<User | null> {
    return await this.update(
      { _id: userId }, 
      { $set: { age: newAge } }
    );
  }

  // Aggregation - thống kê độ tuổi
  async getAgeStatistics() {
    return await this.aggregate([
      {
        $group: {
          _id: null,
          avgAge: { $avg: "$age" },
          minAge: { $min: "$age" },
          maxAge: { $max: "$age" },
          totalUsers: { $sum: 1 }
        }
      }
    ]);
  }
}
```

### 4. Sử dụng Service

```typescript
// Khởi tạo service
const userService = new UserService(dao);
await userService.init();

// Tạo user mới
const newUser = await userService.create({
  name: "Nguyễn Văn A",
  email: "nguyenvana@example.com",
  age: 25,
  createdAt: new Date()
});

// Tìm user theo email
const user = await userService.findByEmail("nguyenvana@example.com");

// Tìm tất cả người trưởng thành
const adults = await userService.findAdults();

// Tìm kiếm theo tên
const searchResults = await userService.searchByName("Nguyễn");

// Cập nhật thông tin
const updatedUser = await userService.updateAge(newUser._id, 26);

// Thống kê
const stats = await userService.getAgeStatistics();
console.log('Thống kê độ tuổi:', stats);
```

### 5. Transaction Support

```typescript
import { MongoTransaction } from '@dqcai/mongodb';

class OrderService extends MongoBaseService<Order> {
  constructor(dao: MongoUniversalDAO, private userService: UserService) {
    super(dao, 'orders');
  }

  async createOrderWithTransaction(userId: string, orderData: any) {
    const transaction = new MongoTransaction(this.dao);
    
    try {
      await transaction.start();

      // Tạo order
      const order = await this.create({
        userId,
        ...orderData,
        status: 'pending'
      }, { session: transaction.session });

      // Cập nhật user statistics
      await this.userService.update(
        { _id: userId },
        { $inc: { totalOrders: 1 } },
        { session: transaction.session }
      );

      await transaction.commit();
      return order;
    } catch (error) {
      await transaction.rollback();
      throw error;
    }
  }
}
```

## API Reference

### MongoBaseService Methods

- `create(data, options?)` - Tạo document mới
- `findOne(filter, options?)` - Tìm một document
- `findMany(filter, options?)` - Tìm nhiều documents
- `update(filter, update, options?)` - Cập nhật document
- `delete(filter, options?)` - Xóa document
- `aggregate(pipeline, options?)` - Thực hiện aggregation
- `count(filter, options?)` - Đếm số lượng documents

### Connection Options

```typescript
const options = {
  maxPoolSize: 10,
  serverSelectionTimeoutMS: 5000,
  socketTimeoutMS: 45000,
  family: 4 // Use IPv4, skip trying IPv6
};

const dao = await MongoDatabaseFactory.createFromSchema(
  schema, 
  connectionString, 
  options
);
```

## Best Practices

1. **Luôn định nghĩa schema** trước khi sử dụng
2. **Sử dụng indexes** cho các truy vấn thường xuyên
3. **Validation** dữ liệu ở cấp database
4. **Transaction** cho các operations phức tạp
5. **Connection pooling** để tối ưu hiệu suất

## Contributing

Chúng tôi hoan nghênh các đóng góp từ cộng đồng. Vui lòng đọc [CONTRIBUTING.md](CONTRIBUTING.md) để biết thêm chi tiết.

## License

MIT License - xem file [LICENSE](LICENSE) để biết thêm chi tiết.