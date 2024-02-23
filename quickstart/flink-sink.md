### Sink

Flink 的 sink（接收器）是 Flink 数据处理管道中的最后一个环节， 用于将处理后的数据发送到外部系统或存储介质中。 它负责将 Flink 内部的数据格式转换为目标系统所需的格式，并将数据传输到目标位置。

### 常见的 Flink sink 实现包括：

- 数据库 sink：将数据写入关系型数据库（如 MySQL、PostgreSQL）或 NoSQL 数据库（如Cassandra、HBase）。
- 文件系统 sink：将数据写入文件系统（如本地文件、HDFS）。
- 消息队列 sink：将数据发送到消息队列（如 Kafka）。
- 服务接口 sink：将数据发送到其他外部服务的接口，例如REST API。