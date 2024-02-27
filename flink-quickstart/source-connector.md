### source 分类

- 读取文件
```text
org.apache.flink.api.java.ExecutionEnvironment.readCsvFile

org.apache.flink.api.java.ExecutionEnvironment.readTextFile(java.lang.String)
```
- 基于Collections （一般用来进行本地调试或验证）
```text
org.apache.flink.api.java.ExecutionEnvironment.fromCollection(java.util.Collection<X>)

org.apache.flink.api.java.ExecutionEnvironment.fromElements(X...)

```

- 基于Socket (nc -lk 8000)
```text
org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.socketTextStream(java.lang.String, int)
```

- 自定义Source (实现 Flink的SourceFunction 或者 ParallelSourceFunction)
```java
//StreamExecutionEnvironment.addSource(org.apache.flink.streaming.api.functions.source.SourceFunction<OUT>)
public class MyStreamingSource implements SourceFunction<MyStreamingSource.Item> {

    private boolean isRunning = true;

    private static Random random = new Random();

    private static List<String> types = Arrays.asList("服装", "电子", "食品");

    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while (isRunning) {
            Item item = generateItem();
            ctx.collect(item);
            TimeUnit.SECONDS.sleep(1L);
        }
    }

    private Item generateItem() {
        int i = random.nextInt(1000);
        Item item = new Item();
        item.setItemId(i);
        item.setItemName("name-" + i);
        item.setItemType(types.get(i % 3));
        return item;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    public static class Item {

        private String itemType;
        private String itemName;
        private Integer itemId;

        public String getItemType() {
            return itemType;
        }

        public void setItemType(String itemType) {
            this.itemType = itemType;
        }

        public String getItemName() {
            return itemName;
        }

        public void setItemName(String itemName) {
            this.itemName = itemName;
        }

        public Integer getItemId() {
            return itemId;
        }

        public void setItemId(Integer itemId) {
            this.itemId = itemId;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "itemType='" + itemType + '\'' +
                    ", itemName='" + itemName + '\'' +
                    ", itemId=" + itemId +
                    '}';
        }
    }
}
```

### 连接器

**使用这些连接器时通常需要引用对应的Jar包依赖，对于某些连接器比如Kafka是有版本要求的
，一定要去官方网站找到对应的依赖版本**

- Apache Kafka
- Apache Cassandra
- Elasticsearch
- Hadoop FileSystem

- Redis连接器
```text

```

- 基于异步I/O和可查询状态
```text

```