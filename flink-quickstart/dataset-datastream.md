# DataSet DataStream

- DataSet: 批处理，处理有限数据集
- DataStream: 流处理，处理无限数据集

### Source

- DataSet: 对于DataSet而言，Source来源于文件、表或者Java集合
- DataStream: 来源于消息中间件，如Kafka

### 流批一体理解

```text
编写代码
有限数据集使用DataSet API
无限数据集使用DataStream API
机器学习使用Machine Learning API

到Flink执行层是统一的
```

### DataStream API
- map：接收一个元素返回一个元素
```java
public interface MapFunction<T, O> extends Function, Serializable {
    O map(T var1) throws Exception;
}
//访问RuntimeContext-广播变量、累加器
//访问Configuration-参数信息
public abstract class AbstractRichFunction implements RichFunction, Serializable {
    private transient RuntimeContext runtimeContext;
    
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    public void open(Configuration parameters) throws Exception {
    }

    public void close() throws Exception {
    }
}
```

- flatmap：接收一个元素返回一个多个元素，如果返回列表会把列表平铺
- filter：过滤元素
- keyBy：根据某个属性或某个字段进行分组，然后对不通的组进行处理，返回KeyedStream，用于后续Reduce风格的算子：sum、reduce、fold
```java
// 可能会造成数据倾斜，常见的使用方法是把所有的数据加上随机的前后缀
public class KeyedStream<T, KEY> extends DataStream<T> {
    
}
```
- aggregations-max：返回最大值 (传入数字表是第几个元素，也可以传入字段属性)
- aggregations-maxBy：返回最大值 (传入数字表是第几个元素，也可以传入字段属性)
- aggregations-min：返回最小值
- aggregations-sum：累加
```text
max、maxBy都返回整个元素
max: 返回指定字段的最大值，其他字段不能保证数值正确
```
- reduce
```text

```