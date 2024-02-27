### Flink支持的窗口函数

- ReduceFunction 增量聚合
- AggregateFunction 增量聚合
- ProcessWindowFunction 全量聚合

### Flink 支持了三种类型的剔除器

https://www.jianshu.com/p/574800567914

- CountEvictor：数量剔除器
```text
CountEvictor 是数量剔除器，在 Window 中保留指定数量的元素，并从窗口头部开始丢弃其余元素。
```
- DeltaEvictor：阈值剔除器
```text
DeltaEvictor 是阈值剔除器，计算 Window 中最后一个元素与其余每个元素之间的增量，丢弃增量大于或等于阈值的元素。
```
- TimeEvictor：时间剔除器
```text
TimeEvictor 是时间剔除器，保留 Window 中最近一段时间内的元素，并丢弃其余元素。
```