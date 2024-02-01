# flink-cookbook

### Flink 核心概念

- 并行度（Parallelism）：在分布式运行环境中我们一个算子任务被切分成了多少个子任务并行执行

```text
算子级别 > 执行环境级别 > 提交任务级别 > 系统配置级别
```

- Flink中的Slot概念

```text
Flink中的TaskManager是执行任务的节点
TaskManager中Slot个数代表的是一个TaskManager的并发执行能力
```

- 并行度设置-算子级别

```java

@Public
public abstract class Operator<OUT, O extends Operator<OUT, O>> extends DataSet<OUT> {

    protected String name;

    protected int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    public O setParallelism(int parallelism) {
        OperatorValidationUtils.validateParallelism(parallelism);

        this.parallelism = parallelism;

        @SuppressWarnings("unchecked")
        O returnType = (O) this;
        return returnType;
    }
}
```

- 并行度设置-执行环境级别

```java
//当前任务所有的算子、Source、Sink生效
public class MyCacheJob {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }
}
```

- 并行度设置-提交任务级别

```shell
# -p 参数指定
./bin/flink run -p 10 WordCount.jar
```

- 并行度设置-系统配置级别

```shell
vim conf/flink-conf.yaml

parallelism.default: 1
```

- 分布式缓存

```text
分布式缓存最初的思想诞生于Hadoop框架，Hadoop会将一些数据或者文件缓存在HDFS上，在分布式环境中让所有的计算节点调用同一个配置文件

Flink提供的分布式缓存类型Hadoop，目的时为了在分布式环境中让每一个TaskManager节点保存一份相同的数据或者文件，当前节点的Task就像读取本地文件一样
拉取这些配置

```

- 分布式缓存应用

```text
表与表进行Join操作时，如果一个表很大，另一个表很小，那么我们就可以把较小的表进行缓存，每个TaskManager都保存一份，然后进行Join操作
```

- 故障恢复（提交任务拆分后的Task级别）和重启策略（任务级别）

```text
自动故障恢复是Flink提供的一个强大的功能
在实际生产环境中，我们会遇到各种各样的问题从而导致应用挂掉，比如我们经常遇到的非法数据、网络抖动等
Flink提供了强大的可配置故障恢复和重启策略来进行故障恢复
```

- 故障恢复应用

```shell
vim conf/flink-conf.yaml
# 可选配置 region full
# full 集群中的Task发生故障, 那么该任务所有Task都回发生重启
# region 集群中一个或几个Task发生了故障，只需要重启有问题的一部分即可
# 在这个策略下，Flink会把我们的任务分成不同的Region，当某一个Task发生故障时，Flink会计算需要故障恢复的最小Region
# 判断逻辑 1.发生Task所在的Region需要重启 2.如果当前Region依赖数据出现损坏或者部分丢失，那么生产数据的Region也需要重启
# 3.为了保证数据一致性，当前Region的下游Region也需要重启
jobmanager.execution.failover-strategy: region
```

- 重启策略（推荐使用代码配置）

```text
默认约定：如果用户配置了checkpoint，但没有设置重启策略，那么会按照固定延迟重启策略进行重启
如果用户没有配置checkpoint，那么默认不会重启

策略：固定延迟重启策略、失败率重启策略、无重启策略：
```

- 无重启策略 应用

```shell
# 配置
vim conf/flink-conf.yaml
restart-strategy: none
# 代码指定
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
```

- 固定延迟重启策略 应用

```shell
# 配置
vim conf/flink-conf.yaml
restart-strategy: fixed-delay
# 重启尝试次数
restart-strategy.fixed-delay.attempts: 3
# 重启时间间隔
restart-strategy.fixed-delay.delay: 5 s
# 代码指定
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
```

- 失败率重启策略 应用

```shell
# 配置
vim conf/flink-conf.yaml
restart-strategy: failure-rate
# 5分钟内若失败了3次，则认为该任务失败，每次失败的重试时间间隔为5秒
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 5 s

# 代码指定
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
```

### Flink 基本概念

- Streams(流)

```text
有界流: 有固定大小，不随时间的增加而增长的数据，例如 hive中一个表
无界流: 随着时间的增加而增长，计算状态持续进行，例如 kafka中的消息
```

- State(状态)

```text
状态：指的是在流式计算过程中的信息
用途：一般用作容错恢复和持久化，流式计算本质是在做增量计算，也就是说不断需要查询过去的状态
作用：状态在Flink中有十分重要的作用，例如为了确保Exactly-once语义需要将数据写入状态中
容错：状态的持久化存储也是集群出现Fail-over的情况下自动重启的前提条件
```

- Time(时间)

```text
分类：Flink支持 Event Time、Ingestion Time、Processing Time等多种语义时间
作用：时间是我们进行Flink程序开发时，判断业务状态是否滞后和延迟的重要指标
```

- API

```text
分类：从上到下分为 
SQL (High-level language)、
Table API (Declarative DSL)、
DataStream/DataSet API (Core APIs)、
Stateful Stream Processing (Low-level building block[steams, state, envet time])
```

- 编程模型和流式处理

```text
概述：Flink程序的基础构建模块是流（Streams）和转换（Transformations）
开始结束：每一个数据流起始于一个或多个Source，并终止于一个或多个Sink
抽象：数据流类似于DAG
```

- Flink执行优化

```text
算子链：将多个算子放在一个Task中由同一个线程执行
```

- Flink集群模型和角色

```text
JobManager：集群管理者，负责调度任务，协调checkpoints、协调故障恢复、收集Job状态信息、管理集群从节点TaskManager
TaskManager：实际负责计算的Worker，在其上执行Flink Job的一组Task，TaskManager还是所在节点管理员，负责把服务器的信息（硬盘、内存、任务运行情况）
向JobManager汇报
```

- Client

```text
用户在提交编写好的Flink工程时，会先创建一个客户端再进行提交，这个客户端就是Client，Client会根据用户提交的参数先择使用 yarn per job模式、
stand-alone模式还是yarn-session模式将Flink程序提交到集群
```

- Flink资源和资源组

```text
一个TaskManager是一个JVM进程，可以用独立的线程来执行task

为了控制一个TaskManager能接收多少个Task，Flink提出了Task Slot的概念，可以把Task Slot理解为计算资源子集，Task Slot仅对内存进行隔离
```

- Flink与其他框架架构比较

```text
storm是经典的主从模式，并且强依赖于zookeeper
spark streaming本质是微批处理，每个batch都依赖
flink 采用了经典的主从模式，程序启动后会把用户代码处理成Stream Graph，进一步优化成 JobGraph，JobManager会根据JobGraph生成ExecutionGraph
ExecutionGraph才是Flink真正能执行的数据结构，当很多个ExecutionGraph分布在集群中，就会形成一张网状的拓扑结构
```

- Flink与其他框架容错比较

```text
storm仅支持了record级别的ack-fail处理，发送出去的每一条消息都可以确定成功或失败，因此storm支持至少处理一次语义

spark streaming任务，我们可以配置对应的checkpoint，也就是保存点，当任务出现failover时候，会从cehckpoint重新加载，使得数据不丢失
但是这个过程会导致原来的数据重复处理，不能做到Exactly-once
```

- Flink 反压(Back Pressure)

```text
反压概念：当消费者速度低于生产者的速度时，则需要消费者将信息反馈给生产者，使生产者的速度能和消费者的速度进行匹配
storm反压实现：直接通知生产者停止生产数据，这种方式的缺点是不能实现逐级反压，且调优困难，设置的消费速度过小导致吞吐量地下，速度过大会导致消费者OOM
spark streaming反压实现：构造了一个”速率控制器“，根据任务处理时间、处理消息数量计算一个速率，在实现控制数据的接收速率中用到了一个经典的算法即”PID算法“
flink没有使用任何复杂的机制，在数据传输过程中使用了分布式阻塞队列，在一个阻塞队列中，当队列满了以后发送者会被天然阻塞，相当于实现了反压
```
