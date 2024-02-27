### Exactly-Once

- 背景
```text
Exactly-Once这种语义会保证每一条消息只被流处理系统处理一次

精确一次语义是Flink1.4.0版本引入的一个重要特性
而且Flink支持 端到端的精确一次 语义
```
- 端到端(End to End)的精确一次
```text
它指的是Flink应用从Source端开始到Sink端结束，数据必须经过起始点和结束点

Flink自身是无法保证外部系统“精确一次”语义的

所以Flink若要实现所谓“端到端的精确一次”要求，那么外部系统必须支持“精确一次”的要求

然后借助Flink提供的分布式快照和两阶段提交才能实现
```
- 分布式快照机制
```text
Flink提供了失败恢复的容错机制，核心就是：持续创建分布式数据流的快照来实现

同Spark相比，Spark仅仅是针对Driver的故障恢复Checkpoint
而Flink的快照可以到算子级别，并且对全局数据也可以做快照

Flink的分布式快照受到 Chandy-Lamport 分布式快照算法启发，同时做了定制开发

```

- 分布式快照机制 Barrier
```text
Flink分布式快照的核心元素之一是Barrier（数据栅栏）
可以把 Barrier 理解为一个标记，该标记是严格有序的，并且随着数据流往下流动

每个Barrier都带有自己的ID，Barrier极其轻量，并不会干扰正常的数据处理

基于checkpoint的快照操作，快照机制能够保证作业出现fail-over后可以从
最新的快照进行恢复，即分布式快照机制可以保证Flink系统内部的精确一次处理
```

- 两阶段提交
```text
在实际生产系统中，Flink会对接各种各样的外部系统，不如Kafka、HDFS等
一旦Flink作业出现失败，作业会重新消费旧数据

针对这种情况，Flink1.4版本引入了一个很重要的功能：两阶段提交（TwoPhaseCommitSinkFunction）
两阶段搭配特定的source和sink（特别是0.11版本的kafka）
使得“精确一次处理语义”成为可能
```
- TwoPhaseCommitSinkFunction
```java
public abstract class TwoPhaseCommitSinkFunction<IN, TXN, CONTEXT>
		extends RichSinkFunction<IN>
		implements CheckpointedFunction, CheckpointListener {
    protected abstract TXN beginTransaction() throws Exception;

    /**
     * Pre commit previously created transaction. Pre commit must make all of the necessary steps to prepare the
     * transaction for a commit that might happen in the future. After this point the transaction might still be
     * aborted, but underlying implementation must ensure that commit calls on already pre committed transactions
     * will always succeed.
     *
     * <p>Usually implementation involves flushing the data.
     */
    protected abstract void preCommit(TXN transaction) throws Exception;

    /**
     * Commit a pre-committed transaction. If this method fail, Flink application will be
     * restarted and {@link TwoPhaseCommitSinkFunction#recoverAndCommit(Object)} will be called again for the
     * same transaction.
     */
    protected abstract void commit(TXN transaction);

    /**
     * Invoked on recovered transactions after a failure. User implementation must ensure that this call will eventually
     * succeed. If it fails, Flink application will be restarted and it will be invoked again. If it does not succeed
     * eventually, a data loss will occur. Transactions will be recovered in an order in which they were created.
     */
    protected void recoverAndCommit(TXN transaction) {
        commit(transaction);
    }

    /**
     * Abort a transaction.
     */
    protected abstract void abort(TXN transaction);
}
```

