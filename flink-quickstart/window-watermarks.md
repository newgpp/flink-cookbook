### Flink Time

- Event Time（事件时间）
```text
指的是数据产生的时间，这个时间一般由数据生产方自身携带，比如Kafka消息，每个生成的消息中自带一个时间戳代表每条数据的产生时间
Event Time从消息的产生就诞生了，不会改变，也是我们使用最频繁的时间
```
```shell
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);   
```

```java
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N> {
    /**
     * Event time timers that are currently in-flight.
     */
    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

    @Override
    public void registerEventTimeTimer(N namespace, long time) {
        eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
    }

    public void advanceWatermark(long time) throws Exception {
        currentWatermark = time;

        InternalTimer<K, N> timer;

        while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            eventTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onEventTime(timer);
        }
    }
}
```
- Processing Time（处理时间）
```text
指的是数据被Flink框架处理时机器的系统时间
Processing Time这个时间存在一定的不确定性，比如消息到达处理节点延迟等影响
```
```shell
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);   
```

```java
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N> {

    private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

    @Override
    public void registerProcessingTimeTimer(N namespace, long time) {
        InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
        if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
            long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
            // check if we need to re-schedule our timer to earlier
            if (time < nextTriggerTime) {
                if (nextTimer != null) {
                    nextTimer.cancel(false);
                }
                nextTimer = processingTimeService.registerTimer(time, this::onProcessingTime);
            }
        }
    }

    private void onProcessingTime(long time) throws Exception {
        // null out the timer in case the Triggerable calls registerProcessingTimeTimer()
        // inside the callback.
        nextTimer = null;

        InternalTimer<K, N> timer;

        while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
            processingTimeTimersQueue.poll();
            keyContext.setCurrentKey(timer.getKey());
            triggerTarget.onProcessingTime(timer);
        }

        if (timer != null && nextTimer == null) {
            nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this::onProcessingTime);
        }
    }
}
```

- Ingestion Time（摄入时间）
```text
Ingestion Time指的是事件进入Flink系统的时间
在Flink的Source中，每个时间都会把当前时间作为时间戳，后续窗口处理都会基于这个时间
理论上Ingestion Time在Event Time和Processing Time之间
```
```text
与事件时间相比，摄入时间无法处理延时和无序的情况，但是不需要明确执行如何生成watermark
在系统内部，摄入时间采用更类似于事件时间的处理方式进行处理
但是有自动生成的时间戳和自动的watermark，可以防止Flink内部处理数据发生乱序，但是无法解决数据叨叨Flink之前发生的乱序问题
如果需要处理此类问题，建议使用Event Time
```

```java
private static class AutomaticWatermarkContext<T> extends WatermarkContext<T> {
    @Override
    public void onProcessingTime(long timestamp) {
        final long currentTime = timeService.getCurrentProcessingTime();

        synchronized (lock) {
            // we should continue to automatically emit watermarks if we are active
            if (streamStatusMaintainer.getStreamStatus().isActive()) {
                if (idleTimeout != -1 && currentTime - lastRecordTime > idleTimeout) {
                    // if we are configured to detect idleness, piggy-back the idle detection check on the
                    // watermark interval, so that we may possibly discover idle sources faster before waiting
                    // for the next idle check to fire
                    markAsTemporarilyIdle();

                    // no need to finish the next check, as we are now idle.
                    cancelNextIdleDetectionTask();
                } else if (currentTime > nextWatermarkTime) {
                    // align the watermarks across all machines. this will ensure that we
                    // don't have watermarks that creep along at different intervals because
                    // the machine clocks are out of sync
                    final long watermarkTime = currentTime - (currentTime % watermarkInterval);

                    output.emitWatermark(new Watermark(watermarkTime));
                    nextWatermarkTime = watermarkTime + watermarkInterval;
                }
            }
        }

        long nextWatermark = currentTime + watermarkInterval;
        nextWatermarkTimer = this.timeService.registerTimer(
                nextWatermark, new WatermarkEmittingTask(this.timeService, lock, output));
    }
    
    //处理数据代码
    @Override
    protected void processAndCollect(T element) {
        lastRecordTime = this.timeService.getCurrentProcessingTime();
        output.collect(reuse.replace(element, lastRecordTime));

        // this is to avoid lock contention in the lockingObject by
        // sending the watermark before the firing of the watermark
        // emission task.
        if (lastRecordTime > nextWatermarkTime) {
            // in case we jumped some watermarks, recompute the next watermark time
            final long watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
            nextWatermarkTime = watermarkTime + watermarkInterval;
            output.emitWatermark(new Watermark(watermarkTime));

            // we do not need to register another timer here
            // because the emitting task will do so.
        }
    }
}
```

### WaterMark（水印）
- 概念
```text
翻译为“水位线”更为合理，它在本质上是一个时间戳

水印的出现是为了解决实时计算中的数据乱序问题，它的本质是DataStream中一个带有时间戳的元素

如果Flink系统中出现了一个WaterMark T，那么就意味着EventTime < T的数据都已经到达窗口

窗口的结束时间和T相同的那个窗口被触发进行计算了

也就是说：水印是Flink判断迟到数据的标准，同时也是窗口触发的标记

当并行度大于1的情况下，多个流会产生多个水印和窗口，这时候Flink会选取时间戳最小的水印
```

- 水印是如何生成的
```text

```

- 时间戳
```text
EventTime每条数据都携带时间戳

ProcessingTime数据不携带任何时间戳信息

IngestionTime和EventTime类似，不同的是Flink会使用系统时间作为时间戳绑定到每条数据上
```


- 触发代码
```java
@PublicEvolving
public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private EventTimeTrigger() {
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        //[start, end) 
        // window.maxTimestamp = end - 1
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }
}
```

- com.felix.job.MyTimeJob.waterMarkJob1 输入数据
```text
hello,1553503185000
hello,1553503188000
hello,1553503189000
hello,1553503190000
hello,1553503187000
hello,1553503186000
hello,1553503191000
hello,1553503192000
hello,1553503193000
hello,1553503194000
hello,1553503195000
hello,1553503196000
hello,1553503197000
hello,1553503198000
hello,1553503199000
hello,1553503200000
hello,1553503201000
hello,1553503202000
hello,1553503203000
```