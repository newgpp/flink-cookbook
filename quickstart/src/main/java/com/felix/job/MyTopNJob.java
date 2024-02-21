package com.felix.job;

import com.felix.entity.GoodsOrder;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * https://cloud.tencent.com/developer/article/1953795
 * 假设电商网站有这样一个榜单，展示1分钟内当前用户购买品类交易额的Top3，并且榜单要每10秒刷新一次。
 * 而且我们现在可以拿到一个交易流，里面记录了交易品类和交易额。
 */
public class MyTopNJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStream<GoodsOrder> source = env.addSource(new MySourceFunction());
        //滑动窗口大小60s 滑动距离10s 计算了每一个品类过去1分钟内的总交易额
        DataStream<GoodsOrder> reduce = source.keyBy("type")
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60L), Time.seconds(10L)))
                .reduce(new ReduceFunction<GoodsOrder>() {
                    @Override
                    public GoodsOrder reduce(GoodsOrder value1, GoodsOrder value2) throws Exception {
                        return new GoodsOrder(value1.getType(), value1.getPrice() + value2.getPrice());
                    }
                });
        //10s滚动窗口 windowAll汇集到一个节点执行
        DataStream<String> process = reduce
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10L)))
                .process(new ProcessAllWindowFunction<GoodsOrder, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<GoodsOrder, String, TimeWindow>.Context context, Iterable<GoodsOrder> elements, Collector<String> out) throws Exception {
                        String top3 = StreamSupport.stream(elements.spliterator(), false)
                                .sorted(Comparator.comparingLong(GoodsOrder::getPrice).reversed())
                                .limit(3)
                                .map(x -> String.format("%s: %s", x.getType(), x.getPrice()))
                                .collect(Collectors.joining("\n"));
                        out.collect("--------" + "\n" + top3);
                    }
                });

        process.print();

        env.execute("myTopN Job");

    }

    private static class MySourceFunction implements SourceFunction<GoodsOrder> {

        private boolean isRunning = true;

        private final List<String> types = Arrays.asList(
                "卫衣",
                "T恤",
                "牛仔裤",
                "西服",
                "风衣",
                // 女装
                "连衣裙",
                "卫衣",
                "衬衫",
                "针织衫",
                "休闲裤",
                // 手机数码
                "手机",
                "手机配件",
                "摄影摄像",
                "影音娱乐",
                "数码配件",
                "智能设备",
                "电子教育",
                // 电脑办公
                "电脑整机",
                "电脑组件",
                "外设",
                "网络产品",
                "办公设备",
                "文具耗材",
                // 家用电器
                "电视",
                "空调",
                "洗衣机",
                "冰箱",
                "厨卫",
                "生活电器",
                // 户外运动
                "运动鞋包",
                "运行服饰",
                "户外鞋服",
                "户外装备",
                "骑行",
                "健身",
                // 家具家装
                "厨房卫浴",
                "灯饰照明",
                "五金工具",
                "客厅家具",
                "餐厅家具",
                // 图书文娱
                "少儿读物",
                "文学",
                "动漫",
                "专业");

        @Override
        public void run(SourceContext<GoodsOrder> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                String type = types.get(random.nextInt(types.size()));
                int price = random.nextInt(3333) + 33;
                ctx.collect(new GoodsOrder(type, (long) price));
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

}
