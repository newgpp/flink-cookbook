package org.myorg.quickstart.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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
