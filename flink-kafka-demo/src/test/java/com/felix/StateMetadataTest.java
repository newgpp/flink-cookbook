package com.felix;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.InputStream;

public class StateMetadataTest {

    @Test
    public void read_metadata_should_success() throws Exception {
        //given
        ClassLoader classLoader = StateMetadataTest.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("_metadata");

        //when
        CheckpointMetadata metadata = Checkpoints.loadCheckpointMetadata(new DataInputStream(inputStream), classLoader, null);

        //then
        System.out.println(JSON.toJSONString(metadata, JSONWriter.Feature.PrettyFormat));
    }
}
