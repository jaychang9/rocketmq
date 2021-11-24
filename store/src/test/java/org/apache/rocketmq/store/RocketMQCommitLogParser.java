package org.apache.rocketmq.store;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

public class RocketMQCommitLogParser {
    public static void test1() throws IOException {
        FileChannel fileChannel = new FileInputStream("E:\\tmp\\rocketmq\\store\\consumerqueue\\ORDER_OPERATE_TOPIC\\2\\00000000000000000000").getChannel();
        long consumeQueueFileSize = fileChannel.size();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
        //fileChannel.close();

        FileChannel commitLogFileChannel = new FileInputStream("E:\\tmp\\rocketmq\\store\\commitlog\\00000000003221225472").getChannel();
        MappedByteBuffer commitLogMappedByteBuffer = commitLogFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, commitLogFileChannel.size());
        //commitLogFileChannel.close();


        for (int i = 0; i < consumeQueueFileSize; i += 20) {
            // 每一个条目共20个字节，分别为8字节的commitlog物理偏移量、4字节的消息长度、8字节tag hashcode
            mappedByteBuffer.position(i);
            long commitLogOffset = mappedByteBuffer.getLong();
            int messageSize = mappedByteBuffer.getInt();
            long tagHashCode = mappedByteBuffer.getLong();

            System.out.println("commitLogOffset=" + commitLogOffset + ",messageSize=" + messageSize + ",tagHashCode=" + tagHashCode);

            byte[] buf = new byte[messageSize];
            int realCommitLogOffset = (int) (commitLogOffset - 3221225472l);
            commitLogMappedByteBuffer.position(realCommitLogOffset);
            commitLogMappedByteBuffer.get(buf, realCommitLogOffset, (int) messageSize);
            System.out.println(new String(buf));

        }
    }

    public static void readCommitLog() throws IOException {
        File commitLogDir = new File("E:\\tmp\\rocketmq\\unit_test_store\\commitlog");
        File[] commitLogFiles = commitLogDir.listFiles();
        Map<String, MappedByteBuffer> commitLogMap = new HashMap<>();
        for (File commitLogFile : commitLogFiles) {
            String commitLogFileName = commitLogFile.getName();
            long commitLogOffset = Long.valueOf(commitLogFileName);
            FileInputStream fileInputStream = new FileInputStream(commitLogFile);
            FileChannel commitLogFileChannel = fileInputStream.getChannel();
            long size = commitLogFileChannel.size();
            MappedByteBuffer mappedByteBuffer = commitLogFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            commitLogFileChannel.close();
            String offsetBeginEnd = commitLogOffset + "_" + (commitLogOffset + size - 1);
            commitLogMap.put(offsetBeginEnd, mappedByteBuffer);


            fileInputStream.close();
        }

        File consumeQueueDir = new File("E:\\tmp\\rocketmq\\unit_test_store\\consumequeue\\abc\\0");
        File[] consumeQueueFiles = consumeQueueDir.listFiles();
        for (File consumeQueueFile : consumeQueueFiles) {
            FileInputStream fileInputStream = new FileInputStream(consumeQueueFile);
            FileChannel consumeQueueFileChanel = fileInputStream.getChannel();
            long size = consumeQueueFileChanel.size();
            MappedByteBuffer consumeQueueMappedByteBuffer = consumeQueueFileChanel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            consumeQueueFileChanel.close();

            for (int i = 0; i < size; i += 20) {
                consumeQueueMappedByteBuffer.position(i);
                long messageOffset = consumeQueueMappedByteBuffer.getLong();
                int messageSize = consumeQueueMappedByteBuffer.getInt();
                long tagHashCode = consumeQueueMappedByteBuffer.getLong();
                System.out.println(String.format("messageOffsetBegin=%s,messageOffsetEnd=%s,messageSize=%s,tagHashCode=%s", messageOffset, messageOffset + messageSize - 1, messageSize, tagHashCode));
                for (Map.Entry<String, MappedByteBuffer> entry : commitLogMap.entrySet()) {
                    String key = entry.getKey();
                    String[] strings = key.split("_");
                    // commitLog offset begin
                    long offsetBegin = Long.valueOf(strings[0]);
                    // commitLog offset end
                    long offsetEnd = Long.valueOf(strings[1]);
                    long messageOffsetBegin = messageOffset;
                    long messageOffsetEnd = messageOffset + messageSize - 1;
                    if ((messageOffsetBegin >= offsetBegin && messageOffsetBegin <= offsetEnd) && (messageOffsetEnd >= offsetBegin && messageOffsetEnd <= offsetEnd)) {
                        MappedByteBuffer mappedByteBuffer = entry.getValue();
                        byte[] buff = new byte[messageSize];
                        mappedByteBuffer.position((int) (messageOffsetBegin - offsetBegin));
                        int mSize = mappedByteBuffer.getInt();
                        System.out.println("mSize="+mSize+",messageSize="+messageSize);
                        int magicCode = mappedByteBuffer.getInt();
                        System.out.println("magicCode="+magicCode);
                        mappedByteBuffer.get(buff, 0, messageSize-8);
                        System.out.println(new String(buff));
                        System.out.println("************************************************************************************************************************************************");
                    }
                }

            }


            fileInputStream.close();

        }
    }

    public static void main(String[] args) throws IOException {
        readCommitLog();
    }
}
