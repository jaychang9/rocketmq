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

        File commitLogDir = new File("E:\\tmp\\rocketmq\\store\\commitlog");
        File[] commitLogFiles = commitLogDir.listFiles();
        Map<String, MappedByteBuffer> commitLogMap = new HashMap<>();
        for (File commitLogFile : commitLogFiles) {
            String commitLogFileName = commitLogFile.getName();
            long commitLogOffset = Long.valueOf(commitLogFileName);
            FileInputStream fileInputStream = new FileInputStream(commitLogFile);
            FileChannel commitLogFileChannel = fileInputStream.getChannel();
            long size = commitLogFileChannel.size();
            MappedByteBuffer mbb = commitLogFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
            commitLogFileChannel.close();
            String offsetBeginEnd = commitLogOffset + "_" + (commitLogOffset + size - 1);
            commitLogMap.put(offsetBeginEnd, mbb);
            fileInputStream.close();
        }


        for (int i = 0; i < consumeQueueFileSize; i += 20) {
            // 每一个条目共20个字节，分别为8字节的commitlog物理偏移量、4字节的消息长度、8字节tag hashcode
            mappedByteBuffer.position(i);
            long messageOffsetBegin = mappedByteBuffer.getLong();
            int messageSize = mappedByteBuffer.getInt();
            long messageOffsetEnd = messageOffsetBegin + messageSize - 1;
            long tagHashCode = mappedByteBuffer.getLong();

            if (messageSize == 0) {
                break;
            }

            for (Map.Entry<String, MappedByteBuffer> entry : commitLogMap.entrySet()) {
                String entryKey = entry.getKey();
                String[] strings = entryKey.split("_");
                // commitLog offset begin
                long offsetBegin = Long.valueOf(strings[0]);
                // commitLog offset end
                long offsetEnd = Long.valueOf(strings[1]);
                if ((offsetBegin <= messageOffsetBegin) && (messageOffsetEnd <= offsetEnd)) {
                    MappedByteBuffer commitLogMappedByteBuffer = entry.getValue();
                    byte[] buff = new byte[messageSize];
                    commitLogMappedByteBuffer.position((int) (messageOffsetBegin - offsetBegin));
                    int mSize = commitLogMappedByteBuffer.getInt();
                    int magicCode = commitLogMappedByteBuffer.getInt();
                    commitLogMappedByteBuffer.get(buff, 0, messageSize - 8);
                    System.out.println(String.format("mSize=%s,messageSize=%s,magicCode=%s,messageBody=%s", mSize, messageSize, magicCode, new String(buff)));
                }
            }
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
                    if ((messageOffsetBegin >= offsetBegin) && (messageOffsetEnd <= offsetEnd)) {
                        MappedByteBuffer mappedByteBuffer = entry.getValue();
                        mappedByteBuffer.position((int) (messageOffsetBegin - offsetBegin));
                        int mSize = mappedByteBuffer.getInt();//4
                        int magicCode = mappedByteBuffer.getInt();//8
                        int bodyCRC = mappedByteBuffer.getInt();//12
                        int queueId = mappedByteBuffer.getInt();//16
                        int flag = mappedByteBuffer.getInt();//20
                        long queueOffset = mappedByteBuffer.getLong();//28
                        long physicalOffset = mappedByteBuffer.getLong();//36
                        int sysFlag = mappedByteBuffer.getInt();//40
                        long bornTimestamp = mappedByteBuffer.getLong();//48
                        long bornHostPort = mappedByteBuffer.getLong();//56
                        long storeTimestamp = mappedByteBuffer.getLong();//64
                        long storeHostPort = mappedByteBuffer.getLong();//72
                        int reconsumeTimes = mappedByteBuffer.getInt();//76
                        long preparedTransactionOffset = mappedByteBuffer.getLong();//84
                        int bodyLength = mappedByteBuffer.getInt();//88
                        byte[] buff = new byte[messageSize - 88];
                        mappedByteBuffer.get(buff, 0, messageSize - 88);
                        byte topicLength = mappedByteBuffer.get();
                        System.out.print(
                                String.format("mSize=%s,messageSize=%s,magicCode=%s,bodyCRC=%s,queueId=%s,flag=%s,queueOffset=%s,physicalOffset=%s",
                                        mSize, messageSize, magicCode, bodyCRC, queueId, flag, queueOffset, physicalOffset));
                        System.out.println(String.format(",sysFlag=%s,bornTimestamp=%s,bornHostPort=%s,storeTimestamp=%s,storeHostPort=%s,reconsumeTimes=%s,preparedTransactionOffset=%s,bodyLength=%s,messageBody=%s,topicLength=%s", sysFlag, bornTimestamp, bornHostPort, storeTimestamp, storeHostPort, reconsumeTimes, preparedTransactionOffset, bodyLength, new String(buff), (int) topicLength));
                    }
                }

            }


            fileInputStream.close();

        }
    }

    public static void main(String[] args) throws IOException {
        //test1();
        readCommitLog();
    }
}
