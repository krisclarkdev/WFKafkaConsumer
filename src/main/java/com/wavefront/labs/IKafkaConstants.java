package com.wavefront.labs;

public interface IKafkaConstants {
    public static Integer MESSAGE_COUNT=1000;
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";
    public static Integer MAX_POLL_RECORDS=1;
}

