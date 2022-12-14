package com.happy.core.clean.entity;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;

import java.io.Serializable;

public class A {

    public class B implements Serializable {
        private C.D d = new C().new D();
    }

    public static void main(String[] args) {
        A a = new A();
        B b = a.new B();

        ClosureCleaner.clean(b, ExecutionConfig.ClosureCleanerLevel.TOP_LEVEL, false);
        ClosureCleaner.ensureSerializable(b);
    }
}


