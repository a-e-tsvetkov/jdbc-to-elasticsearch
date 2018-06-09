package com.gmail.a.e.tsvetkov.driver.resultset;

import java.math.BigDecimal;

public class ResultValueBigDecimal implements ResultValue {
    private final BigDecimal value;

    ResultValueBigDecimal(BigDecimal value) {
        this.value = value;
    }

    @Override
    public byte getByte() {
        return value.byteValueExact();
    }

    @Override
    public short getShort() {
        return value.shortValueExact();
    }

    @Override
    public int getInt() {
        return value.intValueExact();
    }

    @Override
    public long getLong() {
        return value.longValueExact();
    }

    @Override
    public float getFloat() {
        return value.floatValue();
    }

    @Override
    public double getDouble() {
        return value.byteValueExact();
    }
}
