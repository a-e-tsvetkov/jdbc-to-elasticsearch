package com.gmail.a.e.tsvetkov.driver.resultset;

public class ResultValueBoolean implements ResultValue {
    private final boolean value;

    ResultValueBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public boolean getBoolean() {
        return value;
    }
}
