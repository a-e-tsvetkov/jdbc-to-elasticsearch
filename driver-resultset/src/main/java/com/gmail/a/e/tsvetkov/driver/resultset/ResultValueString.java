package com.gmail.a.e.tsvetkov.driver.resultset;

public class ResultValueString implements ResultValue {
    private final String value;

    ResultValueString(String value) {
        this.value = value;
    }

    @Override
    public String getString() {
        return value;
    }
}
