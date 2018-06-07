package com.gmail.a.e.tsvetkov.driver.resultset;

import java.sql.Wrapper;

public abstract class AWrapper implements Wrapper {
    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
