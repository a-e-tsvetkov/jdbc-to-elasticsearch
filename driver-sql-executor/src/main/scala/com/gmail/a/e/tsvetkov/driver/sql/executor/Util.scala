package com.gmail.a.e.tsvetkov.driver.sql.executor

import java.sql.SQLException

object Util {
  def err(msg: String) = {
    throw new SQLException(msg)
  }
}

