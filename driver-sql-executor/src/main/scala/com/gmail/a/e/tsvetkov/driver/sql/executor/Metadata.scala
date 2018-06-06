package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}

case class MetadataDatabase(tables: Seq[MetadataTable] = Seq())

case class MetadataTable(name: String, columns: Seq[MetadataColumn])

case class MetadataColumn(name: String, columnType: MetadataType)

@JsonSerialize(using = classOf[MetadataTypeSD.MetadataTypeSerializer])
@JsonDeserialize(using = classOf[MetadataTypeSD.MetadataTypeDeserializer])
sealed trait MetadataType

case object MetadataTypeChar extends MetadataType

case object MetadataTypeNumeric extends MetadataType