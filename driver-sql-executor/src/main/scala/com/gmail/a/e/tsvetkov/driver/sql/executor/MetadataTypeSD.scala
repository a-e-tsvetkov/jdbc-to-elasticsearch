package com.gmail.a.e.tsvetkov.driver.sql.executor

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.databind.{DeserializationContext, JsonDeserializer, JsonSerializer, SerializerProvider}

object MetadataTypeSD {
  val TYPE_CHAR = "char"
  val TYPE_NUMERIC = "numeric"
  val PROPERTY_TYPE = "type"

  class MetadataTypeSerializer extends JsonSerializer[MetadataType] {
    override def serialize(t: MetadataType,
                           jsonGenerator: JsonGenerator,
                           serializerProvider: SerializerProvider): Unit = {
      jsonGenerator.writeStartObject()
      t match {
        case MetadataTypeChar =>
          jsonGenerator.writeStringField(PROPERTY_TYPE, TYPE_CHAR)
        case MetadataTypeNumeric =>
          jsonGenerator.writeStringField(PROPERTY_TYPE, TYPE_NUMERIC)
      }
      jsonGenerator.writeEndObject()
    }
  }

  class MetadataTypeDeserializer extends JsonDeserializer[MetadataType] {
    override def deserialize(jsonParser: JsonParser,
                             deserializationContext: DeserializationContext): MetadataType = {
      val node = jsonParser.getCodec.readTree[TreeNode](jsonParser)
      val id = node.get(PROPERTY_TYPE).asInstanceOf[TextNode].asText()

      id match {
        case TYPE_CHAR => MetadataTypeChar
        case TYPE_NUMERIC => MetadataTypeNumeric
      }
    }
  }

}