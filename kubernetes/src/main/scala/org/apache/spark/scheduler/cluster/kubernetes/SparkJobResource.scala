/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.kubernetes

import io.fabric8.kubernetes.client.{BaseClient, KubernetesClient}
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.json4s._
import org.json4s.jackson.Serialization.{read, write}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats

 /**
  * Representation of a Spark Job State in Kubernetes
  */

object SparkJobResource {
   type Specification = Map[String, Any]

   case class Metadata(name: String, uid: Option[String] = None,
                       labels: Option[Map[String, String]] = None,
                       annotations: Option[Map[String, String]] = None)

   case class SparkJobState(apiVersion: String, kind: String,
                            metadata: Metadata, spec: Specification)
  }

class SparkJobResource(client: KubernetesClient) extends Logging {

  import SparkJobResource._

  implicit val formats = DefaultFormats
  lazy val httpClient = getHttpClient(client.asInstanceOf[BaseClient])
  lazy val kind = "SparkJob"
  lazy val apiVersion = "apache.io/v1"
  lazy val apiEndpoint = s"${client.getMasterUrl}/apis/$apiVersion/" +
    s"namespaces/${client.getNamespace}/sparkjobs"

  def getHttpClient(client: BaseClient): OkHttpClient = {
    val field = classOf[BaseClient].getDeclaredField("httpClient")
    try {
      field.setAccessible(true)
      field.get(client).asInstanceOf[OkHttpClient]
    } finally {
      field.setAccessible(false)
    }
  }

  def createTPRObject(keyValuePairs: Map[String, Any]): Unit = {
    assert(keyValuePairs.contains("name"))
    val attributesWithoutName = keyValuePairs - "name"
    val resourceObject = SparkJobState(apiVersion, kind,
      Metadata(keyValuePairs("name").toString),
      spec = attributesWithoutName)
    val requestBody = RequestBody.create(MediaType.parse("application/json"),
      compact(render(parse(write(resourceObject)))))
    val request = new Request.Builder()
      .post(requestBody)
      .url(apiEndpoint)
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 201) {
      logInfo(s"Successfully posted resource ${keyValuePairs("name")}: " +
        s"${pretty(render(parse(write(resourceObject))))}")
    } else {
      logError(s"Failed to post resource ${keyValuePairs("name")}. ${response.message()}")
      // Throw exception ? where to catch this ?
    }
  }

  def updateTPRObject(name: String, value: String, fieldPath: String): Unit = {
    val payload = List(("op" -> "replace") ~ ("path" -> fieldPath) ~ ("value" -> value))
    val requestBody = RequestBody.create(MediaType.parse("application/json-patch+json"),
      compact(render(payload)))
    val request = new Request.Builder()
      .post(requestBody)
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully patched resource $name")
    } else {
      logError(s"Failed to patch resource $name. ${response.message()}")
      // throw error or not ? to be caught where ?
    }
  }

  def deleteTPRObject(name: String): Unit = {
    val request = new Request.Builder()
      .delete()
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully deleted resource $name")
    } else {
      logError(s"Failed to delete resource $name. ${response.message()}")
      // throw error or not ? to be caught where ?
    }
  }

  def getTPRObject(name: String): SparkJobState = {
    val request = new Request.Builder()
      .get()
      .url(s"$apiEndpoint/$name")
      .build()
    val response = httpClient.newCall(request).execute()
    if (response.code() == 200) {
      logInfo(s"Successfully retrieved resource $name")
      read[SparkJobState](response.body().string())
    } else {
      val msg = s"Failed to retrieve resource $name. ${response.message()}"
      logError(msg)
      throw new SparkException(msg)
    }
  }

}
