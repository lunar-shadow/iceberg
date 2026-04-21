/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;

public class TestRESTCatalogCaseInsensitive {

  private static RESTCatalogAdapter newAdapter() {
    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize("in-memory", Map.of());
    return new RESTCatalogAdapter(backendCatalog);
  }

  private RESTCatalog createCatalogWithCaseInsensitive(String type) {
    RESTCatalog catalog = new RESTCatalog(config -> newAdapter());
    catalog.initialize(
        "test",
        Map.of(
            "uri", "http://localhost:8080",
            "case-insensitive", "true",
            "case-insensitive-type", type));
    return catalog;
  }

  @Test
  public void testConvertCaseDisabledByDefault() {
    RESTCatalog catalog = new RESTCatalog(config -> newAdapter());
    catalog.initialize("test", Map.of("uri", "http://localhost:8080"));

    Namespace ns = Namespace.of("CustomerDB", "Schema");
    TableIdentifier ident = TableIdentifier.of(ns, "Orders");

    assertThat(catalog.convertCase(ns)).isEqualTo(ns);
    assertThat(catalog.convertCase(ident)).isEqualTo(ident);
  }

  @Test
  public void testConvertCaseLowerCase() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("lower_case");

    Namespace ns = Namespace.of("CustomerDB", "Schema");
    Namespace expected = Namespace.of("customerdb", "schema");
    assertThat(catalog.convertCase(ns)).isEqualTo(expected);

    TableIdentifier ident = TableIdentifier.of(ns, "Orders");
    TableIdentifier expectedIdent = TableIdentifier.of(expected, "orders");
    assertThat(catalog.convertCase(ident)).isEqualTo(expectedIdent);
  }

  @Test
  public void testConvertCaseUpperCase() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("upper_case");

    Namespace ns = Namespace.of("customerdb", "schema");
    Namespace expected = Namespace.of("CUSTOMERDB", "SCHEMA");
    assertThat(catalog.convertCase(ns)).isEqualTo(expected);

    TableIdentifier ident = TableIdentifier.of(ns, "orders");
    TableIdentifier expectedIdent = TableIdentifier.of(expected, "ORDERS");
    assertThat(catalog.convertCase(ident)).isEqualTo(expectedIdent);
  }

  @Test
  public void testConvertCaseMixedInput() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("lower_case");

    Namespace ns = Namespace.of("MyDB", "MySchema");
    assertThat(catalog.convertCase(ns)).isEqualTo(Namespace.of("mydb", "myschema"));

    TableIdentifier ident = TableIdentifier.of("MyDB", "MyTable");
    assertThat(catalog.convertCase(ident)).isEqualTo(TableIdentifier.of("mydb", "mytable"));
  }

  @Test
  public void testConvertCaseEmptyNamespace() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("lower_case");

    Namespace empty = Namespace.empty();
    assertThat(catalog.convertCase(empty)).isEqualTo(empty);
  }

  @Test
  public void testConvertCaseMultiLevelNamespace() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("lower_case");

    Namespace ns = Namespace.of("Finance", "Q1", "Reports");
    assertThat(catalog.convertCase(ns)).isEqualTo(Namespace.of("finance", "q1", "reports"));
  }

  @Test
  public void testCaseVariationsAllNormalizeToSame() {
    RESTCatalog catalog = createCatalogWithCaseInsensitive("lower_case");

    TableIdentifier lower = catalog.convertCase(TableIdentifier.of("customerdb", "orders"));
    TableIdentifier upper = catalog.convertCase(TableIdentifier.of("CUSTOMERDB", "ORDERS"));
    TableIdentifier mixed = catalog.convertCase(TableIdentifier.of("CustomerDB", "Orders"));
    TableIdentifier random = catalog.convertCase(TableIdentifier.of("cUsToMeRdB", "oRdErS"));

    assertThat(lower).isEqualTo(upper).isEqualTo(mixed).isEqualTo(random);
  }

  @Test
  public void testInvalidCaseType() {
    RESTCatalog catalog = new RESTCatalog(config -> newAdapter());

    assertThatThrownBy(
            () ->
                catalog.initialize(
                    "test",
                    Map.of(
                        "uri", "http://localhost:8080",
                        "case-insensitive", "true",
                        "case-insensitive-type", "invalid")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("lower_case, upper_case");
  }
}
