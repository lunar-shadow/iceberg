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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

public class RESTCatalog
    implements Catalog, ViewCatalog, SupportsNamespaces, Configurable<Object>, Closeable {
  private final RESTSessionCatalog sessionCatalog;
  private final Catalog delegate;
  private final SupportsNamespaces nsDelegate;
  private final SessionCatalog.SessionContext context;
  private final ViewCatalog viewSessionCatalog;

  private boolean caseInsensitive = CatalogProperties.CASE_INSENSITIVE_DEFAULT;
  private String caseType = CatalogProperties.CASE_INSENSITIVE_TYPE_DEFAULT;

  public RESTCatalog() {
    this(
        SessionCatalog.SessionContext.createEmpty(),
        config ->
            HTTPClient.builder(config)
                .uri(config.get(CatalogProperties.URI))
                .withHeaders(RESTUtil.configHeaders(config))
                .build());
  }

  public RESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this(SessionCatalog.SessionContext.createEmpty(), clientBuilder);
  }

  public RESTCatalog(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder) {
    this.sessionCatalog = newSessionCatalog(clientBuilder);
    this.delegate = sessionCatalog.asCatalog(context);
    this.nsDelegate = (SupportsNamespaces) delegate;
    this.context = context;
    this.viewSessionCatalog = sessionCatalog.asViewCatalog(context);
  }

  /**
   * Create a new {@link RESTSessionCatalog} instance.
   *
   * <p>This method can be overridden in subclasses to provide custom {@link RESTSessionCatalog}
   * implementations.
   *
   * @param clientBuilder a function to build REST clients
   * @return a new RESTSessionCatalog instance
   */
  protected RESTSessionCatalog newSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder) {
    return new RESTSessionCatalog(clientBuilder, null);
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    Preconditions.checkArgument(props != null, "Invalid configuration: null");

    this.caseInsensitive =
        PropertyUtil.propertyAsBoolean(
            props, CatalogProperties.CASE_INSENSITIVE, CatalogProperties.CASE_INSENSITIVE_DEFAULT);
    this.caseType =
        PropertyUtil.propertyAsString(
            props,
            CatalogProperties.CASE_INSENSITIVE_TYPE,
            CatalogProperties.CASE_INSENSITIVE_TYPE_DEFAULT);
    Preconditions.checkArgument(
        "lower_case".equals(caseType) || "upper_case".equals(caseType),
        "Invalid value for '%s': %s. Allowed values are: lower_case, upper_case",
        CatalogProperties.CASE_INSENSITIVE_TYPE,
        caseType);

    sessionCatalog.initialize(name, props);
  }

  @VisibleForTesting
  Namespace convertCase(Namespace ns) {
    if (!caseInsensitive) {
      return ns;
    }

    String[] levels = ns.levels();
    String[] converted = new String[levels.length];
    for (int i = 0; i < levels.length; i++) {
      converted[i] =
          "upper_case".equals(caseType)
              ? levels[i].toUpperCase(Locale.ROOT)
              : levels[i].toLowerCase(Locale.ROOT);
    }

    return Namespace.of(converted);
  }

  @VisibleForTesting
  TableIdentifier convertCase(TableIdentifier ident) {
    if (!caseInsensitive) {
      return ident;
    }

    Namespace convertedNs = convertCase(ident.namespace());
    String convertedName =
        "upper_case".equals(caseType)
            ? ident.name().toUpperCase(Locale.ROOT)
            : ident.name().toLowerCase(Locale.ROOT);
    return TableIdentifier.of(convertedNs, convertedName);
  }

  protected RESTSessionCatalog sessionCatalog() {
    return sessionCatalog;
  }

  @Override
  public String name() {
    return sessionCatalog.name();
  }

  public Map<String, String> properties() {
    return sessionCatalog.properties();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace ns) {
    return delegate.listTables(convertCase(ns));
  }

  @Override
  public boolean tableExists(TableIdentifier ident) {
    return delegate.tableExists(convertCase(ident));
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    return delegate.loadTable(convertCase(ident));
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    delegate.invalidateTable(convertCase(ident));
  }

  @Override
  public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
    return delegate.buildTable(convertCase(ident), schema);
  }

  @Override
  public Table createTable(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return delegate.createTable(convertCase(ident), schema, spec, location, props);
  }

  @Override
  public Table createTable(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return delegate.createTable(convertCase(ident), schema, spec, props);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return delegate.createTable(convertCase(ident), schema, spec);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return delegate.createTable(convertCase(identifier), schema);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return delegate.newCreateTableTransaction(convertCase(ident), schema, spec, location, props);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return delegate.newCreateTableTransaction(convertCase(ident), schema, spec, props);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return delegate.newCreateTableTransaction(convertCase(ident), schema, spec);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return delegate.newCreateTableTransaction(convertCase(identifier), schema);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props,
      boolean orCreate) {
    return delegate.newReplaceTableTransaction(
        convertCase(ident), schema, spec, location, props, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> props,
      boolean orCreate) {
    return delegate.newReplaceTableTransaction(convertCase(ident), schema, spec, props, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, boolean orCreate) {
    return delegate.newReplaceTableTransaction(convertCase(ident), schema, spec, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, boolean orCreate) {
    return delegate.newReplaceTableTransaction(convertCase(ident), schema, orCreate);
  }

  @Override
  public boolean dropTable(TableIdentifier ident) {
    return delegate.dropTable(convertCase(ident));
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    return delegate.dropTable(convertCase(ident), purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    delegate.renameTable(convertCase(from), convertCase(to));
  }

  @Override
  public Table registerTable(TableIdentifier ident, String metadataFileLocation) {
    return delegate.registerTable(convertCase(ident), metadataFileLocation);
  }

  @Override
  public Table registerTable(
      TableIdentifier ident, String metadataFileLocation, boolean overwrite) {
    return delegate.registerTable(convertCase(ident), metadataFileLocation, overwrite);
  }

  @Override
  public void createNamespace(Namespace ns, Map<String, String> props) {
    nsDelegate.createNamespace(convertCase(ns), props);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace ns) throws NoSuchNamespaceException {
    return nsDelegate.listNamespaces(convertCase(ns));
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return nsDelegate.namespaceExists(convertCase(namespace));
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
    return nsDelegate.loadNamespaceMetadata(convertCase(ns));
  }

  @Override
  public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
    return nsDelegate.dropNamespace(convertCase(ns));
  }

  @Override
  public boolean setProperties(Namespace ns, Map<String, String> props)
      throws NoSuchNamespaceException {
    return nsDelegate.setProperties(convertCase(ns), props);
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    return nsDelegate.removeProperties(convertCase(ns), props);
  }

  @Override
  public void setConf(Object conf) {
    sessionCatalog.setConf(conf);
  }

  @Override
  public void close() throws IOException {
    sessionCatalog.close();
  }

  public void commitTransaction(List<TableCommit> commits) {
    sessionCatalog.commitTransaction(context, commits);
  }

  public void commitTransaction(TableCommit... commits) {
    sessionCatalog.commitTransaction(
        context, ImmutableList.<TableCommit>builder().add(commits).build());
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return viewSessionCatalog.listViews(convertCase(namespace));
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    return viewSessionCatalog.loadView(convertCase(identifier));
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return viewSessionCatalog.buildView(convertCase(identifier));
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return viewSessionCatalog.dropView(convertCase(identifier));
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    viewSessionCatalog.renameView(convertCase(from), convertCase(to));
  }

  @Override
  public boolean viewExists(TableIdentifier identifier) {
    return viewSessionCatalog.viewExists(convertCase(identifier));
  }

  @Override
  public void invalidateView(TableIdentifier identifier) {
    viewSessionCatalog.invalidateView(convertCase(identifier));
  }

  @Override
  public View registerView(TableIdentifier identifier, String metadataFileLocation) {
    return viewSessionCatalog.registerView(convertCase(identifier), metadataFileLocation);
  }
}
