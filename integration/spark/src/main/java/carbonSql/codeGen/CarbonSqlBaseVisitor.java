// Generated from /opt/carbondata/integration/spark/src/main/java/org/apache/spark/sql/CarbonSqlBase.g4 by ANTLR 4.8
package carbonSql.codeGen;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link CarbonSqlBaseParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 *            operations with no return type.
 */
public interface CarbonSqlBaseVisitor<T> extends ParseTreeVisitor<T> {
  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleStatement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleStatement(CarbonSqlBaseParser.SingleStatementContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleExpression(CarbonSqlBaseParser.SingleExpressionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleTableIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleTableIdentifier(CarbonSqlBaseParser.SingleTableIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleMultipartIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleMultipartIdentifier(CarbonSqlBaseParser.SingleMultipartIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleFunctionIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleFunctionIdentifier(CarbonSqlBaseParser.SingleFunctionIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleDataType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleDataType(CarbonSqlBaseParser.SingleDataTypeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#singleTableSchema}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleTableSchema(CarbonSqlBaseParser.SingleTableSchemaContext ctx);

  /**
   * Visit a parse tree produced by the {@code statementDefault}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStatementDefault(CarbonSqlBaseParser.StatementDefaultContext ctx);

  /**
   * Visit a parse tree produced by the {@code dmlStatement}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDmlStatement(CarbonSqlBaseParser.DmlStatementContext ctx);

  /**
   * Visit a parse tree produced by the {@code use}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUse(CarbonSqlBaseParser.UseContext ctx);

  /**
   * Visit a parse tree produced by the {@code createNamespace}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateNamespace(CarbonSqlBaseParser.CreateNamespaceContext ctx);

  /**
   * Visit a parse tree produced by the {@code setNamespaceProperties}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetNamespaceProperties(CarbonSqlBaseParser.SetNamespacePropertiesContext ctx);

  /**
   * Visit a parse tree produced by the {@code setNamespaceLocation}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetNamespaceLocation(CarbonSqlBaseParser.SetNamespaceLocationContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropNamespace}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropNamespace(CarbonSqlBaseParser.DropNamespaceContext ctx);

  /**
   * Visit a parse tree produced by the {@code showNamespaces}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowNamespaces(CarbonSqlBaseParser.ShowNamespacesContext ctx);

  /**
   * Visit a parse tree produced by the {@code createTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateTable(CarbonSqlBaseParser.CreateTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code createHiveTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateHiveTable(CarbonSqlBaseParser.CreateHiveTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code createTableLike}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateTableLike(CarbonSqlBaseParser.CreateTableLikeContext ctx);

  /**
   * Visit a parse tree produced by the {@code replaceTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitReplaceTable(CarbonSqlBaseParser.ReplaceTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code analyze}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAnalyze(CarbonSqlBaseParser.AnalyzeContext ctx);

  /**
   * Visit a parse tree produced by the {@code addTableColumns}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAddTableColumns(CarbonSqlBaseParser.AddTableColumnsContext ctx);

  /**
   * Visit a parse tree produced by the {@code renameTableColumn}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRenameTableColumn(CarbonSqlBaseParser.RenameTableColumnContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropTableColumns}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropTableColumns(CarbonSqlBaseParser.DropTableColumnsContext ctx);

  /**
   * Visit a parse tree produced by the {@code renameTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRenameTable(CarbonSqlBaseParser.RenameTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code setTableProperties}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetTableProperties(CarbonSqlBaseParser.SetTablePropertiesContext ctx);

  /**
   * Visit a parse tree produced by the {@code unsetTableProperties}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnsetTableProperties(CarbonSqlBaseParser.UnsetTablePropertiesContext ctx);

  /**
   * Visit a parse tree produced by the {@code alterTableAlterColumn}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAlterTableAlterColumn(CarbonSqlBaseParser.AlterTableAlterColumnContext ctx);

  /**
   * Visit a parse tree produced by the {@code hiveChangeColumn}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHiveChangeColumn(CarbonSqlBaseParser.HiveChangeColumnContext ctx);

  /**
   * Visit a parse tree produced by the {@code hiveReplaceColumns}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHiveReplaceColumns(CarbonSqlBaseParser.HiveReplaceColumnsContext ctx);

  /**
   * Visit a parse tree produced by the {@code setTableSerDe}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetTableSerDe(CarbonSqlBaseParser.SetTableSerDeContext ctx);

  /**
   * Visit a parse tree produced by the {@code addTablePartition}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAddTablePartition(CarbonSqlBaseParser.AddTablePartitionContext ctx);

  /**
   * Visit a parse tree produced by the {@code renameTablePartition}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRenameTablePartition(CarbonSqlBaseParser.RenameTablePartitionContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropTablePartitions}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropTablePartitions(CarbonSqlBaseParser.DropTablePartitionsContext ctx);

  /**
   * Visit a parse tree produced by the {@code setTableLocation}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetTableLocation(CarbonSqlBaseParser.SetTableLocationContext ctx);

  /**
   * Visit a parse tree produced by the {@code recoverPartitions}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRecoverPartitions(CarbonSqlBaseParser.RecoverPartitionsContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropTable(CarbonSqlBaseParser.DropTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropView}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropView(CarbonSqlBaseParser.DropViewContext ctx);

  /**
   * Visit a parse tree produced by the {@code createView}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateView(CarbonSqlBaseParser.CreateViewContext ctx);

  /**
   * Visit a parse tree produced by the {@code createTempViewUsing}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateTempViewUsing(CarbonSqlBaseParser.CreateTempViewUsingContext ctx);

  /**
   * Visit a parse tree produced by the {@code alterViewQuery}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAlterViewQuery(CarbonSqlBaseParser.AlterViewQueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code createFunction}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateFunction(CarbonSqlBaseParser.CreateFunctionContext ctx);

  /**
   * Visit a parse tree produced by the {@code dropFunction}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDropFunction(CarbonSqlBaseParser.DropFunctionContext ctx);

  /**
   * Visit a parse tree produced by the {@code explain}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExplain(CarbonSqlBaseParser.ExplainContext ctx);

  /**
   * Visit a parse tree produced by the {@code showTables}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowTables(CarbonSqlBaseParser.ShowTablesContext ctx);

  /**
   * Visit a parse tree produced by the {@code showTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowTable(CarbonSqlBaseParser.ShowTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code showTblProperties}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowTblProperties(CarbonSqlBaseParser.ShowTblPropertiesContext ctx);

  /**
   * Visit a parse tree produced by the {@code showColumns}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowColumns(CarbonSqlBaseParser.ShowColumnsContext ctx);

  /**
   * Visit a parse tree produced by the {@code showViews}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowViews(CarbonSqlBaseParser.ShowViewsContext ctx);

  /**
   * Visit a parse tree produced by the {@code showPartitions}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowPartitions(CarbonSqlBaseParser.ShowPartitionsContext ctx);

  /**
   * Visit a parse tree produced by the {@code showFunctions}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowFunctions(CarbonSqlBaseParser.ShowFunctionsContext ctx);

  /**
   * Visit a parse tree produced by the {@code showCreateTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowCreateTable(CarbonSqlBaseParser.ShowCreateTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code showCurrentNamespace}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitShowCurrentNamespace(CarbonSqlBaseParser.ShowCurrentNamespaceContext ctx);

  /**
   * Visit a parse tree produced by the {@code describeFunction}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeFunction(CarbonSqlBaseParser.DescribeFunctionContext ctx);

  /**
   * Visit a parse tree produced by the {@code describeNamespace}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeNamespace(CarbonSqlBaseParser.DescribeNamespaceContext ctx);

  /**
   * Visit a parse tree produced by the {@code describeRelation}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeRelation(CarbonSqlBaseParser.DescribeRelationContext ctx);

  /**
   * Visit a parse tree produced by the {@code describeQuery}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeQuery(CarbonSqlBaseParser.DescribeQueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code commentNamespace}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCommentNamespace(CarbonSqlBaseParser.CommentNamespaceContext ctx);

  /**
   * Visit a parse tree produced by the {@code commentTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCommentTable(CarbonSqlBaseParser.CommentTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code refreshTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRefreshTable(CarbonSqlBaseParser.RefreshTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code refreshFunction}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRefreshFunction(CarbonSqlBaseParser.RefreshFunctionContext ctx);

  /**
   * Visit a parse tree produced by the {@code refreshResource}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRefreshResource(CarbonSqlBaseParser.RefreshResourceContext ctx);

  /**
   * Visit a parse tree produced by the {@code cacheTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCacheTable(CarbonSqlBaseParser.CacheTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code uncacheTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUncacheTable(CarbonSqlBaseParser.UncacheTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code clearCache}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitClearCache(CarbonSqlBaseParser.ClearCacheContext ctx);

  /**
   * Visit a parse tree produced by the {@code loadData}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLoadData(CarbonSqlBaseParser.LoadDataContext ctx);

  /**
   * Visit a parse tree produced by the {@code truncateTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTruncateTable(CarbonSqlBaseParser.TruncateTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code repairTable}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRepairTable(CarbonSqlBaseParser.RepairTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code manageResource}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitManageResource(CarbonSqlBaseParser.ManageResourceContext ctx);

  /**
   * Visit a parse tree produced by the {@code failNativeCommand}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFailNativeCommand(CarbonSqlBaseParser.FailNativeCommandContext ctx);

  /**
   * Visit a parse tree produced by the {@code setTimeZone}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetTimeZone(CarbonSqlBaseParser.SetTimeZoneContext ctx);

  /**
   * Visit a parse tree produced by the {@code setQuotedConfiguration}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetQuotedConfiguration(CarbonSqlBaseParser.SetQuotedConfigurationContext ctx);

  /**
   * Visit a parse tree produced by the {@code setConfiguration}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetConfiguration(CarbonSqlBaseParser.SetConfigurationContext ctx);

  /**
   * Visit a parse tree produced by the {@code resetQuotedConfiguration}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitResetQuotedConfiguration(CarbonSqlBaseParser.ResetQuotedConfigurationContext ctx);

  /**
   * Visit a parse tree produced by the {@code resetConfiguration}
   * labeled alternative in {@link CarbonSqlBaseParser#statement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitResetConfiguration(CarbonSqlBaseParser.ResetConfigurationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#configKey}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConfigKey(CarbonSqlBaseParser.ConfigKeyContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#unsupportedHiveNativeCommands}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnsupportedHiveNativeCommands(
      CarbonSqlBaseParser.UnsupportedHiveNativeCommandsContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#createTableHeader}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateTableHeader(CarbonSqlBaseParser.CreateTableHeaderContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#replaceTableHeader}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitReplaceTableHeader(CarbonSqlBaseParser.ReplaceTableHeaderContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#bucketSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBucketSpec(CarbonSqlBaseParser.BucketSpecContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#skewSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSkewSpec(CarbonSqlBaseParser.SkewSpecContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#locationSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLocationSpec(CarbonSqlBaseParser.LocationSpecContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#commentSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCommentSpec(CarbonSqlBaseParser.CommentSpecContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#query}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQuery(CarbonSqlBaseParser.QueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code insertOverwriteTable}
   * labeled alternative in {@link CarbonSqlBaseParser#insertInto}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInsertOverwriteTable(CarbonSqlBaseParser.InsertOverwriteTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code insertIntoTable}
   * labeled alternative in {@link CarbonSqlBaseParser#insertInto}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInsertIntoTable(CarbonSqlBaseParser.InsertIntoTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code insertOverwriteHiveDir}
   * labeled alternative in {@link CarbonSqlBaseParser#insertInto}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInsertOverwriteHiveDir(CarbonSqlBaseParser.InsertOverwriteHiveDirContext ctx);

  /**
   * Visit a parse tree produced by the {@code insertOverwriteDir}
   * labeled alternative in {@link CarbonSqlBaseParser#insertInto}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInsertOverwriteDir(CarbonSqlBaseParser.InsertOverwriteDirContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#partitionSpecLocation}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPartitionSpecLocation(CarbonSqlBaseParser.PartitionSpecLocationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#partitionSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPartitionSpec(CarbonSqlBaseParser.PartitionSpecContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#partitionVal}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPartitionVal(CarbonSqlBaseParser.PartitionValContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#namespace}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNamespace(CarbonSqlBaseParser.NamespaceContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#describeFuncName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeFuncName(CarbonSqlBaseParser.DescribeFuncNameContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#describeColName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDescribeColName(CarbonSqlBaseParser.DescribeColNameContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#ctes}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCtes(CarbonSqlBaseParser.CtesContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#namedQuery}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNamedQuery(CarbonSqlBaseParser.NamedQueryContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tableProvider}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableProvider(CarbonSqlBaseParser.TableProviderContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#createTableClauses}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateTableClauses(CarbonSqlBaseParser.CreateTableClausesContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tablePropertyList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTablePropertyList(CarbonSqlBaseParser.TablePropertyListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tableProperty}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableProperty(CarbonSqlBaseParser.TablePropertyContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tablePropertyKey}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTablePropertyKey(CarbonSqlBaseParser.TablePropertyKeyContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tablePropertyValue}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTablePropertyValue(CarbonSqlBaseParser.TablePropertyValueContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#constantList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantList(CarbonSqlBaseParser.ConstantListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#nestedConstantList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNestedConstantList(CarbonSqlBaseParser.NestedConstantListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#createFileFormat}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCreateFileFormat(CarbonSqlBaseParser.CreateFileFormatContext ctx);

  /**
   * Visit a parse tree produced by the {@code tableFileFormat}
   * labeled alternative in {@link CarbonSqlBaseParser#fileFormat}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableFileFormat(CarbonSqlBaseParser.TableFileFormatContext ctx);

  /**
   * Visit a parse tree produced by the {@code genericFileFormat}
   * labeled alternative in {@link CarbonSqlBaseParser#fileFormat}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitGenericFileFormat(CarbonSqlBaseParser.GenericFileFormatContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#storageHandler}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStorageHandler(CarbonSqlBaseParser.StorageHandlerContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#resource}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitResource(CarbonSqlBaseParser.ResourceContext ctx);

  /**
   * Visit a parse tree produced by the {@code singleInsertQuery}
   * labeled alternative in {@link CarbonSqlBaseParser#dmlStatementNoWith}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSingleInsertQuery(CarbonSqlBaseParser.SingleInsertQueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code multiInsertQuery}
   * labeled alternative in {@link CarbonSqlBaseParser#dmlStatementNoWith}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultiInsertQuery(CarbonSqlBaseParser.MultiInsertQueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code deleteFromTable}
   * labeled alternative in {@link CarbonSqlBaseParser#dmlStatementNoWith}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDeleteFromTable(CarbonSqlBaseParser.DeleteFromTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code updateTable}
   * labeled alternative in {@link CarbonSqlBaseParser#dmlStatementNoWith}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUpdateTable(CarbonSqlBaseParser.UpdateTableContext ctx);

  /**
   * Visit a parse tree produced by the {@code mergeIntoTable}
   * labeled alternative in {@link CarbonSqlBaseParser#dmlStatementNoWith}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMergeIntoTable(CarbonSqlBaseParser.MergeIntoTableContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#mergeInto}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMergeInto(CarbonSqlBaseParser.MergeIntoContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#queryOrganization}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQueryOrganization(CarbonSqlBaseParser.QueryOrganizationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#multiInsertQueryBody}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultiInsertQueryBody(CarbonSqlBaseParser.MultiInsertQueryBodyContext ctx);

  /**
   * Visit a parse tree produced by the {@code queryTermDefault}
   * labeled alternative in {@link CarbonSqlBaseParser#queryTerm}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQueryTermDefault(CarbonSqlBaseParser.QueryTermDefaultContext ctx);

  /**
   * Visit a parse tree produced by the {@code setOperation}
   * labeled alternative in {@link CarbonSqlBaseParser#queryTerm}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetOperation(CarbonSqlBaseParser.SetOperationContext ctx);

  /**
   * Visit a parse tree produced by the {@code queryPrimaryDefault}
   * labeled alternative in {@link CarbonSqlBaseParser#queryPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQueryPrimaryDefault(CarbonSqlBaseParser.QueryPrimaryDefaultContext ctx);

  /**
   * Visit a parse tree produced by the {@code fromStmt}
   * labeled alternative in {@link CarbonSqlBaseParser#queryPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFromStmt(CarbonSqlBaseParser.FromStmtContext ctx);

  /**
   * Visit a parse tree produced by the {@code table}
   * labeled alternative in {@link CarbonSqlBaseParser#queryPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTable(CarbonSqlBaseParser.TableContext ctx);

  /**
   * Visit a parse tree produced by the {@code inlineTableDefault1}
   * labeled alternative in {@link CarbonSqlBaseParser#queryPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInlineTableDefault1(CarbonSqlBaseParser.InlineTableDefault1Context ctx);

  /**
   * Visit a parse tree produced by the {@code subquery}
   * labeled alternative in {@link CarbonSqlBaseParser#queryPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubquery(CarbonSqlBaseParser.SubqueryContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#sortItem}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSortItem(CarbonSqlBaseParser.SortItemContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#fromStatement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFromStatement(CarbonSqlBaseParser.FromStatementContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#fromStatementBody}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFromStatementBody(CarbonSqlBaseParser.FromStatementBodyContext ctx);

  /**
   * Visit a parse tree produced by the {@code transformQuerySpecification}
   * labeled alternative in {@link CarbonSqlBaseParser#querySpecification}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTransformQuerySpecification(CarbonSqlBaseParser.TransformQuerySpecificationContext ctx);

  /**
   * Visit a parse tree produced by the {@code regularQuerySpecification}
   * labeled alternative in {@link CarbonSqlBaseParser#querySpecification}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRegularQuerySpecification(CarbonSqlBaseParser.RegularQuerySpecificationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#transformClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTransformClause(CarbonSqlBaseParser.TransformClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#selectClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSelectClause(CarbonSqlBaseParser.SelectClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#setClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetClause(CarbonSqlBaseParser.SetClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#matchedClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMatchedClause(CarbonSqlBaseParser.MatchedClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#notMatchedClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNotMatchedClause(CarbonSqlBaseParser.NotMatchedClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#matchedAction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMatchedAction(CarbonSqlBaseParser.MatchedActionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#notMatchedAction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNotMatchedAction(CarbonSqlBaseParser.NotMatchedActionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#assignmentList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAssignmentList(CarbonSqlBaseParser.AssignmentListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#assignment}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAssignment(CarbonSqlBaseParser.AssignmentContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#whereClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWhereClause(CarbonSqlBaseParser.WhereClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#havingClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHavingClause(CarbonSqlBaseParser.HavingClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#hint}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHint(CarbonSqlBaseParser.HintContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#hintStatement}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitHintStatement(CarbonSqlBaseParser.HintStatementContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#fromClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFromClause(CarbonSqlBaseParser.FromClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#aggregationClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAggregationClause(CarbonSqlBaseParser.AggregationClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#groupingSet}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitGroupingSet(CarbonSqlBaseParser.GroupingSetContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#pivotClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPivotClause(CarbonSqlBaseParser.PivotClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#pivotColumn}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPivotColumn(CarbonSqlBaseParser.PivotColumnContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#pivotValue}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPivotValue(CarbonSqlBaseParser.PivotValueContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#lateralView}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLateralView(CarbonSqlBaseParser.LateralViewContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#setQuantifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSetQuantifier(CarbonSqlBaseParser.SetQuantifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#relation}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRelation(CarbonSqlBaseParser.RelationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#joinRelation}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinRelation(CarbonSqlBaseParser.JoinRelationContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#joinType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinType(CarbonSqlBaseParser.JoinTypeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#joinCriteria}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitJoinCriteria(CarbonSqlBaseParser.JoinCriteriaContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#sample}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSample(CarbonSqlBaseParser.SampleContext ctx);

  /**
   * Visit a parse tree produced by the {@code sampleByPercentile}
   * labeled alternative in {@link CarbonSqlBaseParser#sampleMethod}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSampleByPercentile(CarbonSqlBaseParser.SampleByPercentileContext ctx);

  /**
   * Visit a parse tree produced by the {@code sampleByRows}
   * labeled alternative in {@link CarbonSqlBaseParser#sampleMethod}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSampleByRows(CarbonSqlBaseParser.SampleByRowsContext ctx);

  /**
   * Visit a parse tree produced by the {@code sampleByBucket}
   * labeled alternative in {@link CarbonSqlBaseParser#sampleMethod}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSampleByBucket(CarbonSqlBaseParser.SampleByBucketContext ctx);

  /**
   * Visit a parse tree produced by the {@code sampleByBytes}
   * labeled alternative in {@link CarbonSqlBaseParser#sampleMethod}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSampleByBytes(CarbonSqlBaseParser.SampleByBytesContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#identifierList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierList(CarbonSqlBaseParser.IdentifierListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#identifierSeq}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierSeq(CarbonSqlBaseParser.IdentifierSeqContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#orderedIdentifierList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOrderedIdentifierList(CarbonSqlBaseParser.OrderedIdentifierListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#orderedIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOrderedIdentifier(CarbonSqlBaseParser.OrderedIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#identifierCommentList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierCommentList(CarbonSqlBaseParser.IdentifierCommentListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#identifierComment}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifierComment(CarbonSqlBaseParser.IdentifierCommentContext ctx);

  /**
   * Visit a parse tree produced by the {@code tableName}
   * labeled alternative in {@link CarbonSqlBaseParser#relationPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableName(CarbonSqlBaseParser.TableNameContext ctx);

  /**
   * Visit a parse tree produced by the {@code aliasedQuery}
   * labeled alternative in {@link CarbonSqlBaseParser#relationPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAliasedQuery(CarbonSqlBaseParser.AliasedQueryContext ctx);

  /**
   * Visit a parse tree produced by the {@code aliasedRelation}
   * labeled alternative in {@link CarbonSqlBaseParser#relationPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAliasedRelation(CarbonSqlBaseParser.AliasedRelationContext ctx);

  /**
   * Visit a parse tree produced by the {@code inlineTableDefault2}
   * labeled alternative in {@link CarbonSqlBaseParser#relationPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInlineTableDefault2(CarbonSqlBaseParser.InlineTableDefault2Context ctx);

  /**
   * Visit a parse tree produced by the {@code tableValuedFunction}
   * labeled alternative in {@link CarbonSqlBaseParser#relationPrimary}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableValuedFunction(CarbonSqlBaseParser.TableValuedFunctionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#inlineTable}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInlineTable(CarbonSqlBaseParser.InlineTableContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#functionTable}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionTable(CarbonSqlBaseParser.FunctionTableContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tableAlias}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableAlias(CarbonSqlBaseParser.TableAliasContext ctx);

  /**
   * Visit a parse tree produced by the {@code rowFormatSerde}
   * labeled alternative in {@link CarbonSqlBaseParser#rowFormat}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRowFormatSerde(CarbonSqlBaseParser.RowFormatSerdeContext ctx);

  /**
   * Visit a parse tree produced by the {@code rowFormatDelimited}
   * labeled alternative in {@link CarbonSqlBaseParser#rowFormat}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRowFormatDelimited(CarbonSqlBaseParser.RowFormatDelimitedContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#multipartIdentifierList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultipartIdentifierList(CarbonSqlBaseParser.MultipartIdentifierListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#multipartIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultipartIdentifier(CarbonSqlBaseParser.MultipartIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#tableIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTableIdentifier(CarbonSqlBaseParser.TableIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#functionIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionIdentifier(CarbonSqlBaseParser.FunctionIdentifierContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#namedExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNamedExpression(CarbonSqlBaseParser.NamedExpressionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#namedExpressionSeq}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNamedExpressionSeq(CarbonSqlBaseParser.NamedExpressionSeqContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#transformList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTransformList(CarbonSqlBaseParser.TransformListContext ctx);

  /**
   * Visit a parse tree produced by the {@code identityTransform}
   * labeled alternative in {@link CarbonSqlBaseParser#transform}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentityTransform(CarbonSqlBaseParser.IdentityTransformContext ctx);

  /**
   * Visit a parse tree produced by the {@code applyTransform}
   * labeled alternative in {@link CarbonSqlBaseParser#transform}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitApplyTransform(CarbonSqlBaseParser.ApplyTransformContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#transformArgument}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTransformArgument(CarbonSqlBaseParser.TransformArgumentContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#expression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExpression(CarbonSqlBaseParser.ExpressionContext ctx);

  /**
   * Visit a parse tree produced by the {@code logicalNot}
   * labeled alternative in {@link CarbonSqlBaseParser#booleanExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalNot(CarbonSqlBaseParser.LogicalNotContext ctx);

  /**
   * Visit a parse tree produced by the {@code predicated}
   * labeled alternative in {@link CarbonSqlBaseParser#booleanExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPredicated(CarbonSqlBaseParser.PredicatedContext ctx);

  /**
   * Visit a parse tree produced by the {@code exists}
   * labeled alternative in {@link CarbonSqlBaseParser#booleanExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExists(CarbonSqlBaseParser.ExistsContext ctx);

  /**
   * Visit a parse tree produced by the {@code logicalBinary}
   * labeled alternative in {@link CarbonSqlBaseParser#booleanExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLogicalBinary(CarbonSqlBaseParser.LogicalBinaryContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#predicate}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPredicate(CarbonSqlBaseParser.PredicateContext ctx);

  /**
   * Visit a parse tree produced by the {@code valueExpressionDefault}
   * labeled alternative in {@link CarbonSqlBaseParser#valueExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitValueExpressionDefault(CarbonSqlBaseParser.ValueExpressionDefaultContext ctx);

  /**
   * Visit a parse tree produced by the {@code comparison}
   * labeled alternative in {@link CarbonSqlBaseParser#valueExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparison(CarbonSqlBaseParser.ComparisonContext ctx);

  /**
   * Visit a parse tree produced by the {@code arithmeticBinary}
   * labeled alternative in {@link CarbonSqlBaseParser#valueExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticBinary(CarbonSqlBaseParser.ArithmeticBinaryContext ctx);

  /**
   * Visit a parse tree produced by the {@code arithmeticUnary}
   * labeled alternative in {@link CarbonSqlBaseParser#valueExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticUnary(CarbonSqlBaseParser.ArithmeticUnaryContext ctx);

  /**
   * Visit a parse tree produced by the {@code struct}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStruct(CarbonSqlBaseParser.StructContext ctx);

  /**
   * Visit a parse tree produced by the {@code dereference}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDereference(CarbonSqlBaseParser.DereferenceContext ctx);

  /**
   * Visit a parse tree produced by the {@code simpleCase}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSimpleCase(CarbonSqlBaseParser.SimpleCaseContext ctx);

  /**
   * Visit a parse tree produced by the {@code columnReference}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitColumnReference(CarbonSqlBaseParser.ColumnReferenceContext ctx);

  /**
   * Visit a parse tree produced by the {@code rowConstructor}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRowConstructor(CarbonSqlBaseParser.RowConstructorContext ctx);

  /**
   * Visit a parse tree produced by the {@code last}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLast(CarbonSqlBaseParser.LastContext ctx);

  /**
   * Visit a parse tree produced by the {@code star}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStar(CarbonSqlBaseParser.StarContext ctx);

  /**
   * Visit a parse tree produced by the {@code overlay}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitOverlay(CarbonSqlBaseParser.OverlayContext ctx);

  /**
   * Visit a parse tree produced by the {@code subscript}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubscript(CarbonSqlBaseParser.SubscriptContext ctx);

  /**
   * Visit a parse tree produced by the {@code subqueryExpression}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubqueryExpression(CarbonSqlBaseParser.SubqueryExpressionContext ctx);

  /**
   * Visit a parse tree produced by the {@code substring}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSubstring(CarbonSqlBaseParser.SubstringContext ctx);

  /**
   * Visit a parse tree produced by the {@code currentDatetime}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCurrentDatetime(CarbonSqlBaseParser.CurrentDatetimeContext ctx);

  /**
   * Visit a parse tree produced by the {@code cast}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitCast(CarbonSqlBaseParser.CastContext ctx);

  /**
   * Visit a parse tree produced by the {@code constantDefault}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitConstantDefault(CarbonSqlBaseParser.ConstantDefaultContext ctx);

  /**
   * Visit a parse tree produced by the {@code lambda}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLambda(CarbonSqlBaseParser.LambdaContext ctx);

  /**
   * Visit a parse tree produced by the {@code parenthesizedExpression}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitParenthesizedExpression(CarbonSqlBaseParser.ParenthesizedExpressionContext ctx);

  /**
   * Visit a parse tree produced by the {@code extract}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExtract(CarbonSqlBaseParser.ExtractContext ctx);

  /**
   * Visit a parse tree produced by the {@code trim}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTrim(CarbonSqlBaseParser.TrimContext ctx);

  /**
   * Visit a parse tree produced by the {@code functionCall}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionCall(CarbonSqlBaseParser.FunctionCallContext ctx);

  /**
   * Visit a parse tree produced by the {@code searchedCase}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSearchedCase(CarbonSqlBaseParser.SearchedCaseContext ctx);

  /**
   * Visit a parse tree produced by the {@code position}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPosition(CarbonSqlBaseParser.PositionContext ctx);

  /**
   * Visit a parse tree produced by the {@code first}
   * labeled alternative in {@link CarbonSqlBaseParser#primaryExpression}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFirst(CarbonSqlBaseParser.FirstContext ctx);

  /**
   * Visit a parse tree produced by the {@code nullLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNullLiteral(CarbonSqlBaseParser.NullLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code intervalLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntervalLiteral(CarbonSqlBaseParser.IntervalLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code typeConstructor}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTypeConstructor(CarbonSqlBaseParser.TypeConstructorContext ctx);

  /**
   * Visit a parse tree produced by the {@code numericLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNumericLiteral(CarbonSqlBaseParser.NumericLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code booleanLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanLiteral(CarbonSqlBaseParser.BooleanLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code stringLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#constant}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStringLiteral(CarbonSqlBaseParser.StringLiteralContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#comparisonOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComparisonOperator(CarbonSqlBaseParser.ComparisonOperatorContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#arithmeticOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitArithmeticOperator(CarbonSqlBaseParser.ArithmeticOperatorContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#predicateOperator}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPredicateOperator(CarbonSqlBaseParser.PredicateOperatorContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#booleanValue}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBooleanValue(CarbonSqlBaseParser.BooleanValueContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#interval}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitInterval(CarbonSqlBaseParser.IntervalContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#errorCapturingMultiUnitsInterval}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitErrorCapturingMultiUnitsInterval(
      CarbonSqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#multiUnitsInterval}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitMultiUnitsInterval(CarbonSqlBaseParser.MultiUnitsIntervalContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#errorCapturingUnitToUnitInterval}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitErrorCapturingUnitToUnitInterval(
      CarbonSqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#unitToUnitInterval}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnitToUnitInterval(CarbonSqlBaseParser.UnitToUnitIntervalContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#intervalValue}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntervalValue(CarbonSqlBaseParser.IntervalValueContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#colPosition}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitColPosition(CarbonSqlBaseParser.ColPositionContext ctx);

  /**
   * Visit a parse tree produced by the {@code complexDataType}
   * labeled alternative in {@link CarbonSqlBaseParser#dataType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComplexDataType(CarbonSqlBaseParser.ComplexDataTypeContext ctx);

  /**
   * Visit a parse tree produced by the {@code primitiveDataType}
   * labeled alternative in {@link CarbonSqlBaseParser#dataType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitPrimitiveDataType(CarbonSqlBaseParser.PrimitiveDataTypeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#qualifiedColTypeWithPositionList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedColTypeWithPositionList(
      CarbonSqlBaseParser.QualifiedColTypeWithPositionListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#qualifiedColTypeWithPosition}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedColTypeWithPosition(CarbonSqlBaseParser.QualifiedColTypeWithPositionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#colTypeList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitColTypeList(CarbonSqlBaseParser.ColTypeListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#colType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitColType(CarbonSqlBaseParser.ColTypeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#complexColTypeList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComplexColTypeList(CarbonSqlBaseParser.ComplexColTypeListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#complexColType}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitComplexColType(CarbonSqlBaseParser.ComplexColTypeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#whenClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWhenClause(CarbonSqlBaseParser.WhenClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#windowClause}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWindowClause(CarbonSqlBaseParser.WindowClauseContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#namedWindow}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNamedWindow(CarbonSqlBaseParser.NamedWindowContext ctx);

  /**
   * Visit a parse tree produced by the {@code windowRef}
   * labeled alternative in {@link CarbonSqlBaseParser#windowSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWindowRef(CarbonSqlBaseParser.WindowRefContext ctx);

  /**
   * Visit a parse tree produced by the {@code windowDef}
   * labeled alternative in {@link CarbonSqlBaseParser#windowSpec}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWindowDef(CarbonSqlBaseParser.WindowDefContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#windowFrame}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitWindowFrame(CarbonSqlBaseParser.WindowFrameContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#frameBound}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFrameBound(CarbonSqlBaseParser.FrameBoundContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#qualifiedNameList}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedNameList(CarbonSqlBaseParser.QualifiedNameListContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#functionName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFunctionName(CarbonSqlBaseParser.FunctionNameContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#qualifiedName}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQualifiedName(CarbonSqlBaseParser.QualifiedNameContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#errorCapturingIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitErrorCapturingIdentifier(CarbonSqlBaseParser.ErrorCapturingIdentifierContext ctx);

  /**
   * Visit a parse tree produced by the {@code errorIdent}
   * labeled alternative in {@link CarbonSqlBaseParser#errorCapturingIdentifierExtra}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitErrorIdent(CarbonSqlBaseParser.ErrorIdentContext ctx);

  /**
   * Visit a parse tree produced by the {@code realIdent}
   * labeled alternative in {@link CarbonSqlBaseParser#errorCapturingIdentifierExtra}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitRealIdent(CarbonSqlBaseParser.RealIdentContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#identifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIdentifier(CarbonSqlBaseParser.IdentifierContext ctx);

  /**
   * Visit a parse tree produced by the {@code unquotedIdentifier}
   * labeled alternative in {@link CarbonSqlBaseParser#strictIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitUnquotedIdentifier(CarbonSqlBaseParser.UnquotedIdentifierContext ctx);

  /**
   * Visit a parse tree produced by the {@code quotedIdentifierAlternative}
   * labeled alternative in {@link CarbonSqlBaseParser#strictIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQuotedIdentifierAlternative(CarbonSqlBaseParser.QuotedIdentifierAlternativeContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#quotedIdentifier}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitQuotedIdentifier(CarbonSqlBaseParser.QuotedIdentifierContext ctx);

  /**
   * Visit a parse tree produced by the {@code exponentLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitExponentLiteral(CarbonSqlBaseParser.ExponentLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code decimalLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDecimalLiteral(CarbonSqlBaseParser.DecimalLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code legacyDecimalLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitLegacyDecimalLiteral(CarbonSqlBaseParser.LegacyDecimalLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code integerLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitIntegerLiteral(CarbonSqlBaseParser.IntegerLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code bigIntLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBigIntLiteral(CarbonSqlBaseParser.BigIntLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code smallIntLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitSmallIntLiteral(CarbonSqlBaseParser.SmallIntLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code tinyIntLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitTinyIntLiteral(CarbonSqlBaseParser.TinyIntLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code doubleLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitDoubleLiteral(CarbonSqlBaseParser.DoubleLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code floatLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitFloatLiteral(CarbonSqlBaseParser.FloatLiteralContext ctx);

  /**
   * Visit a parse tree produced by the {@code bigDecimalLiteral}
   * labeled alternative in {@link CarbonSqlBaseParser#number}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitBigDecimalLiteral(CarbonSqlBaseParser.BigDecimalLiteralContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#alterColumnAction}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAlterColumnAction(CarbonSqlBaseParser.AlterColumnActionContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#ansiNonReserved}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitAnsiNonReserved(CarbonSqlBaseParser.AnsiNonReservedContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#strictNonReserved}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitStrictNonReserved(CarbonSqlBaseParser.StrictNonReservedContext ctx);

  /**
   * Visit a parse tree produced by {@link CarbonSqlBaseParser#nonReserved}.
   *
   * @param ctx the parse tree
   * @return the visitor result
   */
  T visitNonReserved(CarbonSqlBaseParser.NonReservedContext ctx);
}