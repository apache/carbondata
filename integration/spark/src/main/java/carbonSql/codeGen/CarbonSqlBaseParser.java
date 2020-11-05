// Generated from /opt/carbondata/integration/spark/src/main/java/org/apache/spark/sql/CarbonSqlBase.g4 by ANTLR 4.8
package carbonSql.codeGen;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.Utils;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
public class CarbonSqlBaseParser extends Parser {
  static {
    RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION);
  }

  protected static final DFA[] _decisionToDFA;
  protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();
  public static final int T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7,
      T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, ADD = 12, AFTER = 13, ALL = 14, ALTER = 15,
      ANALYZE = 16, AND = 17, ANTI = 18, ANY = 19, ARCHIVE = 20, ARRAY = 21, AS = 22, ASC = 23, AT =
      24, AUTHORIZATION = 25, BETWEEN = 26, BOTH = 27, BUCKET = 28, BUCKETS = 29, BY = 30, CACHE =
      31, CASCADE = 32, CASE = 33, CAST = 34, CHANGE = 35, CHECK = 36, CLEAR = 37, CLUSTER = 38,
      CLUSTERED = 39, CODEGEN = 40, COLLATE = 41, COLLECTION = 42, COLUMN = 43, COLUMNS = 44,
      COMMENT = 45, COMMIT = 46, COMPACT = 47, COMPACTIONS = 48, COMPUTE = 49, CONCATENATE = 50,
      CONSTRAINT = 51, COST = 52, CREATE = 53, CROSS = 54, CUBE = 55, CURRENT = 56, CURRENT_DATE =
      57, CURRENT_TIME = 58, CURRENT_TIMESTAMP = 59, CURRENT_USER = 60, DATA = 61, DATABASE = 62,
      DATABASES = 63, DBPROPERTIES = 64, DEFINED = 65, DELETE = 66, DELIMITED = 67, DESC = 68,
      DESCRIBE = 69, DFS = 70, DIRECTORIES = 71, DIRECTORY = 72, DISTINCT = 73, DISTRIBUTE = 74,
      DIV = 75, DROP = 76, ELSE = 77, END = 78, ESCAPE = 79, ESCAPED = 80, EXCEPT = 81, EXCHANGE =
      82, EXISTS = 83, EXPLAIN = 84, EXPORT = 85, EXTENDED = 86, EXTERNAL = 87, EXTRACT = 88,
      FALSE = 89, FETCH = 90, FIELDS = 91, FILTER = 92, FILEFORMAT = 93, FIRST = 94, FOLLOWING = 95,
      FOR = 96, FOREIGN = 97, FORMAT = 98, FORMATTED = 99, FROM = 100, FULL = 101, FUNCTION = 102,
      FUNCTIONS = 103, GLOBAL = 104, GRANT = 105, GROUP = 106, GROUPING = 107, HAVING = 108, IF =
      109, IGNORE = 110, IMPORT = 111, IN = 112, INDEX = 113, INDEXES = 114, INNER = 115, INPATH =
      116, INPUTFORMAT = 117, INSERT = 118, INTERSECT = 119, INTERVAL = 120, INTO = 121, IS = 122,
      ITEMS = 123, JOIN = 124, KEYS = 125, LAST = 126, LATERAL = 127, LAZY = 128, LEADING = 129,
      LEFT = 130, LIKE = 131, LIMIT = 132, LINES = 133, LIST = 134, LOAD = 135, LOCAL = 136,
      LOCATION = 137, LOCK = 138, LOCKS = 139, LOGICAL = 140, MACRO = 141, MAP = 142, MATCHED = 143,
      MERGE = 144, MSCK = 145, NAMESPACE = 146, NAMESPACES = 147, NATURAL = 148, NO = 149, NOT =
      150, NULL = 151, NULLS = 152, OF = 153, ON = 154, ONLY = 155, OPTION = 156, OPTIONS = 157,
      OR = 158, ORDER = 159, OUT = 160, OUTER = 161, OUTPUTFORMAT = 162, OVER = 163, OVERLAPS = 164,
      OVERLAY = 165, OVERWRITE = 166, PARTITION = 167, PARTITIONED = 168, PARTITIONS = 169,
      PERCENTLIT = 170, PIVOT = 171, PLACING = 172, POSITION = 173, PRECEDING = 174, PRIMARY = 175,
      PRINCIPALS = 176, PROPERTIES = 177, PURGE = 178, QUERY = 179, RANGE = 180, RECORDREADER = 181,
      RECORDWRITER = 182, RECOVER = 183, REDUCE = 184, REFERENCES = 185, REFRESH = 186, RENAME =
      187, REPAIR = 188, REPLACE = 189, RESET = 190, RESTRICT = 191, REVOKE = 192, RIGHT = 193,
      RLIKE = 194, ROLE = 195, ROLES = 196, ROLLBACK = 197, ROLLUP = 198, ROW = 199, ROWS = 200,
      SCHEMA = 201, SELECT = 202, SEMI = 203, SEPARATED = 204, SERDE = 205, SERDEPROPERTIES = 206,
      SESSION_USER = 207, SET = 208, SETMINUS = 209, SETS = 210, SHOW = 211, SKEWED = 212, SOME =
      213, SORT = 214, SORTED = 215, START = 216, STATISTICS = 217, STORED = 218, STRATIFY = 219,
      STRUCT = 220, SUBSTR = 221, SUBSTRING = 222, TABLE = 223, TABLES = 224, TABLESAMPLE = 225,
      TBLPROPERTIES = 226, TEMPORARY = 227, TERMINATED = 228, THEN = 229, TIME = 230, TO = 231,
      TOUCH = 232, TRAILING = 233, TRANSACTION = 234, TRANSACTIONS = 235, TRANSFORM = 236, TRIM =
      237, TRUE = 238, TRUNCATE = 239, TYPE = 240, UNARCHIVE = 241, UNBOUNDED = 242, UNCACHE = 243,
      UNION = 244, UNIQUE = 245, UNKNOWN = 246, UNLOCK = 247, UNSET = 248, UPDATE = 249, USE = 250,
      USER = 251, USING = 252, VALUES = 253, VIEW = 254, VIEWS = 255, WHEN = 256, WHERE = 257,
      WINDOW = 258, WITH = 259, ZONE = 260, EQ = 261, NSEQ = 262, NEQ = 263, NEQJ = 264, LT = 265,
      LTE = 266, GT = 267, GTE = 268, PLUS = 269, MINUS = 270, ASTERISK = 271, SLASH = 272,
      PERCENT = 273, TILDE = 274, AMPERSAND = 275, PIPE = 276, CONCAT_PIPE = 277, HAT = 278,
      STRING = 279, BIGINT_LITERAL = 280, SMALLINT_LITERAL = 281, TINYINT_LITERAL = 282,
      INTEGER_VALUE = 283, EXPONENT_VALUE = 284, DECIMAL_VALUE = 285, FLOAT_LITERAL = 286,
      DOUBLE_LITERAL = 287, BIGDECIMAL_LITERAL = 288, IDENTIFIER = 289, BACKQUOTED_IDENTIFIER = 290,
      SIMPLE_COMMENT = 291, BRACKETED_COMMENT = 292, WS = 293, UNRECOGNIZED = 294;
  public static final int RULE_singleStatement = 0, RULE_singleExpression = 1,
      RULE_singleTableIdentifier = 2, RULE_singleMultipartIdentifier = 3,
      RULE_singleFunctionIdentifier = 4, RULE_singleDataType = 5, RULE_singleTableSchema = 6,
      RULE_statement = 7, RULE_configKey = 8, RULE_unsupportedHiveNativeCommands = 9,
      RULE_createTableHeader = 10, RULE_replaceTableHeader = 11, RULE_bucketSpec = 12,
      RULE_skewSpec = 13, RULE_locationSpec = 14, RULE_commentSpec = 15, RULE_query = 16,
      RULE_insertInto = 17, RULE_partitionSpecLocation = 18, RULE_partitionSpec = 19,
      RULE_partitionVal = 20, RULE_namespace = 21, RULE_describeFuncName = 22,
      RULE_describeColName = 23, RULE_ctes = 24, RULE_namedQuery = 25, RULE_tableProvider = 26,
      RULE_createTableClauses = 27, RULE_tablePropertyList = 28, RULE_tableProperty = 29,
      RULE_tablePropertyKey = 30, RULE_tablePropertyValue = 31, RULE_constantList = 32,
      RULE_nestedConstantList = 33, RULE_createFileFormat = 34, RULE_fileFormat = 35,
      RULE_storageHandler = 36, RULE_resource = 37, RULE_dmlStatementNoWith = 38, RULE_mergeInto =
      39, RULE_queryOrganization = 40, RULE_multiInsertQueryBody = 41, RULE_queryTerm = 42,
      RULE_queryPrimary = 43, RULE_sortItem = 44, RULE_fromStatement = 45, RULE_fromStatementBody =
      46, RULE_querySpecification = 47, RULE_transformClause = 48, RULE_selectClause = 49,
      RULE_setClause = 50, RULE_matchedClause = 51, RULE_notMatchedClause = 52, RULE_matchedAction =
      53, RULE_notMatchedAction = 54, RULE_assignmentList = 55, RULE_assignment = 56,
      RULE_whereClause = 57, RULE_havingClause = 58, RULE_hint = 59, RULE_hintStatement = 60,
      RULE_fromClause = 61, RULE_aggregationClause = 62, RULE_groupingSet = 63, RULE_pivotClause =
      64, RULE_pivotColumn = 65, RULE_pivotValue = 66, RULE_lateralView = 67, RULE_setQuantifier =
      68, RULE_relation = 69, RULE_joinRelation = 70, RULE_joinType = 71, RULE_joinCriteria = 72,
      RULE_sample = 73, RULE_sampleMethod = 74, RULE_identifierList = 75, RULE_identifierSeq = 76,
      RULE_orderedIdentifierList = 77, RULE_orderedIdentifier = 78, RULE_identifierCommentList = 79,
      RULE_identifierComment = 80, RULE_relationPrimary = 81, RULE_inlineTable = 82,
      RULE_functionTable = 83, RULE_tableAlias = 84, RULE_rowFormat = 85,
      RULE_multipartIdentifierList = 86, RULE_multipartIdentifier = 87, RULE_tableIdentifier = 88,
      RULE_functionIdentifier = 89, RULE_namedExpression = 90, RULE_namedExpressionSeq = 91,
      RULE_transformList = 92, RULE_transform = 93, RULE_transformArgument = 94, RULE_expression =
      95, RULE_booleanExpression = 96, RULE_predicate = 97, RULE_valueExpression = 98,
      RULE_primaryExpression = 99, RULE_constant = 100, RULE_comparisonOperator = 101,
      RULE_arithmeticOperator = 102, RULE_predicateOperator = 103, RULE_booleanValue = 104,
      RULE_interval = 105, RULE_errorCapturingMultiUnitsInterval = 106, RULE_multiUnitsInterval =
      107, RULE_errorCapturingUnitToUnitInterval = 108, RULE_unitToUnitInterval = 109,
      RULE_intervalValue = 110, RULE_colPosition = 111, RULE_dataType = 112,
      RULE_qualifiedColTypeWithPositionList = 113, RULE_qualifiedColTypeWithPosition = 114,
      RULE_colTypeList = 115, RULE_colType = 116, RULE_complexColTypeList = 117,
      RULE_complexColType = 118, RULE_whenClause = 119, RULE_windowClause = 120, RULE_namedWindow =
      121, RULE_windowSpec = 122, RULE_windowFrame = 123, RULE_frameBound = 124,
      RULE_qualifiedNameList = 125, RULE_functionName = 126, RULE_qualifiedName = 127,
      RULE_errorCapturingIdentifier = 128, RULE_errorCapturingIdentifierExtra = 129,
      RULE_identifier = 130, RULE_strictIdentifier = 131, RULE_quotedIdentifier = 132, RULE_number =
      133, RULE_alterColumnAction = 134, RULE_ansiNonReserved = 135, RULE_strictNonReserved = 136,
      RULE_nonReserved = 137;

  private static String[] makeRuleNames() {
    return new String[] { "singleStatement", "singleExpression", "singleTableIdentifier",
        "singleMultipartIdentifier", "singleFunctionIdentifier", "singleDataType",
        "singleTableSchema", "statement", "configKey", "unsupportedHiveNativeCommands",
        "createTableHeader", "replaceTableHeader", "bucketSpec", "skewSpec", "locationSpec",
        "commentSpec", "query", "insertInto", "partitionSpecLocation", "partitionSpec",
        "partitionVal", "namespace", "describeFuncName", "describeColName", "ctes", "namedQuery",
        "tableProvider", "createTableClauses", "tablePropertyList", "tableProperty",
        "tablePropertyKey", "tablePropertyValue", "constantList", "nestedConstantList",
        "createFileFormat", "fileFormat", "storageHandler", "resource", "dmlStatementNoWith",
        "mergeInto", "queryOrganization", "multiInsertQueryBody", "queryTerm", "queryPrimary",
        "sortItem", "fromStatement", "fromStatementBody", "querySpecification", "transformClause",
        "selectClause", "setClause", "matchedClause", "notMatchedClause", "matchedAction",
        "notMatchedAction", "assignmentList", "assignment", "whereClause", "havingClause", "hint",
        "hintStatement", "fromClause", "aggregationClause", "groupingSet", "pivotClause",
        "pivotColumn", "pivotValue", "lateralView", "setQuantifier", "relation", "joinRelation",
        "joinType", "joinCriteria", "sample", "sampleMethod", "identifierList", "identifierSeq",
        "orderedIdentifierList", "orderedIdentifier", "identifierCommentList", "identifierComment",
        "relationPrimary", "inlineTable", "functionTable", "tableAlias", "rowFormat",
        "multipartIdentifierList", "multipartIdentifier", "tableIdentifier", "functionIdentifier",
        "namedExpression", "namedExpressionSeq", "transformList", "transform", "transformArgument",
        "expression", "booleanExpression", "predicate", "valueExpression", "primaryExpression",
        "constant", "comparisonOperator", "arithmeticOperator", "predicateOperator", "booleanValue",
        "interval", "errorCapturingMultiUnitsInterval", "multiUnitsInterval",
        "errorCapturingUnitToUnitInterval", "unitToUnitInterval", "intervalValue", "colPosition",
        "dataType", "qualifiedColTypeWithPositionList", "qualifiedColTypeWithPosition",
        "colTypeList", "colType", "complexColTypeList", "complexColType", "whenClause",
        "windowClause", "namedWindow", "windowSpec", "windowFrame", "frameBound",
        "qualifiedNameList", "functionName", "qualifiedName", "errorCapturingIdentifier",
        "errorCapturingIdentifierExtra", "identifier", "strictIdentifier", "quotedIdentifier",
        "number", "alterColumnAction", "ansiNonReserved", "strictNonReserved", "nonReserved" };
  }

  public static final String[] ruleNames = makeRuleNames();

  private static String[] makeLiteralNames() {
    return new String[] { null, "';'", "'('", "')'", "','", "'.'", "'/*+'", "'*/'", "'->'", "'['",
        "']'", "':'", "'ADD'", "'AFTER'", "'ALL'", "'ALTER'", "'ANALYZE'", "'AND'", "'ANTI'",
        "'ANY'", "'ARCHIVE'", "'ARRAY'", "'AS'", "'ASC'", "'AT'", "'AUTHORIZATION'", "'BETWEEN'",
        "'BOTH'", "'BUCKET'", "'BUCKETS'", "'BY'", "'CACHE'", "'CASCADE'", "'CASE'", "'CAST'",
        "'CHANGE'", "'CHECK'", "'CLEAR'", "'CLUSTER'", "'CLUSTERED'", "'CODEGEN'", "'COLLATE'",
        "'COLLECTION'", "'COLUMN'", "'COLUMNS'", "'COMMENT'", "'COMMIT'", "'COMPACT'",
        "'COMPACTIONS'", "'COMPUTE'", "'CONCATENATE'", "'CONSTRAINT'", "'COST'", "'CREATE'",
        "'CROSS'", "'CUBE'", "'CURRENT'", "'CURRENT_DATE'", "'CURRENT_TIME'", "'CURRENT_TIMESTAMP'",
        "'CURRENT_USER'", "'DATA'", "'DATABASE'", null, "'DBPROPERTIES'", "'DEFINED'", "'DELETE'",
        "'DELIMITED'", "'DESC'", "'DESCRIBE'", "'DFS'", "'DIRECTORIES'", "'DIRECTORY'",
        "'DISTINCT'", "'DISTRIBUTE'", "'DIV'", "'DROP'", "'ELSE'", "'END'", "'ESCAPE'", "'ESCAPED'",
        "'EXCEPT'", "'EXCHANGE'", "'EXISTS'", "'EXPLAIN'", "'EXPORT'", "'EXTENDED'", "'EXTERNAL'",
        "'EXTRACT'", "'FALSE'", "'FETCH'", "'FIELDS'", "'FILTER'", "'FILEFORMAT'", "'FIRST'",
        "'FOLLOWING'", "'FOR'", "'FOREIGN'", "'FORMAT'", "'FORMATTED'", "'FROM'", "'FULL'",
        "'FUNCTION'", "'FUNCTIONS'", "'GLOBAL'", "'GRANT'", "'GROUP'", "'GROUPING'", "'HAVING'",
        "'IF'", "'IGNORE'", "'IMPORT'", "'IN'", "'INDEX'", "'INDEXES'", "'INNER'", "'INPATH'",
        "'INPUTFORMAT'", "'INSERT'", "'INTERSECT'", "'INTERVAL'", "'INTO'", "'IS'", "'ITEMS'",
        "'JOIN'", "'KEYS'", "'LAST'", "'LATERAL'", "'LAZY'", "'LEADING'", "'LEFT'", "'LIKE'",
        "'LIMIT'", "'LINES'", "'LIST'", "'LOAD'", "'LOCAL'", "'LOCATION'", "'LOCK'", "'LOCKS'",
        "'LOGICAL'", "'MACRO'", "'MAP'", "'MATCHED'", "'MERGE'", "'MSCK'", "'NAMESPACE'",
        "'NAMESPACES'", "'NATURAL'", "'NO'", null, "'NULL'", "'NULLS'", "'OF'", "'ON'", "'ONLY'",
        "'OPTION'", "'OPTIONS'", "'OR'", "'ORDER'", "'OUT'", "'OUTER'", "'OUTPUTFORMAT'", "'OVER'",
        "'OVERLAPS'", "'OVERLAY'", "'OVERWRITE'", "'PARTITION'", "'PARTITIONED'", "'PARTITIONS'",
        "'PERCENT'", "'PIVOT'", "'PLACING'", "'POSITION'", "'PRECEDING'", "'PRIMARY'",
        "'PRINCIPALS'", "'PROPERTIES'", "'PURGE'", "'QUERY'", "'RANGE'", "'RECORDREADER'",
        "'RECORDWRITER'", "'RECOVER'", "'REDUCE'", "'REFERENCES'", "'REFRESH'", "'RENAME'",
        "'REPAIR'", "'REPLACE'", "'RESET'", "'RESTRICT'", "'REVOKE'", "'RIGHT'", null, "'ROLE'",
        "'ROLES'", "'ROLLBACK'", "'ROLLUP'", "'ROW'", "'ROWS'", "'SCHEMA'", "'SELECT'", "'SEMI'",
        "'SEPARATED'", "'SERDE'", "'SERDEPROPERTIES'", "'SESSION_USER'", "'SET'", "'MINUS'",
        "'SETS'", "'SHOW'", "'SKEWED'", "'SOME'", "'SORT'", "'SORTED'", "'START'", "'STATISTICS'",
        "'STORED'", "'STRATIFY'", "'STRUCT'", "'SUBSTR'", "'SUBSTRING'", "'TABLE'", "'TABLES'",
        "'TABLESAMPLE'", "'TBLPROPERTIES'", null, "'TERMINATED'", "'THEN'", "'TIME'", "'TO'",
        "'TOUCH'", "'TRAILING'", "'TRANSACTION'", "'TRANSACTIONS'", "'TRANSFORM'", "'TRIM'",
        "'TRUE'", "'TRUNCATE'", "'TYPE'", "'UNARCHIVE'", "'UNBOUNDED'", "'UNCACHE'", "'UNION'",
        "'UNIQUE'", "'UNKNOWN'", "'UNLOCK'", "'UNSET'", "'UPDATE'", "'USE'", "'USER'", "'USING'",
        "'VALUES'", "'VIEW'", "'VIEWS'", "'WHEN'", "'WHERE'", "'WINDOW'", "'WITH'", "'ZONE'", null,
        "'<=>'", "'<>'", "'!='", "'<'", null, "'>'", null, "'+'", "'-'", "'*'", "'/'", "'%'", "'~'",
        "'&'", "'|'", "'||'", "'^'" };
  }

  private static final String[] _LITERAL_NAMES = makeLiteralNames();

  private static String[] makeSymbolicNames() {
    return new String[] { null, null, null, null, null, null, null, null, null, null, null, null,
        "ADD", "AFTER", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", "ANY", "ARCHIVE", "ARRAY", "AS",
        "ASC", "AT", "AUTHORIZATION", "BETWEEN", "BOTH", "BUCKET", "BUCKETS", "BY", "CACHE",
        "CASCADE", "CASE", "CAST", "CHANGE", "CHECK", "CLEAR", "CLUSTER", "CLUSTERED", "CODEGEN",
        "COLLATE", "COLLECTION", "COLUMN", "COLUMNS", "COMMENT", "COMMIT", "COMPACT", "COMPACTIONS",
        "COMPUTE", "CONCATENATE", "CONSTRAINT", "COST", "CREATE", "CROSS", "CUBE", "CURRENT",
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "DATA", "DATABASE",
        "DATABASES", "DBPROPERTIES", "DEFINED", "DELETE", "DELIMITED", "DESC", "DESCRIBE", "DFS",
        "DIRECTORIES", "DIRECTORY", "DISTINCT", "DISTRIBUTE", "DIV", "DROP", "ELSE", "END",
        "ESCAPE", "ESCAPED", "EXCEPT", "EXCHANGE", "EXISTS", "EXPLAIN", "EXPORT", "EXTENDED",
        "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIELDS", "FILTER", "FILEFORMAT", "FIRST",
        "FOLLOWING", "FOR", "FOREIGN", "FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION",
        "FUNCTIONS", "GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "IF", "IGNORE", "IMPORT",
        "IN", "INDEX", "INDEXES", "INNER", "INPATH", "INPUTFORMAT", "INSERT", "INTERSECT",
        "INTERVAL", "INTO", "IS", "ITEMS", "JOIN", "KEYS", "LAST", "LATERAL", "LAZY", "LEADING",
        "LEFT", "LIKE", "LIMIT", "LINES", "LIST", "LOAD", "LOCAL", "LOCATION", "LOCK", "LOCKS",
        "LOGICAL", "MACRO", "MAP", "MATCHED", "MERGE", "MSCK", "NAMESPACE", "NAMESPACES", "NATURAL",
        "NO", "NOT", "NULL", "NULLS", "OF", "ON", "ONLY", "OPTION", "OPTIONS", "OR", "ORDER", "OUT",
        "OUTER", "OUTPUTFORMAT", "OVER", "OVERLAPS", "OVERLAY", "OVERWRITE", "PARTITION",
        "PARTITIONED", "PARTITIONS", "PERCENTLIT", "PIVOT", "PLACING", "POSITION", "PRECEDING",
        "PRIMARY", "PRINCIPALS", "PROPERTIES", "PURGE", "QUERY", "RANGE", "RECORDREADER",
        "RECORDWRITER", "RECOVER", "REDUCE", "REFERENCES", "REFRESH", "RENAME", "REPAIR", "REPLACE",
        "RESET", "RESTRICT", "REVOKE", "RIGHT", "RLIKE", "ROLE", "ROLES", "ROLLBACK", "ROLLUP",
        "ROW", "ROWS", "SCHEMA", "SELECT", "SEMI", "SEPARATED", "SERDE", "SERDEPROPERTIES",
        "SESSION_USER", "SET", "SETMINUS", "SETS", "SHOW", "SKEWED", "SOME", "SORT", "SORTED",
        "START", "STATISTICS", "STORED", "STRATIFY", "STRUCT", "SUBSTR", "SUBSTRING", "TABLE",
        "TABLES", "TABLESAMPLE", "TBLPROPERTIES", "TEMPORARY", "TERMINATED", "THEN", "TIME", "TO",
        "TOUCH", "TRAILING", "TRANSACTION", "TRANSACTIONS", "TRANSFORM", "TRIM", "TRUE", "TRUNCATE",
        "TYPE", "UNARCHIVE", "UNBOUNDED", "UNCACHE", "UNION", "UNIQUE", "UNKNOWN", "UNLOCK",
        "UNSET", "UPDATE", "USE", "USER", "USING", "VALUES", "VIEW", "VIEWS", "WHEN", "WHERE",
        "WINDOW", "WITH", "ZONE", "EQ", "NSEQ", "NEQ", "NEQJ", "LT", "LTE", "GT", "GTE", "PLUS",
        "MINUS", "ASTERISK", "SLASH", "PERCENT", "TILDE", "AMPERSAND", "PIPE", "CONCAT_PIPE", "HAT",
        "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE",
        "EXPONENT_VALUE", "DECIMAL_VALUE", "FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL",
        "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS",
        "UNRECOGNIZED" };
  }

  private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
  public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

  /**
   * @deprecated Use {@link #VOCABULARY} instead.
   */
  @Deprecated public static final String[] tokenNames;

  static {
    tokenNames = new String[_SYMBOLIC_NAMES.length];
    for (int i = 0; i < tokenNames.length; i++) {
      tokenNames[i] = VOCABULARY.getLiteralName(i);
      if (tokenNames[i] == null) {
        tokenNames[i] = VOCABULARY.getSymbolicName(i);
      }

      if (tokenNames[i] == null) {
        tokenNames[i] = "<INVALID>";
      }
    }
  }

  @Override
  @Deprecated
  public String[] getTokenNames() {
    return tokenNames;
  }

  @Override

  public Vocabulary getVocabulary() {
    return VOCABULARY;
  }

  @Override
  public String getGrammarFileName() {
    return "CarbonSqlBase.g4";
  }

  @Override
  public String[] getRuleNames() {
    return ruleNames;
  }

  @Override
  public String getSerializedATN() {
    return _serializedATN;
  }

  @Override
  public ATN getATN() {
    return _ATN;
  }

  /**
   * When false, INTERSECT is given the greater precedence over the other set
   * operations (UNION, EXCEPT and MINUS) as per the SQL standard.
   */
  public boolean legacy_setops_precedence_enbled = false;

  /**
   * When false, a literal with an exponent would be converted into
   * double type rather than decimal type.
   */
  public boolean legacy_exponent_literal_as_decimal_enabled = false;

  /**
   * When true, the behavior of keywords follows ANSI SQL standard.
   */
  public boolean SQL_standard_keyword_behavior = false;

  public CarbonSqlBaseParser(TokenStream input) {
    super(input);
    _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
  }

  public static class SingleStatementContext extends ParserRuleContext {
    public StatementContext statement() {
      return getRuleContext(StatementContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleStatement;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleStatementContext singleStatement() throws RecognitionException {
    SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
    enterRule(_localctx, 0, RULE_singleStatement);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(276);
        statement();
        setState(280);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__0) {
          {
            {
              setState(277);
              match(T__0);
            }
          }
          setState(282);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(283);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleExpressionContext extends ParserRuleContext {
    public NamedExpressionContext namedExpression() {
      return getRuleContext(NamedExpressionContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleExpression;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleExpressionContext singleExpression() throws RecognitionException {
    SingleExpressionContext _localctx = new SingleExpressionContext(_ctx, getState());
    enterRule(_localctx, 2, RULE_singleExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(285);
        namedExpression();
        setState(286);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleTableIdentifierContext extends ParserRuleContext {
    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleTableIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleTableIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleTableIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleTableIdentifierContext singleTableIdentifier() throws RecognitionException {
    SingleTableIdentifierContext _localctx = new SingleTableIdentifierContext(_ctx, getState());
    enterRule(_localctx, 4, RULE_singleTableIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(288);
        tableIdentifier();
        setState(289);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleMultipartIdentifierContext extends ParserRuleContext {
    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleMultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleMultipartIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleMultipartIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleMultipartIdentifierContext singleMultipartIdentifier()
      throws RecognitionException {
    SingleMultipartIdentifierContext _localctx =
        new SingleMultipartIdentifierContext(_ctx, getState());
    enterRule(_localctx, 6, RULE_singleMultipartIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(291);
        multipartIdentifier();
        setState(292);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleFunctionIdentifierContext extends ParserRuleContext {
    public FunctionIdentifierContext functionIdentifier() {
      return getRuleContext(FunctionIdentifierContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleFunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleFunctionIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleFunctionIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleFunctionIdentifierContext singleFunctionIdentifier()
      throws RecognitionException {
    SingleFunctionIdentifierContext _localctx =
        new SingleFunctionIdentifierContext(_ctx, getState());
    enterRule(_localctx, 8, RULE_singleFunctionIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(294);
        functionIdentifier();
        setState(295);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleDataTypeContext extends ParserRuleContext {
    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleDataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleDataType;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleDataTypeContext singleDataType() throws RecognitionException {
    SingleDataTypeContext _localctx = new SingleDataTypeContext(_ctx, getState());
    enterRule(_localctx, 10, RULE_singleDataType);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(297);
        dataType();
        setState(298);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SingleTableSchemaContext extends ParserRuleContext {
    public ColTypeListContext colTypeList() {
      return getRuleContext(ColTypeListContext.class, 0);
    }

    public TerminalNode EOF() {
      return getToken(CarbonSqlBaseParser.EOF, 0);
    }

    public SingleTableSchemaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_singleTableSchema;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleTableSchema(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SingleTableSchemaContext singleTableSchema() throws RecognitionException {
    SingleTableSchemaContext _localctx = new SingleTableSchemaContext(_ctx, getState());
    enterRule(_localctx, 12, RULE_singleTableSchema);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(300);
        colTypeList();
        setState(301);
        match(EOF);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StatementContext extends ParserRuleContext {
    public StatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_statement;
    }

    public StatementContext() {
    }

    public void copyFrom(StatementContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ExplainContext extends StatementContext {
    public TerminalNode EXPLAIN() {
      return getToken(CarbonSqlBaseParser.EXPLAIN, 0);
    }

    public StatementContext statement() {
      return getRuleContext(StatementContext.class, 0);
    }

    public TerminalNode LOGICAL() {
      return getToken(CarbonSqlBaseParser.LOGICAL, 0);
    }

    public TerminalNode FORMATTED() {
      return getToken(CarbonSqlBaseParser.FORMATTED, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public TerminalNode CODEGEN() {
      return getToken(CarbonSqlBaseParser.CODEGEN, 0);
    }

    public TerminalNode COST() {
      return getToken(CarbonSqlBaseParser.COST, 0);
    }

    public ExplainContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitExplain(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ResetConfigurationContext extends StatementContext {
    public TerminalNode RESET() {
      return getToken(CarbonSqlBaseParser.RESET, 0);
    }

    public ResetConfigurationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitResetConfiguration(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AlterViewQueryContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public AlterViewQueryContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAlterViewQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UseContext extends StatementContext {
    public TerminalNode USE() {
      return getToken(CarbonSqlBaseParser.USE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode NAMESPACE() {
      return getToken(CarbonSqlBaseParser.NAMESPACE, 0);
    }

    public UseContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUse(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropNamespaceContext extends StatementContext {
    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode RESTRICT() {
      return getToken(CarbonSqlBaseParser.RESTRICT, 0);
    }

    public TerminalNode CASCADE() {
      return getToken(CarbonSqlBaseParser.CASCADE, 0);
    }

    public DropNamespaceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateTempViewUsingContext extends StatementContext {
    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class, 0);
    }

    public TableProviderContext tableProvider() {
      return getRuleContext(TableProviderContext.class, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode GLOBAL() {
      return getToken(CarbonSqlBaseParser.GLOBAL, 0);
    }

    public ColTypeListContext colTypeList() {
      return getRuleContext(ColTypeListContext.class, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(CarbonSqlBaseParser.OPTIONS, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public CreateTempViewUsingContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateTempViewUsing(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RenameTableContext extends StatementContext {
    public MultipartIdentifierContext from;
    public MultipartIdentifierContext to;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode RENAME() {
      return getToken(CarbonSqlBaseParser.RENAME, 0);
    }

    public TerminalNode TO() {
      return getToken(CarbonSqlBaseParser.TO, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public RenameTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRenameTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FailNativeCommandContext extends StatementContext {
    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode ROLE() {
      return getToken(CarbonSqlBaseParser.ROLE, 0);
    }

    public UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands() {
      return getRuleContext(UnsupportedHiveNativeCommandsContext.class, 0);
    }

    public FailNativeCommandContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFailNativeCommand(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ClearCacheContext extends StatementContext {
    public TerminalNode CLEAR() {
      return getToken(CarbonSqlBaseParser.CLEAR, 0);
    }

    public TerminalNode CACHE() {
      return getToken(CarbonSqlBaseParser.CACHE, 0);
    }

    public ClearCacheContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitClearCache(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropViewContext extends StatementContext {
    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public DropViewContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropView(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowTablesContext extends StatementContext {
    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode TABLES() {
      return getToken(CarbonSqlBaseParser.TABLES, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public ShowTablesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowTables(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RecoverPartitionsContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode RECOVER() {
      return getToken(CarbonSqlBaseParser.RECOVER, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(CarbonSqlBaseParser.PARTITIONS, 0);
    }

    public RecoverPartitionsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRecoverPartitions(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowCurrentNamespaceContext extends StatementContext {
    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(CarbonSqlBaseParser.CURRENT, 0);
    }

    public TerminalNode NAMESPACE() {
      return getToken(CarbonSqlBaseParser.NAMESPACE, 0);
    }

    public ShowCurrentNamespaceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowCurrentNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RenameTablePartitionContext extends StatementContext {
    public PartitionSpecContext from;
    public PartitionSpecContext to;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode RENAME() {
      return getToken(CarbonSqlBaseParser.RENAME, 0);
    }

    public TerminalNode TO() {
      return getToken(CarbonSqlBaseParser.TO, 0);
    }

    public List<PartitionSpecContext> partitionSpec() {
      return getRuleContexts(PartitionSpecContext.class);
    }

    public PartitionSpecContext partitionSpec(int i) {
      return getRuleContext(PartitionSpecContext.class, i);
    }

    public RenameTablePartitionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRenameTablePartition(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RepairTableContext extends StatementContext {
    public TerminalNode MSCK() {
      return getToken(CarbonSqlBaseParser.MSCK, 0);
    }

    public TerminalNode REPAIR() {
      return getToken(CarbonSqlBaseParser.REPAIR, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public RepairTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRepairTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RefreshResourceContext extends StatementContext {
    public TerminalNode REFRESH() {
      return getToken(CarbonSqlBaseParser.REFRESH, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public RefreshResourceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRefreshResource(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowCreateTableContext extends StatementContext {
    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode SERDE() {
      return getToken(CarbonSqlBaseParser.SERDE, 0);
    }

    public ShowCreateTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowCreateTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowNamespacesContext extends StatementContext {
    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode DATABASES() {
      return getToken(CarbonSqlBaseParser.DATABASES, 0);
    }

    public TerminalNode NAMESPACES() {
      return getToken(CarbonSqlBaseParser.NAMESPACES, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public ShowNamespacesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowNamespaces(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowColumnsContext extends StatementContext {
    public MultipartIdentifierContext table;
    public MultipartIdentifierContext ns;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public List<TerminalNode> FROM() {
      return getTokens(CarbonSqlBaseParser.FROM);
    }

    public TerminalNode FROM(int i) {
      return getToken(CarbonSqlBaseParser.FROM, i);
    }

    public List<TerminalNode> IN() {
      return getTokens(CarbonSqlBaseParser.IN);
    }

    public TerminalNode IN(int i) {
      return getToken(CarbonSqlBaseParser.IN, i);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public ShowColumnsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowColumns(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ReplaceTableContext extends StatementContext {
    public ReplaceTableHeaderContext replaceTableHeader() {
      return getRuleContext(ReplaceTableHeaderContext.class, 0);
    }

    public TableProviderContext tableProvider() {
      return getRuleContext(TableProviderContext.class, 0);
    }

    public CreateTableClausesContext createTableClauses() {
      return getRuleContext(CreateTableClausesContext.class, 0);
    }

    public ColTypeListContext colTypeList() {
      return getRuleContext(ColTypeListContext.class, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public ReplaceTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitReplaceTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AddTablePartitionContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode ADD() {
      return getToken(CarbonSqlBaseParser.ADD, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public List<PartitionSpecLocationContext> partitionSpecLocation() {
      return getRuleContexts(PartitionSpecLocationContext.class);
    }

    public PartitionSpecLocationContext partitionSpecLocation(int i) {
      return getRuleContext(PartitionSpecLocationContext.class, i);
    }

    public AddTablePartitionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAddTablePartition(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetNamespaceLocationContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public LocationSpecContext locationSpec() {
      return getRuleContext(LocationSpecContext.class, 0);
    }

    public SetNamespaceLocationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetNamespaceLocation(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RefreshTableContext extends StatementContext {
    public TerminalNode REFRESH() {
      return getToken(CarbonSqlBaseParser.REFRESH, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public RefreshTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRefreshTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetNamespacePropertiesContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public TerminalNode DBPROPERTIES() {
      return getToken(CarbonSqlBaseParser.DBPROPERTIES, 0);
    }

    public TerminalNode PROPERTIES() {
      return getToken(CarbonSqlBaseParser.PROPERTIES, 0);
    }

    public SetNamespacePropertiesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetNamespaceProperties(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ManageResourceContext extends StatementContext {
    public Token op;

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode ADD() {
      return getToken(CarbonSqlBaseParser.ADD, 0);
    }

    public TerminalNode LIST() {
      return getToken(CarbonSqlBaseParser.LIST, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public ManageResourceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitManageResource(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetQuotedConfigurationContext extends StatementContext {
    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public ConfigKeyContext configKey() {
      return getRuleContext(ConfigKeyContext.class, 0);
    }

    public TerminalNode EQ() {
      return getToken(CarbonSqlBaseParser.EQ, 0);
    }

    public SetQuotedConfigurationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetQuotedConfiguration(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AnalyzeContext extends StatementContext {
    public TerminalNode ANALYZE() {
      return getToken(CarbonSqlBaseParser.ANALYZE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode COMPUTE() {
      return getToken(CarbonSqlBaseParser.COMPUTE, 0);
    }

    public TerminalNode STATISTICS() {
      return getToken(CarbonSqlBaseParser.STATISTICS, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode FOR() {
      return getToken(CarbonSqlBaseParser.FOR, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public IdentifierSeqContext identifierSeq() {
      return getRuleContext(IdentifierSeqContext.class, 0);
    }

    public TerminalNode ALL() {
      return getToken(CarbonSqlBaseParser.ALL, 0);
    }

    public AnalyzeContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAnalyze(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateHiveTableContext extends StatementContext {
    public ColTypeListContext columns;
    public ColTypeListContext partitionColumns;
    public IdentifierListContext partitionColumnNames;
    public TablePropertyListContext tableProps;

    public CreateTableHeaderContext createTableHeader() {
      return getRuleContext(CreateTableHeaderContext.class, 0);
    }

    public List<CommentSpecContext> commentSpec() {
      return getRuleContexts(CommentSpecContext.class);
    }

    public CommentSpecContext commentSpec(int i) {
      return getRuleContext(CommentSpecContext.class, i);
    }

    public List<BucketSpecContext> bucketSpec() {
      return getRuleContexts(BucketSpecContext.class);
    }

    public BucketSpecContext bucketSpec(int i) {
      return getRuleContext(BucketSpecContext.class, i);
    }

    public List<SkewSpecContext> skewSpec() {
      return getRuleContexts(SkewSpecContext.class);
    }

    public SkewSpecContext skewSpec(int i) {
      return getRuleContext(SkewSpecContext.class, i);
    }

    public List<RowFormatContext> rowFormat() {
      return getRuleContexts(RowFormatContext.class);
    }

    public RowFormatContext rowFormat(int i) {
      return getRuleContext(RowFormatContext.class, i);
    }

    public List<CreateFileFormatContext> createFileFormat() {
      return getRuleContexts(CreateFileFormatContext.class);
    }

    public CreateFileFormatContext createFileFormat(int i) {
      return getRuleContext(CreateFileFormatContext.class, i);
    }

    public List<LocationSpecContext> locationSpec() {
      return getRuleContexts(LocationSpecContext.class);
    }

    public LocationSpecContext locationSpec(int i) {
      return getRuleContext(LocationSpecContext.class, i);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public List<ColTypeListContext> colTypeList() {
      return getRuleContexts(ColTypeListContext.class);
    }

    public ColTypeListContext colTypeList(int i) {
      return getRuleContext(ColTypeListContext.class, i);
    }

    public List<TerminalNode> PARTITIONED() {
      return getTokens(CarbonSqlBaseParser.PARTITIONED);
    }

    public TerminalNode PARTITIONED(int i) {
      return getToken(CarbonSqlBaseParser.PARTITIONED, i);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public List<TerminalNode> TBLPROPERTIES() {
      return getTokens(CarbonSqlBaseParser.TBLPROPERTIES);
    }

    public TerminalNode TBLPROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, i);
    }

    public List<IdentifierListContext> identifierList() {
      return getRuleContexts(IdentifierListContext.class);
    }

    public IdentifierListContext identifierList(int i) {
      return getRuleContext(IdentifierListContext.class, i);
    }

    public List<TablePropertyListContext> tablePropertyList() {
      return getRuleContexts(TablePropertyListContext.class);
    }

    public TablePropertyListContext tablePropertyList(int i) {
      return getRuleContext(TablePropertyListContext.class, i);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public CreateHiveTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateHiveTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateFunctionContext extends StatementContext {
    public Token className;

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public List<ResourceContext> resource() {
      return getRuleContexts(ResourceContext.class);
    }

    public ResourceContext resource(int i) {
      return getRuleContext(ResourceContext.class, i);
    }

    public CreateFunctionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowTableContext extends StatementContext {
    public MultipartIdentifierContext ns;
    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public ShowTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class HiveReplaceColumnsContext extends StatementContext {
    public MultipartIdentifierContext table;
    public QualifiedColTypeWithPositionListContext columns;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
      return getRuleContext(QualifiedColTypeWithPositionListContext.class, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public HiveReplaceColumnsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitHiveReplaceColumns(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CommentNamespaceContext extends StatementContext {
    public Token comment;

    public TerminalNode COMMENT() {
      return getToken(CarbonSqlBaseParser.COMMENT, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IS() {
      return getToken(CarbonSqlBaseParser.IS, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public CommentNamespaceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCommentNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ResetQuotedConfigurationContext extends StatementContext {
    public TerminalNode RESET() {
      return getToken(CarbonSqlBaseParser.RESET, 0);
    }

    public ConfigKeyContext configKey() {
      return getRuleContext(ConfigKeyContext.class, 0);
    }

    public ResetQuotedConfigurationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitResetQuotedConfiguration(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateTableContext extends StatementContext {
    public CreateTableHeaderContext createTableHeader() {
      return getRuleContext(CreateTableHeaderContext.class, 0);
    }

    public TableProviderContext tableProvider() {
      return getRuleContext(TableProviderContext.class, 0);
    }

    public CreateTableClausesContext createTableClauses() {
      return getRuleContext(CreateTableClausesContext.class, 0);
    }

    public ColTypeListContext colTypeList() {
      return getRuleContext(ColTypeListContext.class, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public CreateTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DmlStatementContext extends StatementContext {
    public DmlStatementNoWithContext dmlStatementNoWith() {
      return getRuleContext(DmlStatementNoWithContext.class, 0);
    }

    public CtesContext ctes() {
      return getRuleContext(CtesContext.class, 0);
    }

    public DmlStatementContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDmlStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateTableLikeContext extends StatementContext {
    public TableIdentifierContext target;
    public TableIdentifierContext source;
    public TablePropertyListContext tableProps;

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public List<TableIdentifierContext> tableIdentifier() {
      return getRuleContexts(TableIdentifierContext.class);
    }

    public TableIdentifierContext tableIdentifier(int i) {
      return getRuleContext(TableIdentifierContext.class, i);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public List<TableProviderContext> tableProvider() {
      return getRuleContexts(TableProviderContext.class);
    }

    public TableProviderContext tableProvider(int i) {
      return getRuleContext(TableProviderContext.class, i);
    }

    public List<RowFormatContext> rowFormat() {
      return getRuleContexts(RowFormatContext.class);
    }

    public RowFormatContext rowFormat(int i) {
      return getRuleContext(RowFormatContext.class, i);
    }

    public List<CreateFileFormatContext> createFileFormat() {
      return getRuleContexts(CreateFileFormatContext.class);
    }

    public CreateFileFormatContext createFileFormat(int i) {
      return getRuleContext(CreateFileFormatContext.class, i);
    }

    public List<LocationSpecContext> locationSpec() {
      return getRuleContexts(LocationSpecContext.class);
    }

    public LocationSpecContext locationSpec(int i) {
      return getRuleContext(LocationSpecContext.class, i);
    }

    public List<TerminalNode> TBLPROPERTIES() {
      return getTokens(CarbonSqlBaseParser.TBLPROPERTIES);
    }

    public TerminalNode TBLPROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, i);
    }

    public List<TablePropertyListContext> tablePropertyList() {
      return getRuleContexts(TablePropertyListContext.class);
    }

    public TablePropertyListContext tablePropertyList(int i) {
      return getRuleContext(TablePropertyListContext.class, i);
    }

    public CreateTableLikeContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateTableLike(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UncacheTableContext extends StatementContext {
    public TerminalNode UNCACHE() {
      return getToken(CarbonSqlBaseParser.UNCACHE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public UncacheTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUncacheTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropFunctionContext extends StatementContext {
    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public DropFunctionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DescribeRelationContext extends StatementContext {
    public Token option;

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public DescribeColNameContext describeColName() {
      return getRuleContext(DescribeColNameContext.class, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public TerminalNode FORMATTED() {
      return getToken(CarbonSqlBaseParser.FORMATTED, 0);
    }

    public DescribeRelationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LoadDataContext extends StatementContext {
    public Token path;

    public TerminalNode LOAD() {
      return getToken(CarbonSqlBaseParser.LOAD, 0);
    }

    public TerminalNode DATA() {
      return getToken(CarbonSqlBaseParser.DATA, 0);
    }

    public TerminalNode INPATH() {
      return getToken(CarbonSqlBaseParser.INPATH, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public LoadDataContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLoadData(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowPartitionsContext extends StatementContext {
    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(CarbonSqlBaseParser.PARTITIONS, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public ShowPartitionsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowPartitions(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DescribeFunctionContext extends StatementContext {
    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public DescribeFuncNameContext describeFuncName() {
      return getRuleContext(DescribeFuncNameContext.class, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public DescribeFunctionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RenameTableColumnContext extends StatementContext {
    public MultipartIdentifierContext table;
    public MultipartIdentifierContext from;
    public ErrorCapturingIdentifierContext to;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode RENAME() {
      return getToken(CarbonSqlBaseParser.RENAME, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public TerminalNode TO() {
      return getToken(CarbonSqlBaseParser.TO, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public RenameTableColumnContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRenameTableColumn(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class StatementDefaultContext extends StatementContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public StatementDefaultContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStatementDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class HiveChangeColumnContext extends StatementContext {
    public MultipartIdentifierContext table;
    public MultipartIdentifierContext colName;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode CHANGE() {
      return getToken(CarbonSqlBaseParser.CHANGE, 0);
    }

    public ColTypeContext colType() {
      return getRuleContext(ColTypeContext.class, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public ColPositionContext colPosition() {
      return getRuleContext(ColPositionContext.class, 0);
    }

    public HiveChangeColumnContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitHiveChangeColumn(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetTimeZoneContext extends StatementContext {
    public Token timezone;

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode TIME() {
      return getToken(CarbonSqlBaseParser.TIME, 0);
    }

    public TerminalNode ZONE() {
      return getToken(CarbonSqlBaseParser.ZONE, 0);
    }

    public IntervalContext interval() {
      return getRuleContext(IntervalContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public SetTimeZoneContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetTimeZone(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DescribeQueryContext extends StatementContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode QUERY() {
      return getToken(CarbonSqlBaseParser.QUERY, 0);
    }

    public DescribeQueryContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TruncateTableContext extends StatementContext {
    public TerminalNode TRUNCATE() {
      return getToken(CarbonSqlBaseParser.TRUNCATE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TruncateTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTruncateTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetTableSerDeContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode SERDE() {
      return getToken(CarbonSqlBaseParser.SERDE, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public TerminalNode SERDEPROPERTIES() {
      return getToken(CarbonSqlBaseParser.SERDEPROPERTIES, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public SetTableSerDeContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetTableSerDe(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateViewContext extends StatementContext {
    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public IdentifierCommentListContext identifierCommentList() {
      return getRuleContext(IdentifierCommentListContext.class, 0);
    }

    public List<CommentSpecContext> commentSpec() {
      return getRuleContexts(CommentSpecContext.class);
    }

    public CommentSpecContext commentSpec(int i) {
      return getRuleContext(CommentSpecContext.class, i);
    }

    public List<TerminalNode> PARTITIONED() {
      return getTokens(CarbonSqlBaseParser.PARTITIONED);
    }

    public TerminalNode PARTITIONED(int i) {
      return getToken(CarbonSqlBaseParser.PARTITIONED, i);
    }

    public List<TerminalNode> ON() {
      return getTokens(CarbonSqlBaseParser.ON);
    }

    public TerminalNode ON(int i) {
      return getToken(CarbonSqlBaseParser.ON, i);
    }

    public List<IdentifierListContext> identifierList() {
      return getRuleContexts(IdentifierListContext.class);
    }

    public IdentifierListContext identifierList(int i) {
      return getRuleContext(IdentifierListContext.class, i);
    }

    public List<TerminalNode> TBLPROPERTIES() {
      return getTokens(CarbonSqlBaseParser.TBLPROPERTIES);
    }

    public TerminalNode TBLPROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, i);
    }

    public List<TablePropertyListContext> tablePropertyList() {
      return getRuleContexts(TablePropertyListContext.class);
    }

    public TablePropertyListContext tablePropertyList(int i) {
      return getRuleContext(TablePropertyListContext.class, i);
    }

    public TerminalNode GLOBAL() {
      return getToken(CarbonSqlBaseParser.GLOBAL, 0);
    }

    public CreateViewContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateView(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropTablePartitionsContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public List<PartitionSpecContext> partitionSpec() {
      return getRuleContexts(PartitionSpecContext.class);
    }

    public PartitionSpecContext partitionSpec(int i) {
      return getRuleContext(PartitionSpecContext.class, i);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode PURGE() {
      return getToken(CarbonSqlBaseParser.PURGE, 0);
    }

    public DropTablePartitionsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropTablePartitions(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetConfigurationContext extends StatementContext {
    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public SetConfigurationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetConfiguration(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropTableContext extends StatementContext {
    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode PURGE() {
      return getToken(CarbonSqlBaseParser.PURGE, 0);
    }

    public DropTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DescribeNamespaceContext extends StatementContext {
    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public DescribeNamespaceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AlterTableAlterColumnContext extends StatementContext {
    public MultipartIdentifierContext table;
    public MultipartIdentifierContext column;

    public List<TerminalNode> ALTER() {
      return getTokens(CarbonSqlBaseParser.ALTER);
    }

    public TerminalNode ALTER(int i) {
      return getToken(CarbonSqlBaseParser.ALTER, i);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public TerminalNode CHANGE() {
      return getToken(CarbonSqlBaseParser.CHANGE, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public AlterColumnActionContext alterColumnAction() {
      return getRuleContext(AlterColumnActionContext.class, 0);
    }

    public AlterTableAlterColumnContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAlterTableAlterColumn(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RefreshFunctionContext extends StatementContext {
    public TerminalNode REFRESH() {
      return getToken(CarbonSqlBaseParser.REFRESH, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public RefreshFunctionContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRefreshFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CommentTableContext extends StatementContext {
    public Token comment;

    public TerminalNode COMMENT() {
      return getToken(CarbonSqlBaseParser.COMMENT, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IS() {
      return getToken(CarbonSqlBaseParser.IS, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public CommentTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCommentTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CreateNamespaceContext extends StatementContext {
    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public NamespaceContext namespace() {
      return getRuleContext(NamespaceContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public List<CommentSpecContext> commentSpec() {
      return getRuleContexts(CommentSpecContext.class);
    }

    public CommentSpecContext commentSpec(int i) {
      return getRuleContext(CommentSpecContext.class, i);
    }

    public List<LocationSpecContext> locationSpec() {
      return getRuleContexts(LocationSpecContext.class);
    }

    public LocationSpecContext locationSpec(int i) {
      return getRuleContext(LocationSpecContext.class, i);
    }

    public List<TerminalNode> WITH() {
      return getTokens(CarbonSqlBaseParser.WITH);
    }

    public TerminalNode WITH(int i) {
      return getToken(CarbonSqlBaseParser.WITH, i);
    }

    public List<TablePropertyListContext> tablePropertyList() {
      return getRuleContexts(TablePropertyListContext.class);
    }

    public TablePropertyListContext tablePropertyList(int i) {
      return getRuleContext(TablePropertyListContext.class, i);
    }

    public List<TerminalNode> DBPROPERTIES() {
      return getTokens(CarbonSqlBaseParser.DBPROPERTIES);
    }

    public TerminalNode DBPROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.DBPROPERTIES, i);
    }

    public List<TerminalNode> PROPERTIES() {
      return getTokens(CarbonSqlBaseParser.PROPERTIES);
    }

    public TerminalNode PROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.PROPERTIES, i);
    }

    public CreateNamespaceContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowTblPropertiesContext extends StatementContext {
    public MultipartIdentifierContext table;
    public TablePropertyKeyContext key;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode TBLPROPERTIES() {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TablePropertyKeyContext tablePropertyKey() {
      return getRuleContext(TablePropertyKeyContext.class, 0);
    }

    public ShowTblPropertiesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowTblProperties(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UnsetTablePropertiesContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode UNSET() {
      return getToken(CarbonSqlBaseParser.UNSET, 0);
    }

    public TerminalNode TBLPROPERTIES() {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public UnsetTablePropertiesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUnsetTableProperties(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetTableLocationContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public LocationSpecContext locationSpec() {
      return getRuleContext(LocationSpecContext.class, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public SetTableLocationContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetTableLocation(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DropTableColumnsContext extends StatementContext {
    public MultipartIdentifierListContext columns;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public MultipartIdentifierListContext multipartIdentifierList() {
      return getRuleContext(MultipartIdentifierListContext.class, 0);
    }

    public DropTableColumnsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDropTableColumns(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowViewsContext extends StatementContext {
    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode VIEWS() {
      return getToken(CarbonSqlBaseParser.VIEWS, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public ShowViewsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowViews(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ShowFunctionsContext extends StatementContext {
    public Token pattern;

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode FUNCTIONS() {
      return getToken(CarbonSqlBaseParser.FUNCTIONS, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public ShowFunctionsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitShowFunctions(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CacheTableContext extends StatementContext {
    public TablePropertyListContext options;

    public TerminalNode CACHE() {
      return getToken(CarbonSqlBaseParser.CACHE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode LAZY() {
      return getToken(CarbonSqlBaseParser.LAZY, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(CarbonSqlBaseParser.OPTIONS, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public CacheTableContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCacheTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AddTableColumnsContext extends StatementContext {
    public QualifiedColTypeWithPositionListContext columns;

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode ADD() {
      return getToken(CarbonSqlBaseParser.ADD, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList() {
      return getRuleContext(QualifiedColTypeWithPositionListContext.class, 0);
    }

    public AddTableColumnsContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAddTableColumns(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetTablePropertiesContext extends StatementContext {
    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode TBLPROPERTIES() {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public SetTablePropertiesContext(StatementContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetTableProperties(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StatementContext statement() throws RecognitionException {
    StatementContext _localctx = new StatementContext(_ctx, getState());
    enterRule(_localctx, 14, RULE_statement);
    int _la;
    try {
      int _alt;
      setState(1047);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 110, _ctx)) {
        case 1:
          _localctx = new StatementDefaultContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(303);
          query();
        }
        break;
        case 2:
          _localctx = new DmlStatementContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(305);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == WITH) {
            {
              setState(304);
              ctes();
            }
          }

          setState(307);
          dmlStatementNoWith();
        }
        break;
        case 3:
          _localctx = new UseContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(308);
          match(USE);
          setState(310);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 2, _ctx)) {
            case 1: {
              setState(309);
              match(NAMESPACE);
            }
            break;
          }
          setState(312);
          multipartIdentifier();
        }
        break;
        case 4:
          _localctx = new CreateNamespaceContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(313);
          match(CREATE);
          setState(314);
          namespace();
          setState(318);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 3, _ctx)) {
            case 1: {
              setState(315);
              match(IF);
              setState(316);
              match(NOT);
              setState(317);
              match(EXISTS);
            }
            break;
          }
          setState(320);
          multipartIdentifier();
          setState(328);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == COMMENT || _la == LOCATION || _la == WITH) {
            {
              setState(326);
              _errHandler.sync(this);
              switch (_input.LA(1)) {
                case COMMENT: {
                  setState(321);
                  commentSpec();
                }
                break;
                case LOCATION: {
                  setState(322);
                  locationSpec();
                }
                break;
                case WITH: {
                  {
                    setState(323);
                    match(WITH);
                    setState(324);
                    _la = _input.LA(1);
                    if (!(_la == DBPROPERTIES || _la == PROPERTIES)) {
                      _errHandler.recoverInline(this);
                    } else {
                      if (_input.LA(1) == Token.EOF) matchedEOF = true;
                      _errHandler.reportMatch(this);
                      consume();
                    }
                    setState(325);
                    tablePropertyList();
                  }
                }
                break;
                default:
                  throw new NoViableAltException(this);
              }
            }
            setState(330);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
        }
        break;
        case 5:
          _localctx = new SetNamespacePropertiesContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(331);
          match(ALTER);
          setState(332);
          namespace();
          setState(333);
          multipartIdentifier();
          setState(334);
          match(SET);
          setState(335);
          _la = _input.LA(1);
          if (!(_la == DBPROPERTIES || _la == PROPERTIES)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(336);
          tablePropertyList();
        }
        break;
        case 6:
          _localctx = new SetNamespaceLocationContext(_localctx);
          enterOuterAlt(_localctx, 6);
        {
          setState(338);
          match(ALTER);
          setState(339);
          namespace();
          setState(340);
          multipartIdentifier();
          setState(341);
          match(SET);
          setState(342);
          locationSpec();
        }
        break;
        case 7:
          _localctx = new DropNamespaceContext(_localctx);
          enterOuterAlt(_localctx, 7);
        {
          setState(344);
          match(DROP);
          setState(345);
          namespace();
          setState(348);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 6, _ctx)) {
            case 1: {
              setState(346);
              match(IF);
              setState(347);
              match(EXISTS);
            }
            break;
          }
          setState(350);
          multipartIdentifier();
          setState(352);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == CASCADE || _la == RESTRICT) {
            {
              setState(351);
              _la = _input.LA(1);
              if (!(_la == CASCADE || _la == RESTRICT)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
            }
          }

        }
        break;
        case 8:
          _localctx = new ShowNamespacesContext(_localctx);
          enterOuterAlt(_localctx, 8);
        {
          setState(354);
          match(SHOW);
          setState(355);
          _la = _input.LA(1);
          if (!(_la == DATABASES || _la == NAMESPACES)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(358);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(356);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(357);
              multipartIdentifier();
            }
          }

          setState(364);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LIKE || _la == STRING) {
            {
              setState(361);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == LIKE) {
                {
                  setState(360);
                  match(LIKE);
                }
              }

              setState(363);
              ((ShowNamespacesContext) _localctx).pattern = match(STRING);
            }
          }

        }
        break;
        case 9:
          _localctx = new CreateTableContext(_localctx);
          enterOuterAlt(_localctx, 9);
        {
          setState(366);
          createTableHeader();
          setState(371);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1) {
            {
              setState(367);
              match(T__1);
              setState(368);
              colTypeList();
              setState(369);
              match(T__2);
            }
          }

          setState(373);
          tableProvider();
          setState(374);
          createTableClauses();
          setState(379);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1 || _la == AS || _la == FROM || _la == MAP || ((((_la - 184)) & ~0x3f) == 0
              && ((1L << (_la - 184)) & ((1L << (REDUCE - 184)) | (1L << (SELECT - 184)) | (1L << (
              TABLE - 184)))) != 0) || _la == VALUES || _la == WITH) {
            {
              setState(376);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == AS) {
                {
                  setState(375);
                  match(AS);
                }
              }

              setState(378);
              query();
            }
          }

        }
        break;
        case 10:
          _localctx = new CreateHiveTableContext(_localctx);
          enterOuterAlt(_localctx, 10);
        {
          setState(381);
          createTableHeader();
          setState(386);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 14, _ctx)) {
            case 1: {
              setState(382);
              match(T__1);
              setState(383);
              ((CreateHiveTableContext) _localctx).columns = colTypeList();
              setState(384);
              match(T__2);
            }
            break;
          }
          setState(409);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == CLUSTERED || _la == COMMENT || ((((_la - 137)) & ~0x3f) == 0 &&
              ((1L << (_la - 137)) & ((1L << (LOCATION - 137)) | (1L << (PARTITIONED - 137)) | (1L
                  << (ROW - 137)))) != 0) || ((((_la - 212)) & ~0x3f) == 0 &&
              ((1L << (_la - 212)) & ((1L << (SKEWED - 212)) | (1L << (STORED - 212)) | (1L << (
                  TBLPROPERTIES - 212)))) != 0)) {
            {
              setState(407);
              _errHandler.sync(this);
              switch (_input.LA(1)) {
                case COMMENT: {
                  setState(388);
                  commentSpec();
                }
                break;
                case PARTITIONED: {
                  setState(398);
                  _errHandler.sync(this);
                  switch (getInterpreter().adaptivePredict(_input, 15, _ctx)) {
                    case 1: {
                      setState(389);
                      match(PARTITIONED);
                      setState(390);
                      match(BY);
                      setState(391);
                      match(T__1);
                      setState(392);
                      ((CreateHiveTableContext) _localctx).partitionColumns = colTypeList();
                      setState(393);
                      match(T__2);
                    }
                    break;
                    case 2: {
                      setState(395);
                      match(PARTITIONED);
                      setState(396);
                      match(BY);
                      setState(397);
                      ((CreateHiveTableContext) _localctx).partitionColumnNames = identifierList();
                    }
                    break;
                  }
                }
                break;
                case CLUSTERED: {
                  setState(400);
                  bucketSpec();
                }
                break;
                case SKEWED: {
                  setState(401);
                  skewSpec();
                }
                break;
                case ROW: {
                  setState(402);
                  rowFormat();
                }
                break;
                case STORED: {
                  setState(403);
                  createFileFormat();
                }
                break;
                case LOCATION: {
                  setState(404);
                  locationSpec();
                }
                break;
                case TBLPROPERTIES: {
                  {
                    setState(405);
                    match(TBLPROPERTIES);
                    setState(406);
                    ((CreateHiveTableContext) _localctx).tableProps = tablePropertyList();
                  }
                }
                break;
                default:
                  throw new NoViableAltException(this);
              }
            }
            setState(411);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(416);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1 || _la == AS || _la == FROM || _la == MAP || ((((_la - 184)) & ~0x3f) == 0
              && ((1L << (_la - 184)) & ((1L << (REDUCE - 184)) | (1L << (SELECT - 184)) | (1L << (
              TABLE - 184)))) != 0) || _la == VALUES || _la == WITH) {
            {
              setState(413);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == AS) {
                {
                  setState(412);
                  match(AS);
                }
              }

              setState(415);
              query();
            }
          }

        }
        break;
        case 11:
          _localctx = new CreateTableLikeContext(_localctx);
          enterOuterAlt(_localctx, 11);
        {
          setState(418);
          match(CREATE);
          setState(419);
          match(TABLE);
          setState(423);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 20, _ctx)) {
            case 1: {
              setState(420);
              match(IF);
              setState(421);
              match(NOT);
              setState(422);
              match(EXISTS);
            }
            break;
          }
          setState(425);
          ((CreateTableLikeContext) _localctx).target = tableIdentifier();
          setState(426);
          match(LIKE);
          setState(427);
          ((CreateTableLikeContext) _localctx).source = tableIdentifier();
          setState(436);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == LOCATION || _la == ROW || ((((_la - 218)) & ~0x3f) == 0 &&
              ((1L << (_la - 218)) & ((1L << (STORED - 218)) | (1L << (TBLPROPERTIES - 218)) | (1L
                  << (USING - 218)))) != 0)) {
            {
              setState(434);
              _errHandler.sync(this);
              switch (_input.LA(1)) {
                case USING: {
                  setState(428);
                  tableProvider();
                }
                break;
                case ROW: {
                  setState(429);
                  rowFormat();
                }
                break;
                case STORED: {
                  setState(430);
                  createFileFormat();
                }
                break;
                case LOCATION: {
                  setState(431);
                  locationSpec();
                }
                break;
                case TBLPROPERTIES: {
                  {
                    setState(432);
                    match(TBLPROPERTIES);
                    setState(433);
                    ((CreateTableLikeContext) _localctx).tableProps = tablePropertyList();
                  }
                }
                break;
                default:
                  throw new NoViableAltException(this);
              }
            }
            setState(438);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
        }
        break;
        case 12:
          _localctx = new ReplaceTableContext(_localctx);
          enterOuterAlt(_localctx, 12);
        {
          setState(439);
          replaceTableHeader();
          setState(444);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1) {
            {
              setState(440);
              match(T__1);
              setState(441);
              colTypeList();
              setState(442);
              match(T__2);
            }
          }

          setState(446);
          tableProvider();
          setState(447);
          createTableClauses();
          setState(452);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1 || _la == AS || _la == FROM || _la == MAP || ((((_la - 184)) & ~0x3f) == 0
              && ((1L << (_la - 184)) & ((1L << (REDUCE - 184)) | (1L << (SELECT - 184)) | (1L << (
              TABLE - 184)))) != 0) || _la == VALUES || _la == WITH) {
            {
              setState(449);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == AS) {
                {
                  setState(448);
                  match(AS);
                }
              }

              setState(451);
              query();
            }
          }

        }
        break;
        case 13:
          _localctx = new AnalyzeContext(_localctx);
          enterOuterAlt(_localctx, 13);
        {
          setState(454);
          match(ANALYZE);
          setState(455);
          match(TABLE);
          setState(456);
          multipartIdentifier();
          setState(458);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(457);
              partitionSpec();
            }
          }

          setState(460);
          match(COMPUTE);
          setState(461);
          match(STATISTICS);
          setState(469);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 27, _ctx)) {
            case 1: {
              setState(462);
              identifier();
            }
            break;
            case 2: {
              setState(463);
              match(FOR);
              setState(464);
              match(COLUMNS);
              setState(465);
              identifierSeq();
            }
            break;
            case 3: {
              setState(466);
              match(FOR);
              setState(467);
              match(ALL);
              setState(468);
              match(COLUMNS);
            }
            break;
          }
        }
        break;
        case 14:
          _localctx = new AddTableColumnsContext(_localctx);
          enterOuterAlt(_localctx, 14);
        {
          setState(471);
          match(ALTER);
          setState(472);
          match(TABLE);
          setState(473);
          multipartIdentifier();
          setState(474);
          match(ADD);
          setState(475);
          _la = _input.LA(1);
          if (!(_la == COLUMN || _la == COLUMNS)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(476);
          ((AddTableColumnsContext) _localctx).columns = qualifiedColTypeWithPositionList();
        }
        break;
        case 15:
          _localctx = new AddTableColumnsContext(_localctx);
          enterOuterAlt(_localctx, 15);
        {
          setState(478);
          match(ALTER);
          setState(479);
          match(TABLE);
          setState(480);
          multipartIdentifier();
          setState(481);
          match(ADD);
          setState(482);
          _la = _input.LA(1);
          if (!(_la == COLUMN || _la == COLUMNS)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(483);
          match(T__1);
          setState(484);
          ((AddTableColumnsContext) _localctx).columns = qualifiedColTypeWithPositionList();
          setState(485);
          match(T__2);
        }
        break;
        case 16:
          _localctx = new RenameTableColumnContext(_localctx);
          enterOuterAlt(_localctx, 16);
        {
          setState(487);
          match(ALTER);
          setState(488);
          match(TABLE);
          setState(489);
          ((RenameTableColumnContext) _localctx).table = multipartIdentifier();
          setState(490);
          match(RENAME);
          setState(491);
          match(COLUMN);
          setState(492);
          ((RenameTableColumnContext) _localctx).from = multipartIdentifier();
          setState(493);
          match(TO);
          setState(494);
          ((RenameTableColumnContext) _localctx).to = errorCapturingIdentifier();
        }
        break;
        case 17:
          _localctx = new DropTableColumnsContext(_localctx);
          enterOuterAlt(_localctx, 17);
        {
          setState(496);
          match(ALTER);
          setState(497);
          match(TABLE);
          setState(498);
          multipartIdentifier();
          setState(499);
          match(DROP);
          setState(500);
          _la = _input.LA(1);
          if (!(_la == COLUMN || _la == COLUMNS)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(501);
          match(T__1);
          setState(502);
          ((DropTableColumnsContext) _localctx).columns = multipartIdentifierList();
          setState(503);
          match(T__2);
        }
        break;
        case 18:
          _localctx = new DropTableColumnsContext(_localctx);
          enterOuterAlt(_localctx, 18);
        {
          setState(505);
          match(ALTER);
          setState(506);
          match(TABLE);
          setState(507);
          multipartIdentifier();
          setState(508);
          match(DROP);
          setState(509);
          _la = _input.LA(1);
          if (!(_la == COLUMN || _la == COLUMNS)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(510);
          ((DropTableColumnsContext) _localctx).columns = multipartIdentifierList();
        }
        break;
        case 19:
          _localctx = new RenameTableContext(_localctx);
          enterOuterAlt(_localctx, 19);
        {
          setState(512);
          match(ALTER);
          setState(513);
          _la = _input.LA(1);
          if (!(_la == TABLE || _la == VIEW)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(514);
          ((RenameTableContext) _localctx).from = multipartIdentifier();
          setState(515);
          match(RENAME);
          setState(516);
          match(TO);
          setState(517);
          ((RenameTableContext) _localctx).to = multipartIdentifier();
        }
        break;
        case 20:
          _localctx = new SetTablePropertiesContext(_localctx);
          enterOuterAlt(_localctx, 20);
        {
          setState(519);
          match(ALTER);
          setState(520);
          _la = _input.LA(1);
          if (!(_la == TABLE || _la == VIEW)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(521);
          multipartIdentifier();
          setState(522);
          match(SET);
          setState(523);
          match(TBLPROPERTIES);
          setState(524);
          tablePropertyList();
        }
        break;
        case 21:
          _localctx = new UnsetTablePropertiesContext(_localctx);
          enterOuterAlt(_localctx, 21);
        {
          setState(526);
          match(ALTER);
          setState(527);
          _la = _input.LA(1);
          if (!(_la == TABLE || _la == VIEW)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(528);
          multipartIdentifier();
          setState(529);
          match(UNSET);
          setState(530);
          match(TBLPROPERTIES);
          setState(533);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == IF) {
            {
              setState(531);
              match(IF);
              setState(532);
              match(EXISTS);
            }
          }

          setState(535);
          tablePropertyList();
        }
        break;
        case 22:
          _localctx = new AlterTableAlterColumnContext(_localctx);
          enterOuterAlt(_localctx, 22);
        {
          setState(537);
          match(ALTER);
          setState(538);
          match(TABLE);
          setState(539);
          ((AlterTableAlterColumnContext) _localctx).table = multipartIdentifier();
          setState(540);
          _la = _input.LA(1);
          if (!(_la == ALTER || _la == CHANGE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(542);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 29, _ctx)) {
            case 1: {
              setState(541);
              match(COLUMN);
            }
            break;
          }
          setState(544);
          ((AlterTableAlterColumnContext) _localctx).column = multipartIdentifier();
          setState(546);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == AFTER || _la == COMMENT || _la == DROP || _la == FIRST || _la == SET
              || _la == TYPE) {
            {
              setState(545);
              alterColumnAction();
            }
          }

        }
        break;
        case 23:
          _localctx = new HiveChangeColumnContext(_localctx);
          enterOuterAlt(_localctx, 23);
        {
          setState(548);
          match(ALTER);
          setState(549);
          match(TABLE);
          setState(550);
          ((HiveChangeColumnContext) _localctx).table = multipartIdentifier();
          setState(552);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(551);
              partitionSpec();
            }
          }

          setState(554);
          match(CHANGE);
          setState(556);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 32, _ctx)) {
            case 1: {
              setState(555);
              match(COLUMN);
            }
            break;
          }
          setState(558);
          ((HiveChangeColumnContext) _localctx).colName = multipartIdentifier();
          setState(559);
          colType();
          setState(561);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == AFTER || _la == FIRST) {
            {
              setState(560);
              colPosition();
            }
          }

        }
        break;
        case 24:
          _localctx = new HiveReplaceColumnsContext(_localctx);
          enterOuterAlt(_localctx, 24);
        {
          setState(563);
          match(ALTER);
          setState(564);
          match(TABLE);
          setState(565);
          ((HiveReplaceColumnsContext) _localctx).table = multipartIdentifier();
          setState(567);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(566);
              partitionSpec();
            }
          }

          setState(569);
          match(REPLACE);
          setState(570);
          match(COLUMNS);
          setState(571);
          match(T__1);
          setState(572);
          ((HiveReplaceColumnsContext) _localctx).columns = qualifiedColTypeWithPositionList();
          setState(573);
          match(T__2);
        }
        break;
        case 25:
          _localctx = new SetTableSerDeContext(_localctx);
          enterOuterAlt(_localctx, 25);
        {
          setState(575);
          match(ALTER);
          setState(576);
          match(TABLE);
          setState(577);
          multipartIdentifier();
          setState(579);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(578);
              partitionSpec();
            }
          }

          setState(581);
          match(SET);
          setState(582);
          match(SERDE);
          setState(583);
          match(STRING);
          setState(587);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == WITH) {
            {
              setState(584);
              match(WITH);
              setState(585);
              match(SERDEPROPERTIES);
              setState(586);
              tablePropertyList();
            }
          }

        }
        break;
        case 26:
          _localctx = new SetTableSerDeContext(_localctx);
          enterOuterAlt(_localctx, 26);
        {
          setState(589);
          match(ALTER);
          setState(590);
          match(TABLE);
          setState(591);
          multipartIdentifier();
          setState(593);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(592);
              partitionSpec();
            }
          }

          setState(595);
          match(SET);
          setState(596);
          match(SERDEPROPERTIES);
          setState(597);
          tablePropertyList();
        }
        break;
        case 27:
          _localctx = new AddTablePartitionContext(_localctx);
          enterOuterAlt(_localctx, 27);
        {
          setState(599);
          match(ALTER);
          setState(600);
          _la = _input.LA(1);
          if (!(_la == TABLE || _la == VIEW)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(601);
          multipartIdentifier();
          setState(602);
          match(ADD);
          setState(606);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == IF) {
            {
              setState(603);
              match(IF);
              setState(604);
              match(NOT);
              setState(605);
              match(EXISTS);
            }
          }

          setState(609);
          _errHandler.sync(this);
          _la = _input.LA(1);
          do {
            {
              {
                setState(608);
                partitionSpecLocation();
              }
            }
            setState(611);
            _errHandler.sync(this);
            _la = _input.LA(1);
          } while (_la == PARTITION);
        }
        break;
        case 28:
          _localctx = new RenameTablePartitionContext(_localctx);
          enterOuterAlt(_localctx, 28);
        {
          setState(613);
          match(ALTER);
          setState(614);
          match(TABLE);
          setState(615);
          multipartIdentifier();
          setState(616);
          ((RenameTablePartitionContext) _localctx).from = partitionSpec();
          setState(617);
          match(RENAME);
          setState(618);
          match(TO);
          setState(619);
          ((RenameTablePartitionContext) _localctx).to = partitionSpec();
        }
        break;
        case 29:
          _localctx = new DropTablePartitionsContext(_localctx);
          enterOuterAlt(_localctx, 29);
        {
          setState(621);
          match(ALTER);
          setState(622);
          _la = _input.LA(1);
          if (!(_la == TABLE || _la == VIEW)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(623);
          multipartIdentifier();
          setState(624);
          match(DROP);
          setState(627);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == IF) {
            {
              setState(625);
              match(IF);
              setState(626);
              match(EXISTS);
            }
          }

          setState(629);
          partitionSpec();
          setState(634);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(630);
                match(T__3);
                setState(631);
                partitionSpec();
              }
            }
            setState(636);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(638);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PURGE) {
            {
              setState(637);
              match(PURGE);
            }
          }

        }
        break;
        case 30:
          _localctx = new SetTableLocationContext(_localctx);
          enterOuterAlt(_localctx, 30);
        {
          setState(640);
          match(ALTER);
          setState(641);
          match(TABLE);
          setState(642);
          multipartIdentifier();
          setState(644);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(643);
              partitionSpec();
            }
          }

          setState(646);
          match(SET);
          setState(647);
          locationSpec();
        }
        break;
        case 31:
          _localctx = new RecoverPartitionsContext(_localctx);
          enterOuterAlt(_localctx, 31);
        {
          setState(649);
          match(ALTER);
          setState(650);
          match(TABLE);
          setState(651);
          multipartIdentifier();
          setState(652);
          match(RECOVER);
          setState(653);
          match(PARTITIONS);
        }
        break;
        case 32:
          _localctx = new DropTableContext(_localctx);
          enterOuterAlt(_localctx, 32);
        {
          setState(655);
          match(DROP);
          setState(656);
          match(TABLE);
          setState(659);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 44, _ctx)) {
            case 1: {
              setState(657);
              match(IF);
              setState(658);
              match(EXISTS);
            }
            break;
          }
          setState(661);
          multipartIdentifier();
          setState(663);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PURGE) {
            {
              setState(662);
              match(PURGE);
            }
          }

        }
        break;
        case 33:
          _localctx = new DropViewContext(_localctx);
          enterOuterAlt(_localctx, 33);
        {
          setState(665);
          match(DROP);
          setState(666);
          match(VIEW);
          setState(669);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 46, _ctx)) {
            case 1: {
              setState(667);
              match(IF);
              setState(668);
              match(EXISTS);
            }
            break;
          }
          setState(671);
          multipartIdentifier();
        }
        break;
        case 34:
          _localctx = new CreateViewContext(_localctx);
          enterOuterAlt(_localctx, 34);
        {
          setState(672);
          match(CREATE);
          setState(675);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OR) {
            {
              setState(673);
              match(OR);
              setState(674);
              match(REPLACE);
            }
          }

          setState(681);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == GLOBAL || _la == TEMPORARY) {
            {
              setState(678);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == GLOBAL) {
                {
                  setState(677);
                  match(GLOBAL);
                }
              }

              setState(680);
              match(TEMPORARY);
            }
          }

          setState(683);
          match(VIEW);
          setState(687);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 50, _ctx)) {
            case 1: {
              setState(684);
              match(IF);
              setState(685);
              match(NOT);
              setState(686);
              match(EXISTS);
            }
            break;
          }
          setState(689);
          multipartIdentifier();
          setState(691);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1) {
            {
              setState(690);
              identifierCommentList();
            }
          }

          setState(701);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == COMMENT || _la == PARTITIONED || _la == TBLPROPERTIES) {
            {
              setState(699);
              _errHandler.sync(this);
              switch (_input.LA(1)) {
                case COMMENT: {
                  setState(693);
                  commentSpec();
                }
                break;
                case PARTITIONED: {
                  {
                    setState(694);
                    match(PARTITIONED);
                    setState(695);
                    match(ON);
                    setState(696);
                    identifierList();
                  }
                }
                break;
                case TBLPROPERTIES: {
                  {
                    setState(697);
                    match(TBLPROPERTIES);
                    setState(698);
                    tablePropertyList();
                  }
                }
                break;
                default:
                  throw new NoViableAltException(this);
              }
            }
            setState(703);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(704);
          match(AS);
          setState(705);
          query();
        }
        break;
        case 35:
          _localctx = new CreateTempViewUsingContext(_localctx);
          enterOuterAlt(_localctx, 35);
        {
          setState(707);
          match(CREATE);
          setState(710);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OR) {
            {
              setState(708);
              match(OR);
              setState(709);
              match(REPLACE);
            }
          }

          setState(713);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == GLOBAL) {
            {
              setState(712);
              match(GLOBAL);
            }
          }

          setState(715);
          match(TEMPORARY);
          setState(716);
          match(VIEW);
          setState(717);
          tableIdentifier();
          setState(722);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1) {
            {
              setState(718);
              match(T__1);
              setState(719);
              colTypeList();
              setState(720);
              match(T__2);
            }
          }

          setState(724);
          tableProvider();
          setState(727);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OPTIONS) {
            {
              setState(725);
              match(OPTIONS);
              setState(726);
              tablePropertyList();
            }
          }

        }
        break;
        case 36:
          _localctx = new AlterViewQueryContext(_localctx);
          enterOuterAlt(_localctx, 36);
        {
          setState(729);
          match(ALTER);
          setState(730);
          match(VIEW);
          setState(731);
          multipartIdentifier();
          setState(733);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == AS) {
            {
              setState(732);
              match(AS);
            }
          }

          setState(735);
          query();
        }
        break;
        case 37:
          _localctx = new CreateFunctionContext(_localctx);
          enterOuterAlt(_localctx, 37);
        {
          setState(737);
          match(CREATE);
          setState(740);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OR) {
            {
              setState(738);
              match(OR);
              setState(739);
              match(REPLACE);
            }
          }

          setState(743);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == TEMPORARY) {
            {
              setState(742);
              match(TEMPORARY);
            }
          }

          setState(745);
          match(FUNCTION);
          setState(749);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 61, _ctx)) {
            case 1: {
              setState(746);
              match(IF);
              setState(747);
              match(NOT);
              setState(748);
              match(EXISTS);
            }
            break;
          }
          setState(751);
          multipartIdentifier();
          setState(752);
          match(AS);
          setState(753);
          ((CreateFunctionContext) _localctx).className = match(STRING);
          setState(763);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == USING) {
            {
              setState(754);
              match(USING);
              setState(755);
              resource();
              setState(760);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(756);
                    match(T__3);
                    setState(757);
                    resource();
                  }
                }
                setState(762);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
          }

        }
        break;
        case 38:
          _localctx = new DropFunctionContext(_localctx);
          enterOuterAlt(_localctx, 38);
        {
          setState(765);
          match(DROP);
          setState(767);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == TEMPORARY) {
            {
              setState(766);
              match(TEMPORARY);
            }
          }

          setState(769);
          match(FUNCTION);
          setState(772);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 65, _ctx)) {
            case 1: {
              setState(770);
              match(IF);
              setState(771);
              match(EXISTS);
            }
            break;
          }
          setState(774);
          multipartIdentifier();
        }
        break;
        case 39:
          _localctx = new ExplainContext(_localctx);
          enterOuterAlt(_localctx, 39);
        {
          setState(775);
          match(EXPLAIN);
          setState(777);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == CODEGEN || _la == COST || ((((_la - 86)) & ~0x3f) == 0 &&
              ((1L << (_la - 86)) & ((1L << (EXTENDED - 86)) | (1L << (FORMATTED - 86)) | (1L << (
                  LOGICAL - 86)))) != 0)) {
            {
              setState(776);
              _la = _input.LA(1);
              if (!(_la == CODEGEN || _la == COST || ((((_la - 86)) & ~0x3f) == 0 &&
                  ((1L << (_la - 86)) & ((1L << (EXTENDED - 86)) | (1L << (FORMATTED - 86)) | (1L
                      << (LOGICAL - 86)))) != 0))) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
            }
          }

          setState(779);
          statement();
        }
        break;
        case 40:
          _localctx = new ShowTablesContext(_localctx);
          enterOuterAlt(_localctx, 40);
        {
          setState(780);
          match(SHOW);
          setState(781);
          match(TABLES);
          setState(784);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(782);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(783);
              multipartIdentifier();
            }
          }

          setState(790);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LIKE || _la == STRING) {
            {
              setState(787);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == LIKE) {
                {
                  setState(786);
                  match(LIKE);
                }
              }

              setState(789);
              ((ShowTablesContext) _localctx).pattern = match(STRING);
            }
          }

        }
        break;
        case 41:
          _localctx = new ShowTableContext(_localctx);
          enterOuterAlt(_localctx, 41);
        {
          setState(792);
          match(SHOW);
          setState(793);
          match(TABLE);
          setState(794);
          match(EXTENDED);
          setState(797);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(795);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(796);
              ((ShowTableContext) _localctx).ns = multipartIdentifier();
            }
          }

          setState(799);
          match(LIKE);
          setState(800);
          ((ShowTableContext) _localctx).pattern = match(STRING);
          setState(802);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(801);
              partitionSpec();
            }
          }

        }
        break;
        case 42:
          _localctx = new ShowTblPropertiesContext(_localctx);
          enterOuterAlt(_localctx, 42);
        {
          setState(804);
          match(SHOW);
          setState(805);
          match(TBLPROPERTIES);
          setState(806);
          ((ShowTblPropertiesContext) _localctx).table = multipartIdentifier();
          setState(811);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1) {
            {
              setState(807);
              match(T__1);
              setState(808);
              ((ShowTblPropertiesContext) _localctx).key = tablePropertyKey();
              setState(809);
              match(T__2);
            }
          }

        }
        break;
        case 43:
          _localctx = new ShowColumnsContext(_localctx);
          enterOuterAlt(_localctx, 43);
        {
          setState(813);
          match(SHOW);
          setState(814);
          match(COLUMNS);
          setState(815);
          _la = _input.LA(1);
          if (!(_la == FROM || _la == IN)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(816);
          ((ShowColumnsContext) _localctx).table = multipartIdentifier();
          setState(819);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(817);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(818);
              ((ShowColumnsContext) _localctx).ns = multipartIdentifier();
            }
          }

        }
        break;
        case 44:
          _localctx = new ShowViewsContext(_localctx);
          enterOuterAlt(_localctx, 44);
        {
          setState(821);
          match(SHOW);
          setState(822);
          match(VIEWS);
          setState(825);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == FROM || _la == IN) {
            {
              setState(823);
              _la = _input.LA(1);
              if (!(_la == FROM || _la == IN)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
              setState(824);
              multipartIdentifier();
            }
          }

          setState(831);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LIKE || _la == STRING) {
            {
              setState(828);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == LIKE) {
                {
                  setState(827);
                  match(LIKE);
                }
              }

              setState(830);
              ((ShowViewsContext) _localctx).pattern = match(STRING);
            }
          }

        }
        break;
        case 45:
          _localctx = new ShowPartitionsContext(_localctx);
          enterOuterAlt(_localctx, 45);
        {
          setState(833);
          match(SHOW);
          setState(834);
          match(PARTITIONS);
          setState(835);
          multipartIdentifier();
          setState(837);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(836);
              partitionSpec();
            }
          }

        }
        break;
        case 46:
          _localctx = new ShowFunctionsContext(_localctx);
          enterOuterAlt(_localctx, 46);
        {
          setState(839);
          match(SHOW);
          setState(841);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 78, _ctx)) {
            case 1: {
              setState(840);
              identifier();
            }
            break;
          }
          setState(843);
          match(FUNCTIONS);
          setState(851);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 81, _ctx)) {
            case 1: {
              setState(845);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 79, _ctx)) {
                case 1: {
                  setState(844);
                  match(LIKE);
                }
                break;
              }
              setState(849);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 80, _ctx)) {
                case 1: {
                  setState(847);
                  multipartIdentifier();
                }
                break;
                case 2: {
                  setState(848);
                  ((ShowFunctionsContext) _localctx).pattern = match(STRING);
                }
                break;
              }
            }
            break;
          }
        }
        break;
        case 47:
          _localctx = new ShowCreateTableContext(_localctx);
          enterOuterAlt(_localctx, 47);
        {
          setState(853);
          match(SHOW);
          setState(854);
          match(CREATE);
          setState(855);
          match(TABLE);
          setState(856);
          multipartIdentifier();
          setState(859);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == AS) {
            {
              setState(857);
              match(AS);
              setState(858);
              match(SERDE);
            }
          }

        }
        break;
        case 48:
          _localctx = new ShowCurrentNamespaceContext(_localctx);
          enterOuterAlt(_localctx, 48);
        {
          setState(861);
          match(SHOW);
          setState(862);
          match(CURRENT);
          setState(863);
          match(NAMESPACE);
        }
        break;
        case 49:
          _localctx = new DescribeFunctionContext(_localctx);
          enterOuterAlt(_localctx, 49);
        {
          setState(864);
          _la = _input.LA(1);
          if (!(_la == DESC || _la == DESCRIBE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(865);
          match(FUNCTION);
          setState(867);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 83, _ctx)) {
            case 1: {
              setState(866);
              match(EXTENDED);
            }
            break;
          }
          setState(869);
          describeFuncName();
        }
        break;
        case 50:
          _localctx = new DescribeNamespaceContext(_localctx);
          enterOuterAlt(_localctx, 50);
        {
          setState(870);
          _la = _input.LA(1);
          if (!(_la == DESC || _la == DESCRIBE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(871);
          namespace();
          setState(873);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 84, _ctx)) {
            case 1: {
              setState(872);
              match(EXTENDED);
            }
            break;
          }
          setState(875);
          multipartIdentifier();
        }
        break;
        case 51:
          _localctx = new DescribeRelationContext(_localctx);
          enterOuterAlt(_localctx, 51);
        {
          setState(877);
          _la = _input.LA(1);
          if (!(_la == DESC || _la == DESCRIBE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(879);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 85, _ctx)) {
            case 1: {
              setState(878);
              match(TABLE);
            }
            break;
          }
          setState(882);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 86, _ctx)) {
            case 1: {
              setState(881);
              ((DescribeRelationContext) _localctx).option = _input.LT(1);
              _la = _input.LA(1);
              if (!(_la == EXTENDED || _la == FORMATTED)) {
                ((DescribeRelationContext) _localctx).option =
                    (Token) _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
            }
            break;
          }
          setState(884);
          multipartIdentifier();
          setState(886);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 87, _ctx)) {
            case 1: {
              setState(885);
              partitionSpec();
            }
            break;
          }
          setState(889);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 88, _ctx)) {
            case 1: {
              setState(888);
              describeColName();
            }
            break;
          }
        }
        break;
        case 52:
          _localctx = new DescribeQueryContext(_localctx);
          enterOuterAlt(_localctx, 52);
        {
          setState(891);
          _la = _input.LA(1);
          if (!(_la == DESC || _la == DESCRIBE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(893);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == QUERY) {
            {
              setState(892);
              match(QUERY);
            }
          }

          setState(895);
          query();
        }
        break;
        case 53:
          _localctx = new CommentNamespaceContext(_localctx);
          enterOuterAlt(_localctx, 53);
        {
          setState(896);
          match(COMMENT);
          setState(897);
          match(ON);
          setState(898);
          namespace();
          setState(899);
          multipartIdentifier();
          setState(900);
          match(IS);
          setState(901);
          ((CommentNamespaceContext) _localctx).comment = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == NULL || _la == STRING)) {
            ((CommentNamespaceContext) _localctx).comment = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 54:
          _localctx = new CommentTableContext(_localctx);
          enterOuterAlt(_localctx, 54);
        {
          setState(903);
          match(COMMENT);
          setState(904);
          match(ON);
          setState(905);
          match(TABLE);
          setState(906);
          multipartIdentifier();
          setState(907);
          match(IS);
          setState(908);
          ((CommentTableContext) _localctx).comment = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == NULL || _la == STRING)) {
            ((CommentTableContext) _localctx).comment = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 55:
          _localctx = new RefreshTableContext(_localctx);
          enterOuterAlt(_localctx, 55);
        {
          setState(910);
          match(REFRESH);
          setState(911);
          match(TABLE);
          setState(912);
          multipartIdentifier();
        }
        break;
        case 56:
          _localctx = new RefreshFunctionContext(_localctx);
          enterOuterAlt(_localctx, 56);
        {
          setState(913);
          match(REFRESH);
          setState(914);
          match(FUNCTION);
          setState(915);
          multipartIdentifier();
        }
        break;
        case 57:
          _localctx = new RefreshResourceContext(_localctx);
          enterOuterAlt(_localctx, 57);
        {
          setState(916);
          match(REFRESH);
          setState(924);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 91, _ctx)) {
            case 1: {
              setState(917);
              match(STRING);
            }
            break;
            case 2: {
              setState(921);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 90, _ctx);
              while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                if (_alt == 1 + 1) {
                  {
                    {
                      setState(918);
                      matchWildcard();
                    }
                  }
                }
                setState(923);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 90, _ctx);
              }
            }
            break;
          }
        }
        break;
        case 58:
          _localctx = new CacheTableContext(_localctx);
          enterOuterAlt(_localctx, 58);
        {
          setState(926);
          match(CACHE);
          setState(928);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LAZY) {
            {
              setState(927);
              match(LAZY);
            }
          }

          setState(930);
          match(TABLE);
          setState(931);
          multipartIdentifier();
          setState(934);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OPTIONS) {
            {
              setState(932);
              match(OPTIONS);
              setState(933);
              ((CacheTableContext) _localctx).options = tablePropertyList();
            }
          }

          setState(940);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == T__1 || _la == AS || _la == FROM || _la == MAP || ((((_la - 184)) & ~0x3f) == 0
              && ((1L << (_la - 184)) & ((1L << (REDUCE - 184)) | (1L << (SELECT - 184)) | (1L << (
              TABLE - 184)))) != 0) || _la == VALUES || _la == WITH) {
            {
              setState(937);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == AS) {
                {
                  setState(936);
                  match(AS);
                }
              }

              setState(939);
              query();
            }
          }

        }
        break;
        case 59:
          _localctx = new UncacheTableContext(_localctx);
          enterOuterAlt(_localctx, 59);
        {
          setState(942);
          match(UNCACHE);
          setState(943);
          match(TABLE);
          setState(946);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 96, _ctx)) {
            case 1: {
              setState(944);
              match(IF);
              setState(945);
              match(EXISTS);
            }
            break;
          }
          setState(948);
          multipartIdentifier();
        }
        break;
        case 60:
          _localctx = new ClearCacheContext(_localctx);
          enterOuterAlt(_localctx, 60);
        {
          setState(949);
          match(CLEAR);
          setState(950);
          match(CACHE);
        }
        break;
        case 61:
          _localctx = new LoadDataContext(_localctx);
          enterOuterAlt(_localctx, 61);
        {
          setState(951);
          match(LOAD);
          setState(952);
          match(DATA);
          setState(954);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LOCAL) {
            {
              setState(953);
              match(LOCAL);
            }
          }

          setState(956);
          match(INPATH);
          setState(957);
          ((LoadDataContext) _localctx).path = match(STRING);
          setState(959);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OVERWRITE) {
            {
              setState(958);
              match(OVERWRITE);
            }
          }

          setState(961);
          match(INTO);
          setState(962);
          match(TABLE);
          setState(963);
          multipartIdentifier();
          setState(965);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(964);
              partitionSpec();
            }
          }

        }
        break;
        case 62:
          _localctx = new TruncateTableContext(_localctx);
          enterOuterAlt(_localctx, 62);
        {
          setState(967);
          match(TRUNCATE);
          setState(968);
          match(TABLE);
          setState(969);
          multipartIdentifier();
          setState(971);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(970);
              partitionSpec();
            }
          }

        }
        break;
        case 63:
          _localctx = new RepairTableContext(_localctx);
          enterOuterAlt(_localctx, 63);
        {
          setState(973);
          match(MSCK);
          setState(974);
          match(REPAIR);
          setState(975);
          match(TABLE);
          setState(976);
          multipartIdentifier();
        }
        break;
        case 64:
          _localctx = new ManageResourceContext(_localctx);
          enterOuterAlt(_localctx, 64);
        {
          setState(977);
          ((ManageResourceContext) _localctx).op = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == ADD || _la == LIST)) {
            ((ManageResourceContext) _localctx).op = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(978);
          identifier();
          setState(986);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 102, _ctx)) {
            case 1: {
              setState(979);
              match(STRING);
            }
            break;
            case 2: {
              setState(983);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 101, _ctx);
              while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                if (_alt == 1 + 1) {
                  {
                    {
                      setState(980);
                      matchWildcard();
                    }
                  }
                }
                setState(985);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 101, _ctx);
              }
            }
            break;
          }
        }
        break;
        case 65:
          _localctx = new FailNativeCommandContext(_localctx);
          enterOuterAlt(_localctx, 65);
        {
          setState(988);
          match(SET);
          setState(989);
          match(ROLE);
          setState(993);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 103, _ctx);
          while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1 + 1) {
              {
                {
                  setState(990);
                  matchWildcard();
                }
              }
            }
            setState(995);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 103, _ctx);
          }
        }
        break;
        case 66:
          _localctx = new SetTimeZoneContext(_localctx);
          enterOuterAlt(_localctx, 66);
        {
          setState(996);
          match(SET);
          setState(997);
          match(TIME);
          setState(998);
          match(ZONE);
          setState(999);
          interval();
        }
        break;
        case 67:
          _localctx = new SetTimeZoneContext(_localctx);
          enterOuterAlt(_localctx, 67);
        {
          setState(1000);
          match(SET);
          setState(1001);
          match(TIME);
          setState(1002);
          match(ZONE);
          setState(1003);
          ((SetTimeZoneContext) _localctx).timezone = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == LOCAL || _la == STRING)) {
            ((SetTimeZoneContext) _localctx).timezone = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 68:
          _localctx = new SetTimeZoneContext(_localctx);
          enterOuterAlt(_localctx, 68);
        {
          setState(1004);
          match(SET);
          setState(1005);
          match(TIME);
          setState(1006);
          match(ZONE);
          setState(1010);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 104, _ctx);
          while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1 + 1) {
              {
                {
                  setState(1007);
                  matchWildcard();
                }
              }
            }
            setState(1012);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 104, _ctx);
          }
        }
        break;
        case 69:
          _localctx = new SetQuotedConfigurationContext(_localctx);
          enterOuterAlt(_localctx, 69);
        {
          setState(1013);
          match(SET);
          setState(1014);
          configKey();
          setState(1022);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == EQ) {
            {
              setState(1015);
              match(EQ);
              setState(1019);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 105, _ctx);
              while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                if (_alt == 1 + 1) {
                  {
                    {
                      setState(1016);
                      matchWildcard();
                    }
                  }
                }
                setState(1021);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 105, _ctx);
              }
            }
          }

        }
        break;
        case 70:
          _localctx = new SetConfigurationContext(_localctx);
          enterOuterAlt(_localctx, 70);
        {
          setState(1024);
          match(SET);
          setState(1028);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 107, _ctx);
          while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1 + 1) {
              {
                {
                  setState(1025);
                  matchWildcard();
                }
              }
            }
            setState(1030);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 107, _ctx);
          }
        }
        break;
        case 71:
          _localctx = new ResetQuotedConfigurationContext(_localctx);
          enterOuterAlt(_localctx, 71);
        {
          setState(1031);
          match(RESET);
          setState(1032);
          configKey();
        }
        break;
        case 72:
          _localctx = new ResetConfigurationContext(_localctx);
          enterOuterAlt(_localctx, 72);
        {
          setState(1033);
          match(RESET);
          setState(1037);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 108, _ctx);
          while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1 + 1) {
              {
                {
                  setState(1034);
                  matchWildcard();
                }
              }
            }
            setState(1039);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 108, _ctx);
          }
        }
        break;
        case 73:
          _localctx = new FailNativeCommandContext(_localctx);
          enterOuterAlt(_localctx, 73);
        {
          setState(1040);
          unsupportedHiveNativeCommands();
          setState(1044);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 109, _ctx);
          while (_alt != 1 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1 + 1) {
              {
                {
                  setState(1041);
                  matchWildcard();
                }
              }
            }
            setState(1046);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 109, _ctx);
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ConfigKeyContext extends ParserRuleContext {
    public QuotedIdentifierContext quotedIdentifier() {
      return getRuleContext(QuotedIdentifierContext.class, 0);
    }

    public ConfigKeyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_configKey;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitConfigKey(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConfigKeyContext configKey() throws RecognitionException {
    ConfigKeyContext _localctx = new ConfigKeyContext(_ctx, getState());
    enterRule(_localctx, 16, RULE_configKey);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1049);
        quotedIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class UnsupportedHiveNativeCommandsContext extends ParserRuleContext {
    public Token kw1;
    public Token kw2;
    public Token kw3;
    public Token kw4;
    public Token kw5;
    public Token kw6;

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode ROLE() {
      return getToken(CarbonSqlBaseParser.ROLE, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode GRANT() {
      return getToken(CarbonSqlBaseParser.GRANT, 0);
    }

    public TerminalNode REVOKE() {
      return getToken(CarbonSqlBaseParser.REVOKE, 0);
    }

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode PRINCIPALS() {
      return getToken(CarbonSqlBaseParser.PRINCIPALS, 0);
    }

    public TerminalNode ROLES() {
      return getToken(CarbonSqlBaseParser.ROLES, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(CarbonSqlBaseParser.CURRENT, 0);
    }

    public TerminalNode EXPORT() {
      return getToken(CarbonSqlBaseParser.EXPORT, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode IMPORT() {
      return getToken(CarbonSqlBaseParser.IMPORT, 0);
    }

    public TerminalNode COMPACTIONS() {
      return getToken(CarbonSqlBaseParser.COMPACTIONS, 0);
    }

    public TerminalNode TRANSACTIONS() {
      return getToken(CarbonSqlBaseParser.TRANSACTIONS, 0);
    }

    public TerminalNode INDEXES() {
      return getToken(CarbonSqlBaseParser.INDEXES, 0);
    }

    public TerminalNode LOCKS() {
      return getToken(CarbonSqlBaseParser.LOCKS, 0);
    }

    public TerminalNode INDEX() {
      return getToken(CarbonSqlBaseParser.INDEX, 0);
    }

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode LOCK() {
      return getToken(CarbonSqlBaseParser.LOCK, 0);
    }

    public TerminalNode DATABASE() {
      return getToken(CarbonSqlBaseParser.DATABASE, 0);
    }

    public TerminalNode UNLOCK() {
      return getToken(CarbonSqlBaseParser.UNLOCK, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode MACRO() {
      return getToken(CarbonSqlBaseParser.MACRO, 0);
    }

    public TableIdentifierContext tableIdentifier() {
      return getRuleContext(TableIdentifierContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode CLUSTERED() {
      return getToken(CarbonSqlBaseParser.CLUSTERED, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public TerminalNode SORTED() {
      return getToken(CarbonSqlBaseParser.SORTED, 0);
    }

    public TerminalNode SKEWED() {
      return getToken(CarbonSqlBaseParser.SKEWED, 0);
    }

    public TerminalNode STORED() {
      return getToken(CarbonSqlBaseParser.STORED, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode DIRECTORIES() {
      return getToken(CarbonSqlBaseParser.DIRECTORIES, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode LOCATION() {
      return getToken(CarbonSqlBaseParser.LOCATION, 0);
    }

    public TerminalNode EXCHANGE() {
      return getToken(CarbonSqlBaseParser.EXCHANGE, 0);
    }

    public TerminalNode PARTITION() {
      return getToken(CarbonSqlBaseParser.PARTITION, 0);
    }

    public TerminalNode ARCHIVE() {
      return getToken(CarbonSqlBaseParser.ARCHIVE, 0);
    }

    public TerminalNode UNARCHIVE() {
      return getToken(CarbonSqlBaseParser.UNARCHIVE, 0);
    }

    public TerminalNode TOUCH() {
      return getToken(CarbonSqlBaseParser.TOUCH, 0);
    }

    public TerminalNode COMPACT() {
      return getToken(CarbonSqlBaseParser.COMPACT, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode CONCATENATE() {
      return getToken(CarbonSqlBaseParser.CONCATENATE, 0);
    }

    public TerminalNode FILEFORMAT() {
      return getToken(CarbonSqlBaseParser.FILEFORMAT, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public TerminalNode START() {
      return getToken(CarbonSqlBaseParser.START, 0);
    }

    public TerminalNode TRANSACTION() {
      return getToken(CarbonSqlBaseParser.TRANSACTION, 0);
    }

    public TerminalNode COMMIT() {
      return getToken(CarbonSqlBaseParser.COMMIT, 0);
    }

    public TerminalNode ROLLBACK() {
      return getToken(CarbonSqlBaseParser.ROLLBACK, 0);
    }

    public TerminalNode DFS() {
      return getToken(CarbonSqlBaseParser.DFS, 0);
    }

    public UnsupportedHiveNativeCommandsContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_unsupportedHiveNativeCommands;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor)
            .visitUnsupportedHiveNativeCommands(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnsupportedHiveNativeCommandsContext unsupportedHiveNativeCommands()
      throws RecognitionException {
    UnsupportedHiveNativeCommandsContext _localctx =
        new UnsupportedHiveNativeCommandsContext(_ctx, getState());
    enterRule(_localctx, 18, RULE_unsupportedHiveNativeCommands);
    int _la;
    try {
      setState(1219);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 118, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1051);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(CREATE);
          setState(1052);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLE);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1053);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(DROP);
          setState(1054);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLE);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(1055);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(GRANT);
          setState(1057);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 111, _ctx)) {
            case 1: {
              setState(1056);
              ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLE);
            }
            break;
          }
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(1059);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(REVOKE);
          setState(1061);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 112, _ctx)) {
            case 1: {
              setState(1060);
              ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLE);
            }
            break;
          }
        }
        break;
        case 5:
          enterOuterAlt(_localctx, 5);
        {
          setState(1063);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1064);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(GRANT);
        }
        break;
        case 6:
          enterOuterAlt(_localctx, 6);
        {
          setState(1065);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1066);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLE);
          setState(1068);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 113, _ctx)) {
            case 1: {
              setState(1067);
              ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(GRANT);
            }
            break;
          }
        }
        break;
        case 7:
          enterOuterAlt(_localctx, 7);
        {
          setState(1070);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1071);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(PRINCIPALS);
        }
        break;
        case 8:
          enterOuterAlt(_localctx, 8);
        {
          setState(1072);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1073);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(ROLES);
        }
        break;
        case 9:
          enterOuterAlt(_localctx, 9);
        {
          setState(1074);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1075);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(CURRENT);
          setState(1076);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(ROLES);
        }
        break;
        case 10:
          enterOuterAlt(_localctx, 10);
        {
          setState(1077);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(EXPORT);
          setState(1078);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
        }
        break;
        case 11:
          enterOuterAlt(_localctx, 11);
        {
          setState(1079);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(IMPORT);
          setState(1080);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
        }
        break;
        case 12:
          enterOuterAlt(_localctx, 12);
        {
          setState(1081);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1082);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(COMPACTIONS);
        }
        break;
        case 13:
          enterOuterAlt(_localctx, 13);
        {
          setState(1083);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1084);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(CREATE);
          setState(1085);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(TABLE);
        }
        break;
        case 14:
          enterOuterAlt(_localctx, 14);
        {
          setState(1086);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1087);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TRANSACTIONS);
        }
        break;
        case 15:
          enterOuterAlt(_localctx, 15);
        {
          setState(1088);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1089);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(INDEXES);
        }
        break;
        case 16:
          enterOuterAlt(_localctx, 16);
        {
          setState(1090);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(SHOW);
          setState(1091);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(LOCKS);
        }
        break;
        case 17:
          enterOuterAlt(_localctx, 17);
        {
          setState(1092);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(CREATE);
          setState(1093);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(INDEX);
        }
        break;
        case 18:
          enterOuterAlt(_localctx, 18);
        {
          setState(1094);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(DROP);
          setState(1095);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(INDEX);
        }
        break;
        case 19:
          enterOuterAlt(_localctx, 19);
        {
          setState(1096);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1097);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(INDEX);
        }
        break;
        case 20:
          enterOuterAlt(_localctx, 20);
        {
          setState(1098);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(LOCK);
          setState(1099);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
        }
        break;
        case 21:
          enterOuterAlt(_localctx, 21);
        {
          setState(1100);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(LOCK);
          setState(1101);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(DATABASE);
        }
        break;
        case 22:
          enterOuterAlt(_localctx, 22);
        {
          setState(1102);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(UNLOCK);
          setState(1103);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
        }
        break;
        case 23:
          enterOuterAlt(_localctx, 23);
        {
          setState(1104);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(UNLOCK);
          setState(1105);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(DATABASE);
        }
        break;
        case 24:
          enterOuterAlt(_localctx, 24);
        {
          setState(1106);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(CREATE);
          setState(1107);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TEMPORARY);
          setState(1108);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(MACRO);
        }
        break;
        case 25:
          enterOuterAlt(_localctx, 25);
        {
          setState(1109);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(DROP);
          setState(1110);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TEMPORARY);
          setState(1111);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(MACRO);
        }
        break;
        case 26:
          enterOuterAlt(_localctx, 26);
        {
          setState(1112);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1113);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1114);
          tableIdentifier();
          setState(1115);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(NOT);
          setState(1116);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(CLUSTERED);
        }
        break;
        case 27:
          enterOuterAlt(_localctx, 27);
        {
          setState(1118);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1119);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1120);
          tableIdentifier();
          setState(1121);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(CLUSTERED);
          setState(1122);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(BY);
        }
        break;
        case 28:
          enterOuterAlt(_localctx, 28);
        {
          setState(1124);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1125);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1126);
          tableIdentifier();
          setState(1127);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(NOT);
          setState(1128);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(SORTED);
        }
        break;
        case 29:
          enterOuterAlt(_localctx, 29);
        {
          setState(1130);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1131);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1132);
          tableIdentifier();
          setState(1133);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(SKEWED);
          setState(1134);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(BY);
        }
        break;
        case 30:
          enterOuterAlt(_localctx, 30);
        {
          setState(1136);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1137);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1138);
          tableIdentifier();
          setState(1139);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(NOT);
          setState(1140);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(SKEWED);
        }
        break;
        case 31:
          enterOuterAlt(_localctx, 31);
        {
          setState(1142);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1143);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1144);
          tableIdentifier();
          setState(1145);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(NOT);
          setState(1146);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(STORED);
          setState(1147);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw5 = match(AS);
          setState(1148);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw6 = match(DIRECTORIES);
        }
        break;
        case 32:
          enterOuterAlt(_localctx, 32);
        {
          setState(1150);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1151);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1152);
          tableIdentifier();
          setState(1153);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(SET);
          setState(1154);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(SKEWED);
          setState(1155);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw5 = match(LOCATION);
        }
        break;
        case 33:
          enterOuterAlt(_localctx, 33);
        {
          setState(1157);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1158);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1159);
          tableIdentifier();
          setState(1160);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(EXCHANGE);
          setState(1161);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(PARTITION);
        }
        break;
        case 34:
          enterOuterAlt(_localctx, 34);
        {
          setState(1163);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1164);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1165);
          tableIdentifier();
          setState(1166);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(ARCHIVE);
          setState(1167);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(PARTITION);
        }
        break;
        case 35:
          enterOuterAlt(_localctx, 35);
        {
          setState(1169);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1170);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1171);
          tableIdentifier();
          setState(1172);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(UNARCHIVE);
          setState(1173);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(PARTITION);
        }
        break;
        case 36:
          enterOuterAlt(_localctx, 36);
        {
          setState(1175);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1176);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1177);
          tableIdentifier();
          setState(1178);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(TOUCH);
        }
        break;
        case 37:
          enterOuterAlt(_localctx, 37);
        {
          setState(1180);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1181);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1182);
          tableIdentifier();
          setState(1184);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1183);
              partitionSpec();
            }
          }

          setState(1186);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(COMPACT);
        }
        break;
        case 38:
          enterOuterAlt(_localctx, 38);
        {
          setState(1188);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1189);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1190);
          tableIdentifier();
          setState(1192);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1191);
              partitionSpec();
            }
          }

          setState(1194);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(CONCATENATE);
        }
        break;
        case 39:
          enterOuterAlt(_localctx, 39);
        {
          setState(1196);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1197);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1198);
          tableIdentifier();
          setState(1200);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1199);
              partitionSpec();
            }
          }

          setState(1202);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(SET);
          setState(1203);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(FILEFORMAT);
        }
        break;
        case 40:
          enterOuterAlt(_localctx, 40);
        {
          setState(1205);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ALTER);
          setState(1206);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TABLE);
          setState(1207);
          tableIdentifier();
          setState(1209);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1208);
              partitionSpec();
            }
          }

          setState(1211);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw3 = match(REPLACE);
          setState(1212);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw4 = match(COLUMNS);
        }
        break;
        case 41:
          enterOuterAlt(_localctx, 41);
        {
          setState(1214);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(START);
          setState(1215);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw2 = match(TRANSACTION);
        }
        break;
        case 42:
          enterOuterAlt(_localctx, 42);
        {
          setState(1216);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(COMMIT);
        }
        break;
        case 43:
          enterOuterAlt(_localctx, 43);
        {
          setState(1217);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(ROLLBACK);
        }
        break;
        case 44:
          enterOuterAlt(_localctx, 44);
        {
          setState(1218);
          ((UnsupportedHiveNativeCommandsContext) _localctx).kw1 = match(DFS);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CreateTableHeaderContext extends ParserRuleContext {
    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode EXTERNAL() {
      return getToken(CarbonSqlBaseParser.EXTERNAL, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public CreateTableHeaderContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_createTableHeader;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateTableHeader(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CreateTableHeaderContext createTableHeader() throws RecognitionException {
    CreateTableHeaderContext _localctx = new CreateTableHeaderContext(_ctx, getState());
    enterRule(_localctx, 20, RULE_createTableHeader);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1221);
        match(CREATE);
        setState(1223);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == TEMPORARY) {
          {
            setState(1222);
            match(TEMPORARY);
          }
        }

        setState(1226);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == EXTERNAL) {
          {
            setState(1225);
            match(EXTERNAL);
          }
        }

        setState(1228);
        match(TABLE);
        setState(1232);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 121, _ctx)) {
          case 1: {
            setState(1229);
            match(IF);
            setState(1230);
            match(NOT);
            setState(1231);
            match(EXISTS);
          }
          break;
        }
        setState(1234);
        multipartIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ReplaceTableHeaderContext extends ParserRuleContext {
    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public ReplaceTableHeaderContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_replaceTableHeader;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitReplaceTableHeader(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ReplaceTableHeaderContext replaceTableHeader() throws RecognitionException {
    ReplaceTableHeaderContext _localctx = new ReplaceTableHeaderContext(_ctx, getState());
    enterRule(_localctx, 22, RULE_replaceTableHeader);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1238);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == CREATE) {
          {
            setState(1236);
            match(CREATE);
            setState(1237);
            match(OR);
          }
        }

        setState(1240);
        match(REPLACE);
        setState(1241);
        match(TABLE);
        setState(1242);
        multipartIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BucketSpecContext extends ParserRuleContext {
    public TerminalNode CLUSTERED() {
      return getToken(CarbonSqlBaseParser.CLUSTERED, 0);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public TerminalNode INTEGER_VALUE() {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode BUCKETS() {
      return getToken(CarbonSqlBaseParser.BUCKETS, 0);
    }

    public TerminalNode SORTED() {
      return getToken(CarbonSqlBaseParser.SORTED, 0);
    }

    public OrderedIdentifierListContext orderedIdentifierList() {
      return getRuleContext(OrderedIdentifierListContext.class, 0);
    }

    public BucketSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_bucketSpec;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitBucketSpec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BucketSpecContext bucketSpec() throws RecognitionException {
    BucketSpecContext _localctx = new BucketSpecContext(_ctx, getState());
    enterRule(_localctx, 24, RULE_bucketSpec);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1244);
        match(CLUSTERED);
        setState(1245);
        match(BY);
        setState(1246);
        identifierList();
        setState(1250);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == SORTED) {
          {
            setState(1247);
            match(SORTED);
            setState(1248);
            match(BY);
            setState(1249);
            orderedIdentifierList();
          }
        }

        setState(1252);
        match(INTO);
        setState(1253);
        match(INTEGER_VALUE);
        setState(1254);
        match(BUCKETS);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SkewSpecContext extends ParserRuleContext {
    public TerminalNode SKEWED() {
      return getToken(CarbonSqlBaseParser.SKEWED, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public ConstantListContext constantList() {
      return getRuleContext(ConstantListContext.class, 0);
    }

    public NestedConstantListContext nestedConstantList() {
      return getRuleContext(NestedConstantListContext.class, 0);
    }

    public TerminalNode STORED() {
      return getToken(CarbonSqlBaseParser.STORED, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode DIRECTORIES() {
      return getToken(CarbonSqlBaseParser.DIRECTORIES, 0);
    }

    public SkewSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_skewSpec;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSkewSpec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SkewSpecContext skewSpec() throws RecognitionException {
    SkewSpecContext _localctx = new SkewSpecContext(_ctx, getState());
    enterRule(_localctx, 26, RULE_skewSpec);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1256);
        match(SKEWED);
        setState(1257);
        match(BY);
        setState(1258);
        identifierList();
        setState(1259);
        match(ON);
        setState(1262);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 124, _ctx)) {
          case 1: {
            setState(1260);
            constantList();
          }
          break;
          case 2: {
            setState(1261);
            nestedConstantList();
          }
          break;
        }
        setState(1267);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 125, _ctx)) {
          case 1: {
            setState(1264);
            match(STORED);
            setState(1265);
            match(AS);
            setState(1266);
            match(DIRECTORIES);
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LocationSpecContext extends ParserRuleContext {
    public TerminalNode LOCATION() {
      return getToken(CarbonSqlBaseParser.LOCATION, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public LocationSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_locationSpec;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLocationSpec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LocationSpecContext locationSpec() throws RecognitionException {
    LocationSpecContext _localctx = new LocationSpecContext(_ctx, getState());
    enterRule(_localctx, 28, RULE_locationSpec);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1269);
        match(LOCATION);
        setState(1270);
        match(STRING);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CommentSpecContext extends ParserRuleContext {
    public TerminalNode COMMENT() {
      return getToken(CarbonSqlBaseParser.COMMENT, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public CommentSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_commentSpec;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCommentSpec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CommentSpecContext commentSpec() throws RecognitionException {
    CommentSpecContext _localctx = new CommentSpecContext(_ctx, getState());
    enterRule(_localctx, 30, RULE_commentSpec);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1272);
        match(COMMENT);
        setState(1273);
        match(STRING);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryContext extends ParserRuleContext {
    public QueryTermContext queryTerm() {
      return getRuleContext(QueryTermContext.class, 0);
    }

    public QueryOrganizationContext queryOrganization() {
      return getRuleContext(QueryOrganizationContext.class, 0);
    }

    public CtesContext ctes() {
      return getRuleContext(CtesContext.class, 0);
    }

    public QueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_query;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryContext query() throws RecognitionException {
    QueryContext _localctx = new QueryContext(_ctx, getState());
    enterRule(_localctx, 32, RULE_query);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1276);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == WITH) {
          {
            setState(1275);
            ctes();
          }
        }

        setState(1278);
        queryTerm(0);
        setState(1279);
        queryOrganization();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class InsertIntoContext extends ParserRuleContext {
    public InsertIntoContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_insertInto;
    }

    public InsertIntoContext() {
    }

    public void copyFrom(InsertIntoContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class InsertOverwriteHiveDirContext extends InsertIntoContext {
    public Token path;

    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public TerminalNode DIRECTORY() {
      return getToken(CarbonSqlBaseParser.DIRECTORY, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public RowFormatContext rowFormat() {
      return getRuleContext(RowFormatContext.class, 0);
    }

    public CreateFileFormatContext createFileFormat() {
      return getRuleContext(CreateFileFormatContext.class, 0);
    }

    public InsertOverwriteHiveDirContext(InsertIntoContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInsertOverwriteHiveDir(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InsertOverwriteDirContext extends InsertIntoContext {
    public Token path;
    public TablePropertyListContext options;

    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public TerminalNode DIRECTORY() {
      return getToken(CarbonSqlBaseParser.DIRECTORY, 0);
    }

    public TableProviderContext tableProvider() {
      return getRuleContext(TableProviderContext.class, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(CarbonSqlBaseParser.OPTIONS, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public InsertOverwriteDirContext(InsertIntoContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInsertOverwriteDir(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InsertOverwriteTableContext extends InsertIntoContext {
    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public InsertOverwriteTableContext(InsertIntoContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInsertOverwriteTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InsertIntoTableContext extends InsertIntoContext {
    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public InsertIntoTableContext(InsertIntoContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInsertIntoTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InsertIntoContext insertInto() throws RecognitionException {
    InsertIntoContext _localctx = new InsertIntoContext(_ctx, getState());
    enterRule(_localctx, 34, RULE_insertInto);
    int _la;
    try {
      setState(1336);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 139, _ctx)) {
        case 1:
          _localctx = new InsertOverwriteTableContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1281);
          match(INSERT);
          setState(1282);
          match(OVERWRITE);
          setState(1284);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 127, _ctx)) {
            case 1: {
              setState(1283);
              match(TABLE);
            }
            break;
          }
          setState(1286);
          multipartIdentifier();
          setState(1293);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1287);
              partitionSpec();
              setState(1291);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == IF) {
                {
                  setState(1288);
                  match(IF);
                  setState(1289);
                  match(NOT);
                  setState(1290);
                  match(EXISTS);
                }
              }

            }
          }

        }
        break;
        case 2:
          _localctx = new InsertIntoTableContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1295);
          match(INSERT);
          setState(1296);
          match(INTO);
          setState(1298);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 130, _ctx)) {
            case 1: {
              setState(1297);
              match(TABLE);
            }
            break;
          }
          setState(1300);
          multipartIdentifier();
          setState(1302);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PARTITION) {
            {
              setState(1301);
              partitionSpec();
            }
          }

          setState(1307);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == IF) {
            {
              setState(1304);
              match(IF);
              setState(1305);
              match(NOT);
              setState(1306);
              match(EXISTS);
            }
          }

        }
        break;
        case 3:
          _localctx = new InsertOverwriteHiveDirContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1309);
          match(INSERT);
          setState(1310);
          match(OVERWRITE);
          setState(1312);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LOCAL) {
            {
              setState(1311);
              match(LOCAL);
            }
          }

          setState(1314);
          match(DIRECTORY);
          setState(1315);
          ((InsertOverwriteHiveDirContext) _localctx).path = match(STRING);
          setState(1317);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == ROW) {
            {
              setState(1316);
              rowFormat();
            }
          }

          setState(1320);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == STORED) {
            {
              setState(1319);
              createFileFormat();
            }
          }

        }
        break;
        case 4:
          _localctx = new InsertOverwriteDirContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1322);
          match(INSERT);
          setState(1323);
          match(OVERWRITE);
          setState(1325);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LOCAL) {
            {
              setState(1324);
              match(LOCAL);
            }
          }

          setState(1327);
          match(DIRECTORY);
          setState(1329);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == STRING) {
            {
              setState(1328);
              ((InsertOverwriteDirContext) _localctx).path = match(STRING);
            }
          }

          setState(1331);
          tableProvider();
          setState(1334);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OPTIONS) {
            {
              setState(1332);
              match(OPTIONS);
              setState(1333);
              ((InsertOverwriteDirContext) _localctx).options = tablePropertyList();
            }
          }

        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PartitionSpecLocationContext extends ParserRuleContext {
    public PartitionSpecContext partitionSpec() {
      return getRuleContext(PartitionSpecContext.class, 0);
    }

    public LocationSpecContext locationSpec() {
      return getRuleContext(LocationSpecContext.class, 0);
    }

    public PartitionSpecLocationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_partitionSpecLocation;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPartitionSpecLocation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PartitionSpecLocationContext partitionSpecLocation() throws RecognitionException {
    PartitionSpecLocationContext _localctx = new PartitionSpecLocationContext(_ctx, getState());
    enterRule(_localctx, 36, RULE_partitionSpecLocation);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1338);
        partitionSpec();
        setState(1340);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == LOCATION) {
          {
            setState(1339);
            locationSpec();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PartitionSpecContext extends ParserRuleContext {
    public TerminalNode PARTITION() {
      return getToken(CarbonSqlBaseParser.PARTITION, 0);
    }

    public List<PartitionValContext> partitionVal() {
      return getRuleContexts(PartitionValContext.class);
    }

    public PartitionValContext partitionVal(int i) {
      return getRuleContext(PartitionValContext.class, i);
    }

    public PartitionSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_partitionSpec;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPartitionSpec(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PartitionSpecContext partitionSpec() throws RecognitionException {
    PartitionSpecContext _localctx = new PartitionSpecContext(_ctx, getState());
    enterRule(_localctx, 38, RULE_partitionSpec);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1342);
        match(PARTITION);
        setState(1343);
        match(T__1);
        setState(1344);
        partitionVal();
        setState(1349);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1345);
              match(T__3);
              setState(1346);
              partitionVal();
            }
          }
          setState(1351);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1352);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PartitionValContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode EQ() {
      return getToken(CarbonSqlBaseParser.EQ, 0);
    }

    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class, 0);
    }

    public PartitionValContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_partitionVal;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPartitionVal(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PartitionValContext partitionVal() throws RecognitionException {
    PartitionValContext _localctx = new PartitionValContext(_ctx, getState());
    enterRule(_localctx, 40, RULE_partitionVal);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1354);
        identifier();
        setState(1357);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == EQ) {
          {
            setState(1355);
            match(EQ);
            setState(1356);
            constant();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamespaceContext extends ParserRuleContext {
    public TerminalNode NAMESPACE() {
      return getToken(CarbonSqlBaseParser.NAMESPACE, 0);
    }

    public TerminalNode DATABASE() {
      return getToken(CarbonSqlBaseParser.DATABASE, 0);
    }

    public TerminalNode SCHEMA() {
      return getToken(CarbonSqlBaseParser.SCHEMA, 0);
    }

    public NamespaceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namespace;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNamespace(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamespaceContext namespace() throws RecognitionException {
    NamespaceContext _localctx = new NamespaceContext(_ctx, getState());
    enterRule(_localctx, 42, RULE_namespace);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1359);
        _la = _input.LA(1);
        if (!(_la == DATABASE || _la == NAMESPACE || _la == SCHEMA)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DescribeFuncNameContext extends ParserRuleContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class, 0);
    }

    public ArithmeticOperatorContext arithmeticOperator() {
      return getRuleContext(ArithmeticOperatorContext.class, 0);
    }

    public PredicateOperatorContext predicateOperator() {
      return getRuleContext(PredicateOperatorContext.class, 0);
    }

    public DescribeFuncNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_describeFuncName;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeFuncName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DescribeFuncNameContext describeFuncName() throws RecognitionException {
    DescribeFuncNameContext _localctx = new DescribeFuncNameContext(_ctx, getState());
    enterRule(_localctx, 44, RULE_describeFuncName);
    try {
      setState(1366);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 143, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1361);
          qualifiedName();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1362);
          match(STRING);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(1363);
          comparisonOperator();
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(1364);
          arithmeticOperator();
        }
        break;
        case 5:
          enterOuterAlt(_localctx, 5);
        {
          setState(1365);
          predicateOperator();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DescribeColNameContext extends ParserRuleContext {
    public IdentifierContext identifier;
    public List<IdentifierContext> nameParts = new ArrayList<IdentifierContext>();

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public DescribeColNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_describeColName;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDescribeColName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DescribeColNameContext describeColName() throws RecognitionException {
    DescribeColNameContext _localctx = new DescribeColNameContext(_ctx, getState());
    enterRule(_localctx, 46, RULE_describeColName);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1368);
        ((DescribeColNameContext) _localctx).identifier = identifier();
        ((DescribeColNameContext) _localctx).nameParts
            .add(((DescribeColNameContext) _localctx).identifier);
        setState(1373);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__4) {
          {
            {
              setState(1369);
              match(T__4);
              setState(1370);
              ((DescribeColNameContext) _localctx).identifier = identifier();
              ((DescribeColNameContext) _localctx).nameParts
                  .add(((DescribeColNameContext) _localctx).identifier);
            }
          }
          setState(1375);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CtesContext extends ParserRuleContext {
    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public List<NamedQueryContext> namedQuery() {
      return getRuleContexts(NamedQueryContext.class);
    }

    public NamedQueryContext namedQuery(int i) {
      return getRuleContext(NamedQueryContext.class, i);
    }

    public CtesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_ctes;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCtes(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CtesContext ctes() throws RecognitionException {
    CtesContext _localctx = new CtesContext(_ctx, getState());
    enterRule(_localctx, 48, RULE_ctes);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1376);
        match(WITH);
        setState(1377);
        namedQuery();
        setState(1382);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1378);
              match(T__3);
              setState(1379);
              namedQuery();
            }
          }
          setState(1384);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamedQueryContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext name;
    public IdentifierListContext columnAliases;

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public NamedQueryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namedQuery;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNamedQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedQueryContext namedQuery() throws RecognitionException {
    NamedQueryContext _localctx = new NamedQueryContext(_ctx, getState());
    enterRule(_localctx, 50, RULE_namedQuery);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1385);
        ((NamedQueryContext) _localctx).name = errorCapturingIdentifier();
        setState(1387);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 146, _ctx)) {
          case 1: {
            setState(1386);
            ((NamedQueryContext) _localctx).columnAliases = identifierList();
          }
          break;
        }
        setState(1390);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == AS) {
          {
            setState(1389);
            match(AS);
          }
        }

        setState(1392);
        match(T__1);
        setState(1393);
        query();
        setState(1394);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TableProviderContext extends ParserRuleContext {
    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TableProviderContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableProvider;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableProvider(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TableProviderContext tableProvider() throws RecognitionException {
    TableProviderContext _localctx = new TableProviderContext(_ctx, getState());
    enterRule(_localctx, 52, RULE_tableProvider);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1396);
        match(USING);
        setState(1397);
        multipartIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CreateTableClausesContext extends ParserRuleContext {
    public TablePropertyListContext options;
    public TransformListContext partitioning;
    public TablePropertyListContext tableProps;

    public List<BucketSpecContext> bucketSpec() {
      return getRuleContexts(BucketSpecContext.class);
    }

    public BucketSpecContext bucketSpec(int i) {
      return getRuleContext(BucketSpecContext.class, i);
    }

    public List<LocationSpecContext> locationSpec() {
      return getRuleContexts(LocationSpecContext.class);
    }

    public LocationSpecContext locationSpec(int i) {
      return getRuleContext(LocationSpecContext.class, i);
    }

    public List<CommentSpecContext> commentSpec() {
      return getRuleContexts(CommentSpecContext.class);
    }

    public CommentSpecContext commentSpec(int i) {
      return getRuleContext(CommentSpecContext.class, i);
    }

    public List<TerminalNode> OPTIONS() {
      return getTokens(CarbonSqlBaseParser.OPTIONS);
    }

    public TerminalNode OPTIONS(int i) {
      return getToken(CarbonSqlBaseParser.OPTIONS, i);
    }

    public List<TerminalNode> PARTITIONED() {
      return getTokens(CarbonSqlBaseParser.PARTITIONED);
    }

    public TerminalNode PARTITIONED(int i) {
      return getToken(CarbonSqlBaseParser.PARTITIONED, i);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public List<TerminalNode> TBLPROPERTIES() {
      return getTokens(CarbonSqlBaseParser.TBLPROPERTIES);
    }

    public TerminalNode TBLPROPERTIES(int i) {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, i);
    }

    public List<TablePropertyListContext> tablePropertyList() {
      return getRuleContexts(TablePropertyListContext.class);
    }

    public TablePropertyListContext tablePropertyList(int i) {
      return getRuleContext(TablePropertyListContext.class, i);
    }

    public List<TransformListContext> transformList() {
      return getRuleContexts(TransformListContext.class);
    }

    public TransformListContext transformList(int i) {
      return getRuleContext(TransformListContext.class, i);
    }

    public CreateTableClausesContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_createTableClauses;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateTableClauses(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CreateTableClausesContext createTableClauses() throws RecognitionException {
    CreateTableClausesContext _localctx = new CreateTableClausesContext(_ctx, getState());
    enterRule(_localctx, 54, RULE_createTableClauses);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1411);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == CLUSTERED || _la == COMMENT || ((((_la - 137)) & ~0x3f) == 0 &&
            ((1L << (_la - 137)) & ((1L << (LOCATION - 137)) | (1L << (OPTIONS - 137)) | (1L << (
                PARTITIONED - 137)))) != 0) || _la == TBLPROPERTIES) {
          {
            setState(1409);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
              case OPTIONS: {
                {
                  setState(1399);
                  match(OPTIONS);
                  setState(1400);
                  ((CreateTableClausesContext) _localctx).options = tablePropertyList();
                }
              }
              break;
              case PARTITIONED: {
                {
                  setState(1401);
                  match(PARTITIONED);
                  setState(1402);
                  match(BY);
                  setState(1403);
                  ((CreateTableClausesContext) _localctx).partitioning = transformList();
                }
              }
              break;
              case CLUSTERED: {
                setState(1404);
                bucketSpec();
              }
              break;
              case LOCATION: {
                setState(1405);
                locationSpec();
              }
              break;
              case COMMENT: {
                setState(1406);
                commentSpec();
              }
              break;
              case TBLPROPERTIES: {
                {
                  setState(1407);
                  match(TBLPROPERTIES);
                  setState(1408);
                  ((CreateTableClausesContext) _localctx).tableProps = tablePropertyList();
                }
              }
              break;
              default:
                throw new NoViableAltException(this);
            }
          }
          setState(1413);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertyListContext extends ParserRuleContext {
    public List<TablePropertyContext> tableProperty() {
      return getRuleContexts(TablePropertyContext.class);
    }

    public TablePropertyContext tableProperty(int i) {
      return getRuleContext(TablePropertyContext.class, i);
    }

    public TablePropertyListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tablePropertyList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTablePropertyList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TablePropertyListContext tablePropertyList() throws RecognitionException {
    TablePropertyListContext _localctx = new TablePropertyListContext(_ctx, getState());
    enterRule(_localctx, 56, RULE_tablePropertyList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1414);
        match(T__1);
        setState(1415);
        tableProperty();
        setState(1420);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1416);
              match(T__3);
              setState(1417);
              tableProperty();
            }
          }
          setState(1422);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1423);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertyContext extends ParserRuleContext {
    public TablePropertyKeyContext key;
    public TablePropertyValueContext value;

    public TablePropertyKeyContext tablePropertyKey() {
      return getRuleContext(TablePropertyKeyContext.class, 0);
    }

    public TablePropertyValueContext tablePropertyValue() {
      return getRuleContext(TablePropertyValueContext.class, 0);
    }

    public TerminalNode EQ() {
      return getToken(CarbonSqlBaseParser.EQ, 0);
    }

    public TablePropertyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableProperty;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableProperty(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TablePropertyContext tableProperty() throws RecognitionException {
    TablePropertyContext _localctx = new TablePropertyContext(_ctx, getState());
    enterRule(_localctx, 58, RULE_tableProperty);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1425);
        ((TablePropertyContext) _localctx).key = tablePropertyKey();
        setState(1430);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == FALSE || ((((_la - 238)) & ~0x3f) == 0 &&
            ((1L << (_la - 238)) & ((1L << (TRUE - 238)) | (1L << (EQ - 238)) | (1L << (STRING
                - 238)) | (1L << (INTEGER_VALUE - 238)) | (1L << (DECIMAL_VALUE - 238)))) != 0)) {
          {
            setState(1427);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == EQ) {
              {
                setState(1426);
                match(EQ);
              }
            }

            setState(1429);
            ((TablePropertyContext) _localctx).value = tablePropertyValue();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertyKeyContext extends ParserRuleContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TablePropertyKeyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tablePropertyKey;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTablePropertyKey(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TablePropertyKeyContext tablePropertyKey() throws RecognitionException {
    TablePropertyKeyContext _localctx = new TablePropertyKeyContext(_ctx, getState());
    enterRule(_localctx, 60, RULE_tablePropertyKey);
    int _la;
    try {
      setState(1441);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 154, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1432);
          identifier();
          setState(1437);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__4) {
            {
              {
                setState(1433);
                match(T__4);
                setState(1434);
                identifier();
              }
            }
            setState(1439);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1440);
          match(STRING);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TablePropertyValueContext extends ParserRuleContext {
    public TerminalNode INTEGER_VALUE() {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode DECIMAL_VALUE() {
      return getToken(CarbonSqlBaseParser.DECIMAL_VALUE, 0);
    }

    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TablePropertyValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tablePropertyValue;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTablePropertyValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TablePropertyValueContext tablePropertyValue() throws RecognitionException {
    TablePropertyValueContext _localctx = new TablePropertyValueContext(_ctx, getState());
    enterRule(_localctx, 62, RULE_tablePropertyValue);
    try {
      setState(1447);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case INTEGER_VALUE:
          enterOuterAlt(_localctx, 1);
        {
          setState(1443);
          match(INTEGER_VALUE);
        }
        break;
        case DECIMAL_VALUE:
          enterOuterAlt(_localctx, 2);
        {
          setState(1444);
          match(DECIMAL_VALUE);
        }
        break;
        case FALSE:
        case TRUE:
          enterOuterAlt(_localctx, 3);
        {
          setState(1445);
          booleanValue();
        }
        break;
        case STRING:
          enterOuterAlt(_localctx, 4);
        {
          setState(1446);
          match(STRING);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ConstantListContext extends ParserRuleContext {
    public List<ConstantContext> constant() {
      return getRuleContexts(ConstantContext.class);
    }

    public ConstantContext constant(int i) {
      return getRuleContext(ConstantContext.class, i);
    }

    public ConstantListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_constantList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitConstantList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantListContext constantList() throws RecognitionException {
    ConstantListContext _localctx = new ConstantListContext(_ctx, getState());
    enterRule(_localctx, 64, RULE_constantList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1449);
        match(T__1);
        setState(1450);
        constant();
        setState(1455);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1451);
              match(T__3);
              setState(1452);
              constant();
            }
          }
          setState(1457);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1458);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NestedConstantListContext extends ParserRuleContext {
    public List<ConstantListContext> constantList() {
      return getRuleContexts(ConstantListContext.class);
    }

    public ConstantListContext constantList(int i) {
      return getRuleContext(ConstantListContext.class, i);
    }

    public NestedConstantListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_nestedConstantList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNestedConstantList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NestedConstantListContext nestedConstantList() throws RecognitionException {
    NestedConstantListContext _localctx = new NestedConstantListContext(_ctx, getState());
    enterRule(_localctx, 66, RULE_nestedConstantList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1460);
        match(T__1);
        setState(1461);
        constantList();
        setState(1466);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1462);
              match(T__3);
              setState(1463);
              constantList();
            }
          }
          setState(1468);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1469);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class CreateFileFormatContext extends ParserRuleContext {
    public TerminalNode STORED() {
      return getToken(CarbonSqlBaseParser.STORED, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public FileFormatContext fileFormat() {
      return getRuleContext(FileFormatContext.class, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public StorageHandlerContext storageHandler() {
      return getRuleContext(StorageHandlerContext.class, 0);
    }

    public CreateFileFormatContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_createFileFormat;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCreateFileFormat(this);
      else return visitor.visitChildren(this);
    }
  }

  public final CreateFileFormatContext createFileFormat() throws RecognitionException {
    CreateFileFormatContext _localctx = new CreateFileFormatContext(_ctx, getState());
    enterRule(_localctx, 68, RULE_createFileFormat);
    try {
      setState(1477);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 158, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1471);
          match(STORED);
          setState(1472);
          match(AS);
          setState(1473);
          fileFormat();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1474);
          match(STORED);
          setState(1475);
          match(BY);
          setState(1476);
          storageHandler();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FileFormatContext extends ParserRuleContext {
    public FileFormatContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fileFormat;
    }

    public FileFormatContext() {
    }

    public void copyFrom(FileFormatContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class TableFileFormatContext extends FileFormatContext {
    public Token inFmt;
    public Token outFmt;

    public TerminalNode INPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.INPUTFORMAT, 0);
    }

    public TerminalNode OUTPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.OUTPUTFORMAT, 0);
    }

    public List<TerminalNode> STRING() {
      return getTokens(CarbonSqlBaseParser.STRING);
    }

    public TerminalNode STRING(int i) {
      return getToken(CarbonSqlBaseParser.STRING, i);
    }

    public TableFileFormatContext(FileFormatContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableFileFormat(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class GenericFileFormatContext extends FileFormatContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public GenericFileFormatContext(FileFormatContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitGenericFileFormat(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FileFormatContext fileFormat() throws RecognitionException {
    FileFormatContext _localctx = new FileFormatContext(_ctx, getState());
    enterRule(_localctx, 70, RULE_fileFormat);
    try {
      setState(1484);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 159, _ctx)) {
        case 1:
          _localctx = new TableFileFormatContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1479);
          match(INPUTFORMAT);
          setState(1480);
          ((TableFileFormatContext) _localctx).inFmt = match(STRING);
          setState(1481);
          match(OUTPUTFORMAT);
          setState(1482);
          ((TableFileFormatContext) _localctx).outFmt = match(STRING);
        }
        break;
        case 2:
          _localctx = new GenericFileFormatContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1483);
          identifier();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StorageHandlerContext extends ParserRuleContext {
    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public TerminalNode SERDEPROPERTIES() {
      return getToken(CarbonSqlBaseParser.SERDEPROPERTIES, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public StorageHandlerContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_storageHandler;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStorageHandler(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StorageHandlerContext storageHandler() throws RecognitionException {
    StorageHandlerContext _localctx = new StorageHandlerContext(_ctx, getState());
    enterRule(_localctx, 72, RULE_storageHandler);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1486);
        match(STRING);
        setState(1490);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 160, _ctx)) {
          case 1: {
            setState(1487);
            match(WITH);
            setState(1488);
            match(SERDEPROPERTIES);
            setState(1489);
            tablePropertyList();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ResourceContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public ResourceContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_resource;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitResource(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ResourceContext resource() throws RecognitionException {
    ResourceContext _localctx = new ResourceContext(_ctx, getState());
    enterRule(_localctx, 74, RULE_resource);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1492);
        identifier();
        setState(1493);
        match(STRING);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DmlStatementNoWithContext extends ParserRuleContext {
    public DmlStatementNoWithContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_dmlStatementNoWith;
    }

    public DmlStatementNoWithContext() {
    }

    public void copyFrom(DmlStatementNoWithContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class DeleteFromTableContext extends DmlStatementNoWithContext {
    public TerminalNode DELETE() {
      return getToken(CarbonSqlBaseParser.DELETE, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public WhereClauseContext whereClause() {
      return getRuleContext(WhereClauseContext.class, 0);
    }

    public DeleteFromTableContext(DmlStatementNoWithContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDeleteFromTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SingleInsertQueryContext extends DmlStatementNoWithContext {
    public InsertIntoContext insertInto() {
      return getRuleContext(InsertIntoContext.class, 0);
    }

    public QueryTermContext queryTerm() {
      return getRuleContext(QueryTermContext.class, 0);
    }

    public QueryOrganizationContext queryOrganization() {
      return getRuleContext(QueryOrganizationContext.class, 0);
    }

    public SingleInsertQueryContext(DmlStatementNoWithContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSingleInsertQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class MultiInsertQueryContext extends DmlStatementNoWithContext {
    public FromClauseContext fromClause() {
      return getRuleContext(FromClauseContext.class, 0);
    }

    public List<MultiInsertQueryBodyContext> multiInsertQueryBody() {
      return getRuleContexts(MultiInsertQueryBodyContext.class);
    }

    public MultiInsertQueryBodyContext multiInsertQueryBody(int i) {
      return getRuleContext(MultiInsertQueryBodyContext.class, i);
    }

    public MultiInsertQueryContext(DmlStatementNoWithContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMultiInsertQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UpdateTableContext extends DmlStatementNoWithContext {
    public TerminalNode UPDATE() {
      return getToken(CarbonSqlBaseParser.UPDATE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public SetClauseContext setClause() {
      return getRuleContext(SetClauseContext.class, 0);
    }

    public WhereClauseContext whereClause() {
      return getRuleContext(WhereClauseContext.class, 0);
    }

    public UpdateTableContext(DmlStatementNoWithContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUpdateTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class MergeIntoTableContext extends DmlStatementNoWithContext {
    public MultipartIdentifierContext target;
    public TableAliasContext targetAlias;
    public MultipartIdentifierContext source;
    public QueryContext sourceQuery;
    public TableAliasContext sourceAlias;
    public BooleanExpressionContext mergeCondition;

    public TerminalNode MERGE() {
      return getToken(CarbonSqlBaseParser.MERGE, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public List<TableAliasContext> tableAlias() {
      return getRuleContexts(TableAliasContext.class);
    }

    public TableAliasContext tableAlias(int i) {
      return getRuleContext(TableAliasContext.class, i);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public List<MatchedClauseContext> matchedClause() {
      return getRuleContexts(MatchedClauseContext.class);
    }

    public MatchedClauseContext matchedClause(int i) {
      return getRuleContext(MatchedClauseContext.class, i);
    }

    public List<NotMatchedClauseContext> notMatchedClause() {
      return getRuleContexts(NotMatchedClauseContext.class);
    }

    public NotMatchedClauseContext notMatchedClause(int i) {
      return getRuleContext(NotMatchedClauseContext.class, i);
    }

    public MergeIntoTableContext(DmlStatementNoWithContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMergeIntoTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DmlStatementNoWithContext dmlStatementNoWith() throws RecognitionException {
    DmlStatementNoWithContext _localctx = new DmlStatementNoWithContext(_ctx, getState());
    enterRule(_localctx, 76, RULE_dmlStatementNoWith);
    int _la;
    try {
      int _alt;
      setState(1546);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case INSERT:
          _localctx = new SingleInsertQueryContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1495);
          insertInto();
          setState(1496);
          queryTerm(0);
          setState(1497);
          queryOrganization();
        }
        break;
        case FROM:
          _localctx = new MultiInsertQueryContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1499);
          fromClause();
          setState(1501);
          _errHandler.sync(this);
          _la = _input.LA(1);
          do {
            {
              {
                setState(1500);
                multiInsertQueryBody();
              }
            }
            setState(1503);
            _errHandler.sync(this);
            _la = _input.LA(1);
          } while (_la == INSERT);
        }
        break;
        case DELETE:
          _localctx = new DeleteFromTableContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1505);
          match(DELETE);
          setState(1506);
          match(FROM);
          setState(1507);
          multipartIdentifier();
          setState(1508);
          tableAlias();
          setState(1510);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == WHERE) {
            {
              setState(1509);
              whereClause();
            }
          }

        }
        break;
        case UPDATE:
          _localctx = new UpdateTableContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1512);
          match(UPDATE);
          setState(1513);
          multipartIdentifier();
          setState(1514);
          tableAlias();
          setState(1515);
          setClause();
          setState(1517);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == WHERE) {
            {
              setState(1516);
              whereClause();
            }
          }

        }
        break;
        case MERGE:
          _localctx = new MergeIntoTableContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(1519);
          match(MERGE);
          setState(1520);
          match(INTO);
          setState(1521);
          ((MergeIntoTableContext) _localctx).target = multipartIdentifier();
          setState(1522);
          ((MergeIntoTableContext) _localctx).targetAlias = tableAlias();
          setState(1523);
          match(USING);
          setState(1529);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 164, _ctx)) {
            case 1: {
              setState(1524);
              ((MergeIntoTableContext) _localctx).source = multipartIdentifier();
            }
            break;
            case 2: {
              setState(1525);
              match(T__1);
              setState(1526);
              ((MergeIntoTableContext) _localctx).sourceQuery = query();
              setState(1527);
              match(T__2);
            }
            break;
          }
          setState(1531);
          ((MergeIntoTableContext) _localctx).sourceAlias = tableAlias();
          setState(1532);
          match(ON);
          setState(1533);
          ((MergeIntoTableContext) _localctx).mergeCondition = booleanExpression(0);
          setState(1537);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 165, _ctx);
          while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1) {
              {
                {
                  setState(1534);
                  matchedClause();
                }
              }
            }
            setState(1539);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 165, _ctx);
          }
          setState(1543);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == WHEN) {
            {
              {
                setState(1540);
                notMatchedClause();
              }
            }
            setState(1545);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MergeIntoContext extends ParserRuleContext {
    public MultipartIdentifierContext target;
    public TableAliasContext targetAlias;
    public MultipartIdentifierContext source;
    public QueryContext sourceQuery;
    public TableAliasContext sourceAlias;
    public BooleanExpressionContext mergeCondition;

    public TerminalNode MERGE() {
      return getToken(CarbonSqlBaseParser.MERGE, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public List<TableAliasContext> tableAlias() {
      return getRuleContexts(TableAliasContext.class);
    }

    public TableAliasContext tableAlias(int i) {
      return getRuleContext(TableAliasContext.class, i);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public List<MatchedClauseContext> matchedClause() {
      return getRuleContexts(MatchedClauseContext.class);
    }

    public MatchedClauseContext matchedClause(int i) {
      return getRuleContext(MatchedClauseContext.class, i);
    }

    public List<NotMatchedClauseContext> notMatchedClause() {
      return getRuleContexts(NotMatchedClauseContext.class);
    }

    public NotMatchedClauseContext notMatchedClause(int i) {
      return getRuleContext(NotMatchedClauseContext.class, i);
    }

    public MergeIntoContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_mergeInto;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMergeInto(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MergeIntoContext mergeInto() throws RecognitionException {
    MergeIntoContext _localctx = new MergeIntoContext(_ctx, getState());
    enterRule(_localctx, 78, RULE_mergeInto);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1548);
        match(MERGE);
        setState(1549);
        match(INTO);
        setState(1550);
        ((MergeIntoContext) _localctx).target = multipartIdentifier();
        setState(1551);
        ((MergeIntoContext) _localctx).targetAlias = tableAlias();
        setState(1552);
        match(USING);
        setState(1558);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 168, _ctx)) {
          case 1: {
            setState(1553);
            ((MergeIntoContext) _localctx).source = multipartIdentifier();
          }
          break;
          case 2: {
            setState(1554);
            match(T__1);
            setState(1555);
            ((MergeIntoContext) _localctx).sourceQuery = query();
            setState(1556);
            match(T__2);
          }
          break;
        }
        setState(1560);
        ((MergeIntoContext) _localctx).sourceAlias = tableAlias();
        setState(1561);
        match(ON);
        setState(1562);
        ((MergeIntoContext) _localctx).mergeCondition = booleanExpression(0);
        setState(1566);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 169, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1563);
                matchedClause();
              }
            }
          }
          setState(1568);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 169, _ctx);
        }
        setState(1572);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == WHEN) {
          {
            {
              setState(1569);
              notMatchedClause();
            }
          }
          setState(1574);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryOrganizationContext extends ParserRuleContext {
    public SortItemContext sortItem;
    public List<SortItemContext> order = new ArrayList<SortItemContext>();
    public ExpressionContext expression;
    public List<ExpressionContext> clusterBy = new ArrayList<ExpressionContext>();
    public List<ExpressionContext> distributeBy = new ArrayList<ExpressionContext>();
    public List<SortItemContext> sort = new ArrayList<SortItemContext>();
    public ExpressionContext limit;

    public TerminalNode ORDER() {
      return getToken(CarbonSqlBaseParser.ORDER, 0);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public TerminalNode CLUSTER() {
      return getToken(CarbonSqlBaseParser.CLUSTER, 0);
    }

    public TerminalNode DISTRIBUTE() {
      return getToken(CarbonSqlBaseParser.DISTRIBUTE, 0);
    }

    public TerminalNode SORT() {
      return getToken(CarbonSqlBaseParser.SORT, 0);
    }

    public WindowClauseContext windowClause() {
      return getRuleContext(WindowClauseContext.class, 0);
    }

    public TerminalNode LIMIT() {
      return getToken(CarbonSqlBaseParser.LIMIT, 0);
    }

    public List<SortItemContext> sortItem() {
      return getRuleContexts(SortItemContext.class);
    }

    public SortItemContext sortItem(int i) {
      return getRuleContext(SortItemContext.class, i);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode ALL() {
      return getToken(CarbonSqlBaseParser.ALL, 0);
    }

    public QueryOrganizationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryOrganization;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQueryOrganization(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryOrganizationContext queryOrganization() throws RecognitionException {
    QueryOrganizationContext _localctx = new QueryOrganizationContext(_ctx, getState());
    enterRule(_localctx, 80, RULE_queryOrganization);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1585);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 172, _ctx)) {
          case 1: {
            setState(1575);
            match(ORDER);
            setState(1576);
            match(BY);
            setState(1577);
            ((QueryOrganizationContext) _localctx).sortItem = sortItem();
            ((QueryOrganizationContext) _localctx).order
                .add(((QueryOrganizationContext) _localctx).sortItem);
            setState(1582);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 171, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(1578);
                    match(T__3);
                    setState(1579);
                    ((QueryOrganizationContext) _localctx).sortItem = sortItem();
                    ((QueryOrganizationContext) _localctx).order
                        .add(((QueryOrganizationContext) _localctx).sortItem);
                  }
                }
              }
              setState(1584);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 171, _ctx);
            }
          }
          break;
        }
        setState(1597);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 174, _ctx)) {
          case 1: {
            setState(1587);
            match(CLUSTER);
            setState(1588);
            match(BY);
            setState(1589);
            ((QueryOrganizationContext) _localctx).expression = expression();
            ((QueryOrganizationContext) _localctx).clusterBy
                .add(((QueryOrganizationContext) _localctx).expression);
            setState(1594);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 173, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(1590);
                    match(T__3);
                    setState(1591);
                    ((QueryOrganizationContext) _localctx).expression = expression();
                    ((QueryOrganizationContext) _localctx).clusterBy
                        .add(((QueryOrganizationContext) _localctx).expression);
                  }
                }
              }
              setState(1596);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 173, _ctx);
            }
          }
          break;
        }
        setState(1609);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 176, _ctx)) {
          case 1: {
            setState(1599);
            match(DISTRIBUTE);
            setState(1600);
            match(BY);
            setState(1601);
            ((QueryOrganizationContext) _localctx).expression = expression();
            ((QueryOrganizationContext) _localctx).distributeBy
                .add(((QueryOrganizationContext) _localctx).expression);
            setState(1606);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 175, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(1602);
                    match(T__3);
                    setState(1603);
                    ((QueryOrganizationContext) _localctx).expression = expression();
                    ((QueryOrganizationContext) _localctx).distributeBy
                        .add(((QueryOrganizationContext) _localctx).expression);
                  }
                }
              }
              setState(1608);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 175, _ctx);
            }
          }
          break;
        }
        setState(1621);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 178, _ctx)) {
          case 1: {
            setState(1611);
            match(SORT);
            setState(1612);
            match(BY);
            setState(1613);
            ((QueryOrganizationContext) _localctx).sortItem = sortItem();
            ((QueryOrganizationContext) _localctx).sort
                .add(((QueryOrganizationContext) _localctx).sortItem);
            setState(1618);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 177, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(1614);
                    match(T__3);
                    setState(1615);
                    ((QueryOrganizationContext) _localctx).sortItem = sortItem();
                    ((QueryOrganizationContext) _localctx).sort
                        .add(((QueryOrganizationContext) _localctx).sortItem);
                  }
                }
              }
              setState(1620);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 177, _ctx);
            }
          }
          break;
        }
        setState(1624);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 179, _ctx)) {
          case 1: {
            setState(1623);
            windowClause();
          }
          break;
        }
        setState(1631);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 181, _ctx)) {
          case 1: {
            setState(1626);
            match(LIMIT);
            setState(1629);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 180, _ctx)) {
              case 1: {
                setState(1627);
                match(ALL);
              }
              break;
              case 2: {
                setState(1628);
                ((QueryOrganizationContext) _localctx).limit = expression();
              }
              break;
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MultiInsertQueryBodyContext extends ParserRuleContext {
    public InsertIntoContext insertInto() {
      return getRuleContext(InsertIntoContext.class, 0);
    }

    public FromStatementBodyContext fromStatementBody() {
      return getRuleContext(FromStatementBodyContext.class, 0);
    }

    public MultiInsertQueryBodyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_multiInsertQueryBody;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMultiInsertQueryBody(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MultiInsertQueryBodyContext multiInsertQueryBody() throws RecognitionException {
    MultiInsertQueryBodyContext _localctx = new MultiInsertQueryBodyContext(_ctx, getState());
    enterRule(_localctx, 82, RULE_multiInsertQueryBody);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1633);
        insertInto();
        setState(1634);
        fromStatementBody();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QueryTermContext extends ParserRuleContext {
    public QueryTermContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryTerm;
    }

    public QueryTermContext() {
    }

    public void copyFrom(QueryTermContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class QueryTermDefaultContext extends QueryTermContext {
    public QueryPrimaryContext queryPrimary() {
      return getRuleContext(QueryPrimaryContext.class, 0);
    }

    public QueryTermDefaultContext(QueryTermContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQueryTermDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SetOperationContext extends QueryTermContext {
    public QueryTermContext left;
    public Token operator;
    public QueryTermContext right;

    public List<QueryTermContext> queryTerm() {
      return getRuleContexts(QueryTermContext.class);
    }

    public QueryTermContext queryTerm(int i) {
      return getRuleContext(QueryTermContext.class, i);
    }

    public TerminalNode INTERSECT() {
      return getToken(CarbonSqlBaseParser.INTERSECT, 0);
    }

    public TerminalNode UNION() {
      return getToken(CarbonSqlBaseParser.UNION, 0);
    }

    public TerminalNode EXCEPT() {
      return getToken(CarbonSqlBaseParser.EXCEPT, 0);
    }

    public TerminalNode SETMINUS() {
      return getToken(CarbonSqlBaseParser.SETMINUS, 0);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public SetOperationContext(QueryTermContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetOperation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryTermContext queryTerm() throws RecognitionException {
    return queryTerm(0);
  }

  private QueryTermContext queryTerm(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    QueryTermContext _localctx = new QueryTermContext(_ctx, _parentState);
    QueryTermContext _prevctx = _localctx;
    int _startState = 84;
    enterRecursionRule(_localctx, 84, RULE_queryTerm, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        {
          _localctx = new QueryTermDefaultContext(_localctx);
          _ctx = _localctx;
          _prevctx = _localctx;

          setState(1637);
          queryPrimary();
        }
        _ctx.stop = _input.LT(-1);
        setState(1662);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 186, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(1660);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 185, _ctx)) {
                case 1: {
                  _localctx =
                      new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
                  ((SetOperationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
                  setState(1639);
                  if (!(precpred(_ctx, 3)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                  setState(1640);
                  if (!(legacy_setops_precedence_enbled))
                    throw new FailedPredicateException(this, "legacy_setops_precedence_enbled");
                  setState(1641);
                  ((SetOperationContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(_la == EXCEPT || _la == INTERSECT || _la == SETMINUS || _la == UNION)) {
                    ((SetOperationContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(1643);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (_la == ALL || _la == DISTINCT) {
                    {
                      setState(1642);
                      setQuantifier();
                    }
                  }

                  setState(1645);
                  ((SetOperationContext) _localctx).right = queryTerm(4);
                }
                break;
                case 2: {
                  _localctx =
                      new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
                  ((SetOperationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
                  setState(1646);
                  if (!(precpred(_ctx, 2)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  setState(1647);
                  if (!(!legacy_setops_precedence_enbled))
                    throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
                  setState(1648);
                  ((SetOperationContext) _localctx).operator = match(INTERSECT);
                  setState(1650);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (_la == ALL || _la == DISTINCT) {
                    {
                      setState(1649);
                      setQuantifier();
                    }
                  }

                  setState(1652);
                  ((SetOperationContext) _localctx).right = queryTerm(3);
                }
                break;
                case 3: {
                  _localctx =
                      new SetOperationContext(new QueryTermContext(_parentctx, _parentState));
                  ((SetOperationContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_queryTerm);
                  setState(1653);
                  if (!(precpred(_ctx, 1)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  setState(1654);
                  if (!(!legacy_setops_precedence_enbled))
                    throw new FailedPredicateException(this, "!legacy_setops_precedence_enbled");
                  setState(1655);
                  ((SetOperationContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(_la == EXCEPT || _la == SETMINUS || _la == UNION)) {
                    ((SetOperationContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(1657);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  if (_la == ALL || _la == DISTINCT) {
                    {
                      setState(1656);
                      setQuantifier();
                    }
                  }

                  setState(1659);
                  ((SetOperationContext) _localctx).right = queryTerm(2);
                }
                break;
              }
            }
          }
          setState(1664);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 186, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class QueryPrimaryContext extends ParserRuleContext {
    public QueryPrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_queryPrimary;
    }

    public QueryPrimaryContext() {
    }

    public void copyFrom(QueryPrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SubqueryContext extends QueryPrimaryContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public SubqueryContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSubquery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class QueryPrimaryDefaultContext extends QueryPrimaryContext {
    public QuerySpecificationContext querySpecification() {
      return getRuleContext(QuerySpecificationContext.class, 0);
    }

    public QueryPrimaryDefaultContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQueryPrimaryDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InlineTableDefault1Context extends QueryPrimaryContext {
    public InlineTableContext inlineTable() {
      return getRuleContext(InlineTableContext.class, 0);
    }

    public InlineTableDefault1Context(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInlineTableDefault1(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FromStmtContext extends QueryPrimaryContext {
    public FromStatementContext fromStatement() {
      return getRuleContext(FromStatementContext.class, 0);
    }

    public FromStmtContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFromStmt(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TableContext extends QueryPrimaryContext {
    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TableContext(QueryPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QueryPrimaryContext queryPrimary() throws RecognitionException {
    QueryPrimaryContext _localctx = new QueryPrimaryContext(_ctx, getState());
    enterRule(_localctx, 86, RULE_queryPrimary);
    try {
      setState(1674);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case MAP:
        case REDUCE:
        case SELECT:
          _localctx = new QueryPrimaryDefaultContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1665);
          querySpecification();
        }
        break;
        case FROM:
          _localctx = new FromStmtContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1666);
          fromStatement();
        }
        break;
        case TABLE:
          _localctx = new TableContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(1667);
          match(TABLE);
          setState(1668);
          multipartIdentifier();
        }
        break;
        case VALUES:
          _localctx = new InlineTableDefault1Context(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(1669);
          inlineTable();
        }
        break;
        case T__1:
          _localctx = new SubqueryContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(1670);
          match(T__1);
          setState(1671);
          query();
          setState(1672);
          match(T__2);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SortItemContext extends ParserRuleContext {
    public Token ordering;
    public Token nullOrder;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode NULLS() {
      return getToken(CarbonSqlBaseParser.NULLS, 0);
    }

    public TerminalNode ASC() {
      return getToken(CarbonSqlBaseParser.ASC, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode LAST() {
      return getToken(CarbonSqlBaseParser.LAST, 0);
    }

    public TerminalNode FIRST() {
      return getToken(CarbonSqlBaseParser.FIRST, 0);
    }

    public SortItemContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_sortItem;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSortItem(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SortItemContext sortItem() throws RecognitionException {
    SortItemContext _localctx = new SortItemContext(_ctx, getState());
    enterRule(_localctx, 88, RULE_sortItem);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1676);
        expression();
        setState(1678);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 188, _ctx)) {
          case 1: {
            setState(1677);
            ((SortItemContext) _localctx).ordering = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == ASC || _la == DESC)) {
              ((SortItemContext) _localctx).ordering = (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
        }
        setState(1682);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 189, _ctx)) {
          case 1: {
            setState(1680);
            match(NULLS);
            setState(1681);
            ((SortItemContext) _localctx).nullOrder = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == FIRST || _la == LAST)) {
              ((SortItemContext) _localctx).nullOrder = (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FromStatementContext extends ParserRuleContext {
    public FromClauseContext fromClause() {
      return getRuleContext(FromClauseContext.class, 0);
    }

    public List<FromStatementBodyContext> fromStatementBody() {
      return getRuleContexts(FromStatementBodyContext.class);
    }

    public FromStatementBodyContext fromStatementBody(int i) {
      return getRuleContext(FromStatementBodyContext.class, i);
    }

    public FromStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fromStatement;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFromStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromStatementContext fromStatement() throws RecognitionException {
    FromStatementContext _localctx = new FromStatementContext(_ctx, getState());
    enterRule(_localctx, 90, RULE_fromStatement);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1684);
        fromClause();
        setState(1686);
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
            case 1: {
              {
                setState(1685);
                fromStatementBody();
              }
            }
            break;
            default:
              throw new NoViableAltException(this);
          }
          setState(1688);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 190, _ctx);
        } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FromStatementBodyContext extends ParserRuleContext {
    public TransformClauseContext transformClause() {
      return getRuleContext(TransformClauseContext.class, 0);
    }

    public QueryOrganizationContext queryOrganization() {
      return getRuleContext(QueryOrganizationContext.class, 0);
    }

    public WhereClauseContext whereClause() {
      return getRuleContext(WhereClauseContext.class, 0);
    }

    public SelectClauseContext selectClause() {
      return getRuleContext(SelectClauseContext.class, 0);
    }

    public List<LateralViewContext> lateralView() {
      return getRuleContexts(LateralViewContext.class);
    }

    public LateralViewContext lateralView(int i) {
      return getRuleContext(LateralViewContext.class, i);
    }

    public AggregationClauseContext aggregationClause() {
      return getRuleContext(AggregationClauseContext.class, 0);
    }

    public HavingClauseContext havingClause() {
      return getRuleContext(HavingClauseContext.class, 0);
    }

    public WindowClauseContext windowClause() {
      return getRuleContext(WindowClauseContext.class, 0);
    }

    public FromStatementBodyContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fromStatementBody;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFromStatementBody(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromStatementBodyContext fromStatementBody() throws RecognitionException {
    FromStatementBodyContext _localctx = new FromStatementBodyContext(_ctx, getState());
    enterRule(_localctx, 92, RULE_fromStatementBody);
    try {
      int _alt;
      setState(1717);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 197, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1690);
          transformClause();
          setState(1692);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 191, _ctx)) {
            case 1: {
              setState(1691);
              whereClause();
            }
            break;
          }
          setState(1694);
          queryOrganization();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1696);
          selectClause();
          setState(1700);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 192, _ctx);
          while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1) {
              {
                {
                  setState(1697);
                  lateralView();
                }
              }
            }
            setState(1702);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 192, _ctx);
          }
          setState(1704);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 193, _ctx)) {
            case 1: {
              setState(1703);
              whereClause();
            }
            break;
          }
          setState(1707);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 194, _ctx)) {
            case 1: {
              setState(1706);
              aggregationClause();
            }
            break;
          }
          setState(1710);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 195, _ctx)) {
            case 1: {
              setState(1709);
              havingClause();
            }
            break;
          }
          setState(1713);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 196, _ctx)) {
            case 1: {
              setState(1712);
              windowClause();
            }
            break;
          }
          setState(1715);
          queryOrganization();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QuerySpecificationContext extends ParserRuleContext {
    public QuerySpecificationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_querySpecification;
    }

    public QuerySpecificationContext() {
    }

    public void copyFrom(QuerySpecificationContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class RegularQuerySpecificationContext extends QuerySpecificationContext {
    public SelectClauseContext selectClause() {
      return getRuleContext(SelectClauseContext.class, 0);
    }

    public FromClauseContext fromClause() {
      return getRuleContext(FromClauseContext.class, 0);
    }

    public List<LateralViewContext> lateralView() {
      return getRuleContexts(LateralViewContext.class);
    }

    public LateralViewContext lateralView(int i) {
      return getRuleContext(LateralViewContext.class, i);
    }

    public WhereClauseContext whereClause() {
      return getRuleContext(WhereClauseContext.class, 0);
    }

    public AggregationClauseContext aggregationClause() {
      return getRuleContext(AggregationClauseContext.class, 0);
    }

    public HavingClauseContext havingClause() {
      return getRuleContext(HavingClauseContext.class, 0);
    }

    public WindowClauseContext windowClause() {
      return getRuleContext(WindowClauseContext.class, 0);
    }

    public RegularQuerySpecificationContext(QuerySpecificationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRegularQuerySpecification(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TransformQuerySpecificationContext extends QuerySpecificationContext {
    public TransformClauseContext transformClause() {
      return getRuleContext(TransformClauseContext.class, 0);
    }

    public FromClauseContext fromClause() {
      return getRuleContext(FromClauseContext.class, 0);
    }

    public WhereClauseContext whereClause() {
      return getRuleContext(WhereClauseContext.class, 0);
    }

    public TransformQuerySpecificationContext(QuerySpecificationContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTransformQuerySpecification(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QuerySpecificationContext querySpecification() throws RecognitionException {
    QuerySpecificationContext _localctx = new QuerySpecificationContext(_ctx, getState());
    enterRule(_localctx, 94, RULE_querySpecification);
    try {
      int _alt;
      setState(1748);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 206, _ctx)) {
        case 1:
          _localctx = new TransformQuerySpecificationContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(1719);
          transformClause();
          setState(1721);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 198, _ctx)) {
            case 1: {
              setState(1720);
              fromClause();
            }
            break;
          }
          setState(1724);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 199, _ctx)) {
            case 1: {
              setState(1723);
              whereClause();
            }
            break;
          }
        }
        break;
        case 2:
          _localctx = new RegularQuerySpecificationContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(1726);
          selectClause();
          setState(1728);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 200, _ctx)) {
            case 1: {
              setState(1727);
              fromClause();
            }
            break;
          }
          setState(1733);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 201, _ctx);
          while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1) {
              {
                {
                  setState(1730);
                  lateralView();
                }
              }
            }
            setState(1735);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 201, _ctx);
          }
          setState(1737);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 202, _ctx)) {
            case 1: {
              setState(1736);
              whereClause();
            }
            break;
          }
          setState(1740);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 203, _ctx)) {
            case 1: {
              setState(1739);
              aggregationClause();
            }
            break;
          }
          setState(1743);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 204, _ctx)) {
            case 1: {
              setState(1742);
              havingClause();
            }
            break;
          }
          setState(1746);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 205, _ctx)) {
            case 1: {
              setState(1745);
              windowClause();
            }
            break;
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TransformClauseContext extends ParserRuleContext {
    public Token kind;
    public RowFormatContext inRowFormat;
    public Token recordWriter;
    public Token script;
    public RowFormatContext outRowFormat;
    public Token recordReader;

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public List<TerminalNode> STRING() {
      return getTokens(CarbonSqlBaseParser.STRING);
    }

    public TerminalNode STRING(int i) {
      return getToken(CarbonSqlBaseParser.STRING, i);
    }

    public TerminalNode SELECT() {
      return getToken(CarbonSqlBaseParser.SELECT, 0);
    }

    public NamedExpressionSeqContext namedExpressionSeq() {
      return getRuleContext(NamedExpressionSeqContext.class, 0);
    }

    public TerminalNode TRANSFORM() {
      return getToken(CarbonSqlBaseParser.TRANSFORM, 0);
    }

    public TerminalNode MAP() {
      return getToken(CarbonSqlBaseParser.MAP, 0);
    }

    public TerminalNode REDUCE() {
      return getToken(CarbonSqlBaseParser.REDUCE, 0);
    }

    public TerminalNode RECORDWRITER() {
      return getToken(CarbonSqlBaseParser.RECORDWRITER, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode RECORDREADER() {
      return getToken(CarbonSqlBaseParser.RECORDREADER, 0);
    }

    public List<RowFormatContext> rowFormat() {
      return getRuleContexts(RowFormatContext.class);
    }

    public RowFormatContext rowFormat(int i) {
      return getRuleContext(RowFormatContext.class, i);
    }

    public IdentifierSeqContext identifierSeq() {
      return getRuleContext(IdentifierSeqContext.class, 0);
    }

    public ColTypeListContext colTypeList() {
      return getRuleContext(ColTypeListContext.class, 0);
    }

    public TransformClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transformClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTransformClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TransformClauseContext transformClause() throws RecognitionException {
    TransformClauseContext _localctx = new TransformClauseContext(_ctx, getState());
    enterRule(_localctx, 96, RULE_transformClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1760);
        _errHandler.sync(this);
        switch (_input.LA(1)) {
          case SELECT: {
            setState(1750);
            match(SELECT);
            setState(1751);
            ((TransformClauseContext) _localctx).kind = match(TRANSFORM);
            setState(1752);
            match(T__1);
            setState(1753);
            namedExpressionSeq();
            setState(1754);
            match(T__2);
          }
          break;
          case MAP: {
            setState(1756);
            ((TransformClauseContext) _localctx).kind = match(MAP);
            setState(1757);
            namedExpressionSeq();
          }
          break;
          case REDUCE: {
            setState(1758);
            ((TransformClauseContext) _localctx).kind = match(REDUCE);
            setState(1759);
            namedExpressionSeq();
          }
          break;
          default:
            throw new NoViableAltException(this);
        }
        setState(1763);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == ROW) {
          {
            setState(1762);
            ((TransformClauseContext) _localctx).inRowFormat = rowFormat();
          }
        }

        setState(1767);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == RECORDWRITER) {
          {
            setState(1765);
            match(RECORDWRITER);
            setState(1766);
            ((TransformClauseContext) _localctx).recordWriter = match(STRING);
          }
        }

        setState(1769);
        match(USING);
        setState(1770);
        ((TransformClauseContext) _localctx).script = match(STRING);
        setState(1783);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 212, _ctx)) {
          case 1: {
            setState(1771);
            match(AS);
            setState(1781);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 211, _ctx)) {
              case 1: {
                setState(1772);
                identifierSeq();
              }
              break;
              case 2: {
                setState(1773);
                colTypeList();
              }
              break;
              case 3: {
                {
                  setState(1774);
                  match(T__1);
                  setState(1777);
                  _errHandler.sync(this);
                  switch (getInterpreter().adaptivePredict(_input, 210, _ctx)) {
                    case 1: {
                      setState(1775);
                      identifierSeq();
                    }
                    break;
                    case 2: {
                      setState(1776);
                      colTypeList();
                    }
                    break;
                  }
                  setState(1779);
                  match(T__2);
                }
              }
              break;
            }
          }
          break;
        }
        setState(1786);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 213, _ctx)) {
          case 1: {
            setState(1785);
            ((TransformClauseContext) _localctx).outRowFormat = rowFormat();
          }
          break;
        }
        setState(1790);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 214, _ctx)) {
          case 1: {
            setState(1788);
            match(RECORDREADER);
            setState(1789);
            ((TransformClauseContext) _localctx).recordReader = match(STRING);
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SelectClauseContext extends ParserRuleContext {
    public HintContext hint;
    public List<HintContext> hints = new ArrayList<HintContext>();

    public TerminalNode SELECT() {
      return getToken(CarbonSqlBaseParser.SELECT, 0);
    }

    public NamedExpressionSeqContext namedExpressionSeq() {
      return getRuleContext(NamedExpressionSeqContext.class, 0);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public List<HintContext> hint() {
      return getRuleContexts(HintContext.class);
    }

    public HintContext hint(int i) {
      return getRuleContext(HintContext.class, i);
    }

    public SelectClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_selectClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSelectClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SelectClauseContext selectClause() throws RecognitionException {
    SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
    enterRule(_localctx, 98, RULE_selectClause);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1792);
        match(SELECT);
        setState(1796);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 215, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1793);
                ((SelectClauseContext) _localctx).hint = hint();
                ((SelectClauseContext) _localctx).hints.add(((SelectClauseContext) _localctx).hint);
              }
            }
          }
          setState(1798);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 215, _ctx);
        }
        setState(1800);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 216, _ctx)) {
          case 1: {
            setState(1799);
            setQuantifier();
          }
          break;
        }
        setState(1802);
        namedExpressionSeq();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SetClauseContext extends ParserRuleContext {
    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public AssignmentListContext assignmentList() {
      return getRuleContext(AssignmentListContext.class, 0);
    }

    public SetClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_setClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SetClauseContext setClause() throws RecognitionException {
    SetClauseContext _localctx = new SetClauseContext(_ctx, getState());
    enterRule(_localctx, 100, RULE_setClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1804);
        match(SET);
        setState(1805);
        assignmentList();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MatchedClauseContext extends ParserRuleContext {
    public BooleanExpressionContext matchedCond;

    public TerminalNode WHEN() {
      return getToken(CarbonSqlBaseParser.WHEN, 0);
    }

    public TerminalNode MATCHED() {
      return getToken(CarbonSqlBaseParser.MATCHED, 0);
    }

    public TerminalNode THEN() {
      return getToken(CarbonSqlBaseParser.THEN, 0);
    }

    public MatchedActionContext matchedAction() {
      return getRuleContext(MatchedActionContext.class, 0);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public MatchedClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_matchedClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMatchedClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchedClauseContext matchedClause() throws RecognitionException {
    MatchedClauseContext _localctx = new MatchedClauseContext(_ctx, getState());
    enterRule(_localctx, 102, RULE_matchedClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1807);
        match(WHEN);
        setState(1808);
        match(MATCHED);
        setState(1811);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == AND) {
          {
            setState(1809);
            match(AND);
            setState(1810);
            ((MatchedClauseContext) _localctx).matchedCond = booleanExpression(0);
          }
        }

        setState(1813);
        match(THEN);
        setState(1814);
        matchedAction();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NotMatchedClauseContext extends ParserRuleContext {
    public BooleanExpressionContext notMatchedCond;

    public TerminalNode WHEN() {
      return getToken(CarbonSqlBaseParser.WHEN, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode MATCHED() {
      return getToken(CarbonSqlBaseParser.MATCHED, 0);
    }

    public TerminalNode THEN() {
      return getToken(CarbonSqlBaseParser.THEN, 0);
    }

    public NotMatchedActionContext notMatchedAction() {
      return getRuleContext(NotMatchedActionContext.class, 0);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public NotMatchedClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_notMatchedClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNotMatchedClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NotMatchedClauseContext notMatchedClause() throws RecognitionException {
    NotMatchedClauseContext _localctx = new NotMatchedClauseContext(_ctx, getState());
    enterRule(_localctx, 104, RULE_notMatchedClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1816);
        match(WHEN);
        setState(1817);
        match(NOT);
        setState(1818);
        match(MATCHED);
        setState(1821);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == AND) {
          {
            setState(1819);
            match(AND);
            setState(1820);
            ((NotMatchedClauseContext) _localctx).notMatchedCond = booleanExpression(0);
          }
        }

        setState(1823);
        match(THEN);
        setState(1824);
        notMatchedAction();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MatchedActionContext extends ParserRuleContext {
    public TerminalNode DELETE() {
      return getToken(CarbonSqlBaseParser.DELETE, 0);
    }

    public TerminalNode UPDATE() {
      return getToken(CarbonSqlBaseParser.UPDATE, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode ASTERISK() {
      return getToken(CarbonSqlBaseParser.ASTERISK, 0);
    }

    public AssignmentListContext assignmentList() {
      return getRuleContext(AssignmentListContext.class, 0);
    }

    public MatchedActionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_matchedAction;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMatchedAction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MatchedActionContext matchedAction() throws RecognitionException {
    MatchedActionContext _localctx = new MatchedActionContext(_ctx, getState());
    enterRule(_localctx, 106, RULE_matchedAction);
    try {
      setState(1833);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 219, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1826);
          match(DELETE);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1827);
          match(UPDATE);
          setState(1828);
          match(SET);
          setState(1829);
          match(ASTERISK);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(1830);
          match(UPDATE);
          setState(1831);
          match(SET);
          setState(1832);
          assignmentList();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NotMatchedActionContext extends ParserRuleContext {
    public MultipartIdentifierListContext columns;

    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode ASTERISK() {
      return getToken(CarbonSqlBaseParser.ASTERISK, 0);
    }

    public TerminalNode VALUES() {
      return getToken(CarbonSqlBaseParser.VALUES, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public MultipartIdentifierListContext multipartIdentifierList() {
      return getRuleContext(MultipartIdentifierListContext.class, 0);
    }

    public NotMatchedActionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_notMatchedAction;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNotMatchedAction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NotMatchedActionContext notMatchedAction() throws RecognitionException {
    NotMatchedActionContext _localctx = new NotMatchedActionContext(_ctx, getState());
    enterRule(_localctx, 108, RULE_notMatchedAction);
    int _la;
    try {
      setState(1853);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 221, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1835);
          match(INSERT);
          setState(1836);
          match(ASTERISK);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1837);
          match(INSERT);
          setState(1838);
          match(T__1);
          setState(1839);
          ((NotMatchedActionContext) _localctx).columns = multipartIdentifierList();
          setState(1840);
          match(T__2);
          setState(1841);
          match(VALUES);
          setState(1842);
          match(T__1);
          setState(1843);
          expression();
          setState(1848);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(1844);
                match(T__3);
                setState(1845);
                expression();
              }
            }
            setState(1850);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(1851);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AssignmentListContext extends ParserRuleContext {
    public List<AssignmentContext> assignment() {
      return getRuleContexts(AssignmentContext.class);
    }

    public AssignmentContext assignment(int i) {
      return getRuleContext(AssignmentContext.class, i);
    }

    public AssignmentListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_assignmentList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAssignmentList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AssignmentListContext assignmentList() throws RecognitionException {
    AssignmentListContext _localctx = new AssignmentListContext(_ctx, getState());
    enterRule(_localctx, 110, RULE_assignmentList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1855);
        assignment();
        setState(1860);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1856);
              match(T__3);
              setState(1857);
              assignment();
            }
          }
          setState(1862);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AssignmentContext extends ParserRuleContext {
    public MultipartIdentifierContext key;
    public ExpressionContext value;

    public TerminalNode EQ() {
      return getToken(CarbonSqlBaseParser.EQ, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public AssignmentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_assignment;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAssignment(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AssignmentContext assignment() throws RecognitionException {
    AssignmentContext _localctx = new AssignmentContext(_ctx, getState());
    enterRule(_localctx, 112, RULE_assignment);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1863);
        ((AssignmentContext) _localctx).key = multipartIdentifier();
        setState(1864);
        match(EQ);
        setState(1865);
        ((AssignmentContext) _localctx).value = expression();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WhereClauseContext extends ParserRuleContext {
    public TerminalNode WHERE() {
      return getToken(CarbonSqlBaseParser.WHERE, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public WhereClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_whereClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWhereClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhereClauseContext whereClause() throws RecognitionException {
    WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
    enterRule(_localctx, 114, RULE_whereClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1867);
        match(WHERE);
        setState(1868);
        booleanExpression(0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class HavingClauseContext extends ParserRuleContext {
    public TerminalNode HAVING() {
      return getToken(CarbonSqlBaseParser.HAVING, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public HavingClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_havingClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitHavingClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final HavingClauseContext havingClause() throws RecognitionException {
    HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
    enterRule(_localctx, 116, RULE_havingClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1870);
        match(HAVING);
        setState(1871);
        booleanExpression(0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class HintContext extends ParserRuleContext {
    public HintStatementContext hintStatement;
    public List<HintStatementContext> hintStatements = new ArrayList<HintStatementContext>();

    public List<HintStatementContext> hintStatement() {
      return getRuleContexts(HintStatementContext.class);
    }

    public HintStatementContext hintStatement(int i) {
      return getRuleContext(HintStatementContext.class, i);
    }

    public HintContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_hint;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitHint(this);
      else return visitor.visitChildren(this);
    }
  }

  public final HintContext hint() throws RecognitionException {
    HintContext _localctx = new HintContext(_ctx, getState());
    enterRule(_localctx, 118, RULE_hint);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1873);
        match(T__5);
        setState(1874);
        ((HintContext) _localctx).hintStatement = hintStatement();
        ((HintContext) _localctx).hintStatements.add(((HintContext) _localctx).hintStatement);
        setState(1881);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 224, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1876);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 223, _ctx)) {
                  case 1: {
                    setState(1875);
                    match(T__3);
                  }
                  break;
                }
                setState(1878);
                ((HintContext) _localctx).hintStatement = hintStatement();
                ((HintContext) _localctx).hintStatements
                    .add(((HintContext) _localctx).hintStatement);
              }
            }
          }
          setState(1883);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 224, _ctx);
        }
        setState(1884);
        match(T__6);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class HintStatementContext extends ParserRuleContext {
    public IdentifierContext hintName;
    public PrimaryExpressionContext primaryExpression;
    public List<PrimaryExpressionContext> parameters = new ArrayList<PrimaryExpressionContext>();

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public List<PrimaryExpressionContext> primaryExpression() {
      return getRuleContexts(PrimaryExpressionContext.class);
    }

    public PrimaryExpressionContext primaryExpression(int i) {
      return getRuleContext(PrimaryExpressionContext.class, i);
    }

    public HintStatementContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_hintStatement;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitHintStatement(this);
      else return visitor.visitChildren(this);
    }
  }

  public final HintStatementContext hintStatement() throws RecognitionException {
    HintStatementContext _localctx = new HintStatementContext(_ctx, getState());
    enterRule(_localctx, 120, RULE_hintStatement);
    int _la;
    try {
      setState(1899);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 226, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1886);
          ((HintStatementContext) _localctx).hintName = identifier();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1887);
          ((HintStatementContext) _localctx).hintName = identifier();
          setState(1888);
          match(T__1);
          setState(1889);
          ((HintStatementContext) _localctx).primaryExpression = primaryExpression(0);
          ((HintStatementContext) _localctx).parameters
              .add(((HintStatementContext) _localctx).primaryExpression);
          setState(1894);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(1890);
                match(T__3);
                setState(1891);
                ((HintStatementContext) _localctx).primaryExpression = primaryExpression(0);
                ((HintStatementContext) _localctx).parameters
                    .add(((HintStatementContext) _localctx).primaryExpression);
              }
            }
            setState(1896);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(1897);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FromClauseContext extends ParserRuleContext {
    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public List<RelationContext> relation() {
      return getRuleContexts(RelationContext.class);
    }

    public RelationContext relation(int i) {
      return getRuleContext(RelationContext.class, i);
    }

    public List<LateralViewContext> lateralView() {
      return getRuleContexts(LateralViewContext.class);
    }

    public LateralViewContext lateralView(int i) {
      return getRuleContext(LateralViewContext.class, i);
    }

    public PivotClauseContext pivotClause() {
      return getRuleContext(PivotClauseContext.class, 0);
    }

    public FromClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_fromClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFromClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FromClauseContext fromClause() throws RecognitionException {
    FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
    enterRule(_localctx, 122, RULE_fromClause);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(1901);
        match(FROM);
        setState(1902);
        relation();
        setState(1907);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 227, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1903);
                match(T__3);
                setState(1904);
                relation();
              }
            }
          }
          setState(1909);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 227, _ctx);
        }
        setState(1913);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 228, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(1910);
                lateralView();
              }
            }
          }
          setState(1915);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 228, _ctx);
        }
        setState(1917);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 229, _ctx)) {
          case 1: {
            setState(1916);
            pivotClause();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AggregationClauseContext extends ParserRuleContext {
    public ExpressionContext expression;
    public List<ExpressionContext> groupingExpressions = new ArrayList<ExpressionContext>();
    public Token kind;

    public TerminalNode GROUP() {
      return getToken(CarbonSqlBaseParser.GROUP, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public TerminalNode SETS() {
      return getToken(CarbonSqlBaseParser.SETS, 0);
    }

    public List<GroupingSetContext> groupingSet() {
      return getRuleContexts(GroupingSetContext.class);
    }

    public GroupingSetContext groupingSet(int i) {
      return getRuleContext(GroupingSetContext.class, i);
    }

    public TerminalNode ROLLUP() {
      return getToken(CarbonSqlBaseParser.ROLLUP, 0);
    }

    public TerminalNode CUBE() {
      return getToken(CarbonSqlBaseParser.CUBE, 0);
    }

    public TerminalNode GROUPING() {
      return getToken(CarbonSqlBaseParser.GROUPING, 0);
    }

    public AggregationClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_aggregationClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAggregationClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AggregationClauseContext aggregationClause() throws RecognitionException {
    AggregationClauseContext _localctx = new AggregationClauseContext(_ctx, getState());
    enterRule(_localctx, 124, RULE_aggregationClause);
    int _la;
    try {
      int _alt;
      setState(1963);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 234, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1919);
          match(GROUP);
          setState(1920);
          match(BY);
          setState(1921);
          ((AggregationClauseContext) _localctx).expression = expression();
          ((AggregationClauseContext) _localctx).groupingExpressions
              .add(((AggregationClauseContext) _localctx).expression);
          setState(1926);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 230, _ctx);
          while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
            if (_alt == 1) {
              {
                {
                  setState(1922);
                  match(T__3);
                  setState(1923);
                  ((AggregationClauseContext) _localctx).expression = expression();
                  ((AggregationClauseContext) _localctx).groupingExpressions
                      .add(((AggregationClauseContext) _localctx).expression);
                }
              }
            }
            setState(1928);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 230, _ctx);
          }
          setState(1946);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 232, _ctx)) {
            case 1: {
              setState(1929);
              match(WITH);
              setState(1930);
              ((AggregationClauseContext) _localctx).kind = match(ROLLUP);
            }
            break;
            case 2: {
              setState(1931);
              match(WITH);
              setState(1932);
              ((AggregationClauseContext) _localctx).kind = match(CUBE);
            }
            break;
            case 3: {
              setState(1933);
              ((AggregationClauseContext) _localctx).kind = match(GROUPING);
              setState(1934);
              match(SETS);
              setState(1935);
              match(T__1);
              setState(1936);
              groupingSet();
              setState(1941);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(1937);
                    match(T__3);
                    setState(1938);
                    groupingSet();
                  }
                }
                setState(1943);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
              setState(1944);
              match(T__2);
            }
            break;
          }
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1948);
          match(GROUP);
          setState(1949);
          match(BY);
          setState(1950);
          ((AggregationClauseContext) _localctx).kind = match(GROUPING);
          setState(1951);
          match(SETS);
          setState(1952);
          match(T__1);
          setState(1953);
          groupingSet();
          setState(1958);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(1954);
                match(T__3);
                setState(1955);
                groupingSet();
              }
            }
            setState(1960);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(1961);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class GroupingSetContext extends ParserRuleContext {
    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public GroupingSetContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_groupingSet;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitGroupingSet(this);
      else return visitor.visitChildren(this);
    }
  }

  public final GroupingSetContext groupingSet() throws RecognitionException {
    GroupingSetContext _localctx = new GroupingSetContext(_ctx, getState());
    enterRule(_localctx, 126, RULE_groupingSet);
    int _la;
    try {
      setState(1978);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 237, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1965);
          match(T__1);
          setState(1974);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 236, _ctx)) {
            case 1: {
              setState(1966);
              expression();
              setState(1971);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(1967);
                    match(T__3);
                    setState(1968);
                    expression();
                  }
                }
                setState(1973);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
            break;
          }
          setState(1976);
          match(T__2);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1977);
          expression();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PivotClauseContext extends ParserRuleContext {
    public NamedExpressionSeqContext aggregates;
    public PivotValueContext pivotValue;
    public List<PivotValueContext> pivotValues = new ArrayList<PivotValueContext>();

    public TerminalNode PIVOT() {
      return getToken(CarbonSqlBaseParser.PIVOT, 0);
    }

    public TerminalNode FOR() {
      return getToken(CarbonSqlBaseParser.FOR, 0);
    }

    public PivotColumnContext pivotColumn() {
      return getRuleContext(PivotColumnContext.class, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public NamedExpressionSeqContext namedExpressionSeq() {
      return getRuleContext(NamedExpressionSeqContext.class, 0);
    }

    public List<PivotValueContext> pivotValue() {
      return getRuleContexts(PivotValueContext.class);
    }

    public PivotValueContext pivotValue(int i) {
      return getRuleContext(PivotValueContext.class, i);
    }

    public PivotClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_pivotClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPivotClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PivotClauseContext pivotClause() throws RecognitionException {
    PivotClauseContext _localctx = new PivotClauseContext(_ctx, getState());
    enterRule(_localctx, 128, RULE_pivotClause);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(1980);
        match(PIVOT);
        setState(1981);
        match(T__1);
        setState(1982);
        ((PivotClauseContext) _localctx).aggregates = namedExpressionSeq();
        setState(1983);
        match(FOR);
        setState(1984);
        pivotColumn();
        setState(1985);
        match(IN);
        setState(1986);
        match(T__1);
        setState(1987);
        ((PivotClauseContext) _localctx).pivotValue = pivotValue();
        ((PivotClauseContext) _localctx).pivotValues
            .add(((PivotClauseContext) _localctx).pivotValue);
        setState(1992);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(1988);
              match(T__3);
              setState(1989);
              ((PivotClauseContext) _localctx).pivotValue = pivotValue();
              ((PivotClauseContext) _localctx).pivotValues
                  .add(((PivotClauseContext) _localctx).pivotValue);
            }
          }
          setState(1994);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(1995);
        match(T__2);
        setState(1996);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PivotColumnContext extends ParserRuleContext {
    public IdentifierContext identifier;
    public List<IdentifierContext> identifiers = new ArrayList<IdentifierContext>();

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public PivotColumnContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_pivotColumn;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPivotColumn(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PivotColumnContext pivotColumn() throws RecognitionException {
    PivotColumnContext _localctx = new PivotColumnContext(_ctx, getState());
    enterRule(_localctx, 130, RULE_pivotColumn);
    int _la;
    try {
      setState(2010);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 240, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(1998);
          ((PivotColumnContext) _localctx).identifier = identifier();
          ((PivotColumnContext) _localctx).identifiers
              .add(((PivotColumnContext) _localctx).identifier);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(1999);
          match(T__1);
          setState(2000);
          ((PivotColumnContext) _localctx).identifier = identifier();
          ((PivotColumnContext) _localctx).identifiers
              .add(((PivotColumnContext) _localctx).identifier);
          setState(2005);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(2001);
                match(T__3);
                setState(2002);
                ((PivotColumnContext) _localctx).identifier = identifier();
                ((PivotColumnContext) _localctx).identifiers
                    .add(((PivotColumnContext) _localctx).identifier);
              }
            }
            setState(2007);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(2008);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PivotValueContext extends ParserRuleContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public PivotValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_pivotValue;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPivotValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PivotValueContext pivotValue() throws RecognitionException {
    PivotValueContext _localctx = new PivotValueContext(_ctx, getState());
    enterRule(_localctx, 132, RULE_pivotValue);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2012);
        expression();
        setState(2017);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 242, _ctx)) {
          case 1: {
            setState(2014);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 241, _ctx)) {
              case 1: {
                setState(2013);
                match(AS);
              }
              break;
            }
            setState(2016);
            identifier();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class LateralViewContext extends ParserRuleContext {
    public IdentifierContext tblName;
    public IdentifierContext identifier;
    public List<IdentifierContext> colName = new ArrayList<IdentifierContext>();

    public TerminalNode LATERAL() {
      return getToken(CarbonSqlBaseParser.LATERAL, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public TerminalNode OUTER() {
      return getToken(CarbonSqlBaseParser.OUTER, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public LateralViewContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_lateralView;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLateralView(this);
      else return visitor.visitChildren(this);
    }
  }

  public final LateralViewContext lateralView() throws RecognitionException {
    LateralViewContext _localctx = new LateralViewContext(_ctx, getState());
    enterRule(_localctx, 134, RULE_lateralView);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2019);
        match(LATERAL);
        setState(2020);
        match(VIEW);
        setState(2022);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 243, _ctx)) {
          case 1: {
            setState(2021);
            match(OUTER);
          }
          break;
        }
        setState(2024);
        qualifiedName();
        setState(2025);
        match(T__1);
        setState(2034);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 245, _ctx)) {
          case 1: {
            setState(2026);
            expression();
            setState(2031);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__3) {
              {
                {
                  setState(2027);
                  match(T__3);
                  setState(2028);
                  expression();
                }
              }
              setState(2033);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
          break;
        }
        setState(2036);
        match(T__2);
        setState(2037);
        ((LateralViewContext) _localctx).tblName = identifier();
        setState(2049);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 248, _ctx)) {
          case 1: {
            setState(2039);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 246, _ctx)) {
              case 1: {
                setState(2038);
                match(AS);
              }
              break;
            }
            setState(2041);
            ((LateralViewContext) _localctx).identifier = identifier();
            ((LateralViewContext) _localctx).colName
                .add(((LateralViewContext) _localctx).identifier);
            setState(2046);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 247, _ctx);
            while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
              if (_alt == 1) {
                {
                  {
                    setState(2042);
                    match(T__3);
                    setState(2043);
                    ((LateralViewContext) _localctx).identifier = identifier();
                    ((LateralViewContext) _localctx).colName
                        .add(((LateralViewContext) _localctx).identifier);
                  }
                }
              }
              setState(2048);
              _errHandler.sync(this);
              _alt = getInterpreter().adaptivePredict(_input, 247, _ctx);
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SetQuantifierContext extends ParserRuleContext {
    public TerminalNode DISTINCT() {
      return getToken(CarbonSqlBaseParser.DISTINCT, 0);
    }

    public TerminalNode ALL() {
      return getToken(CarbonSqlBaseParser.ALL, 0);
    }

    public SetQuantifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_setQuantifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSetQuantifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SetQuantifierContext setQuantifier() throws RecognitionException {
    SetQuantifierContext _localctx = new SetQuantifierContext(_ctx, getState());
    enterRule(_localctx, 136, RULE_setQuantifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2051);
        _la = _input.LA(1);
        if (!(_la == ALL || _la == DISTINCT)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class RelationContext extends ParserRuleContext {
    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class, 0);
    }

    public List<JoinRelationContext> joinRelation() {
      return getRuleContexts(JoinRelationContext.class);
    }

    public JoinRelationContext joinRelation(int i) {
      return getRuleContext(JoinRelationContext.class, i);
    }

    public RelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_relation;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RelationContext relation() throws RecognitionException {
    RelationContext _localctx = new RelationContext(_ctx, getState());
    enterRule(_localctx, 138, RULE_relation);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2053);
        relationPrimary();
        setState(2057);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 249, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2054);
                joinRelation();
              }
            }
          }
          setState(2059);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 249, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class JoinRelationContext extends ParserRuleContext {
    public RelationPrimaryContext right;

    public TerminalNode JOIN() {
      return getToken(CarbonSqlBaseParser.JOIN, 0);
    }

    public RelationPrimaryContext relationPrimary() {
      return getRuleContext(RelationPrimaryContext.class, 0);
    }

    public JoinTypeContext joinType() {
      return getRuleContext(JoinTypeContext.class, 0);
    }

    public JoinCriteriaContext joinCriteria() {
      return getRuleContext(JoinCriteriaContext.class, 0);
    }

    public TerminalNode NATURAL() {
      return getToken(CarbonSqlBaseParser.NATURAL, 0);
    }

    public JoinRelationContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_joinRelation;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitJoinRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinRelationContext joinRelation() throws RecognitionException {
    JoinRelationContext _localctx = new JoinRelationContext(_ctx, getState());
    enterRule(_localctx, 140, RULE_joinRelation);
    try {
      setState(2071);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case ANTI:
        case CROSS:
        case FULL:
        case INNER:
        case JOIN:
        case LEFT:
        case RIGHT:
        case SEMI:
          enterOuterAlt(_localctx, 1);
        {
          {
            setState(2060);
            joinType();
          }
          setState(2061);
          match(JOIN);
          setState(2062);
          ((JoinRelationContext) _localctx).right = relationPrimary();
          setState(2064);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 250, _ctx)) {
            case 1: {
              setState(2063);
              joinCriteria();
            }
            break;
          }
        }
        break;
        case NATURAL:
          enterOuterAlt(_localctx, 2);
        {
          setState(2066);
          match(NATURAL);
          setState(2067);
          joinType();
          setState(2068);
          match(JOIN);
          setState(2069);
          ((JoinRelationContext) _localctx).right = relationPrimary();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class JoinTypeContext extends ParserRuleContext {
    public TerminalNode INNER() {
      return getToken(CarbonSqlBaseParser.INNER, 0);
    }

    public TerminalNode CROSS() {
      return getToken(CarbonSqlBaseParser.CROSS, 0);
    }

    public TerminalNode LEFT() {
      return getToken(CarbonSqlBaseParser.LEFT, 0);
    }

    public TerminalNode OUTER() {
      return getToken(CarbonSqlBaseParser.OUTER, 0);
    }

    public TerminalNode SEMI() {
      return getToken(CarbonSqlBaseParser.SEMI, 0);
    }

    public TerminalNode RIGHT() {
      return getToken(CarbonSqlBaseParser.RIGHT, 0);
    }

    public TerminalNode FULL() {
      return getToken(CarbonSqlBaseParser.FULL, 0);
    }

    public TerminalNode ANTI() {
      return getToken(CarbonSqlBaseParser.ANTI, 0);
    }

    public JoinTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_joinType;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitJoinType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinTypeContext joinType() throws RecognitionException {
    JoinTypeContext _localctx = new JoinTypeContext(_ctx, getState());
    enterRule(_localctx, 142, RULE_joinType);
    int _la;
    try {
      setState(2097);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 258, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2074);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == INNER) {
            {
              setState(2073);
              match(INNER);
            }
          }

        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2076);
          match(CROSS);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(2077);
          match(LEFT);
          setState(2079);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(2078);
              match(OUTER);
            }
          }

        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(2082);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LEFT) {
            {
              setState(2081);
              match(LEFT);
            }
          }

          setState(2084);
          match(SEMI);
        }
        break;
        case 5:
          enterOuterAlt(_localctx, 5);
        {
          setState(2085);
          match(RIGHT);
          setState(2087);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(2086);
              match(OUTER);
            }
          }

        }
        break;
        case 6:
          enterOuterAlt(_localctx, 6);
        {
          setState(2089);
          match(FULL);
          setState(2091);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == OUTER) {
            {
              setState(2090);
              match(OUTER);
            }
          }

        }
        break;
        case 7:
          enterOuterAlt(_localctx, 7);
        {
          setState(2094);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == LEFT) {
            {
              setState(2093);
              match(LEFT);
            }
          }

          setState(2096);
          match(ANTI);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class JoinCriteriaContext extends ParserRuleContext {
    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public JoinCriteriaContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_joinCriteria;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitJoinCriteria(this);
      else return visitor.visitChildren(this);
    }
  }

  public final JoinCriteriaContext joinCriteria() throws RecognitionException {
    JoinCriteriaContext _localctx = new JoinCriteriaContext(_ctx, getState());
    enterRule(_localctx, 144, RULE_joinCriteria);
    try {
      setState(2103);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case ON:
          enterOuterAlt(_localctx, 1);
        {
          setState(2099);
          match(ON);
          setState(2100);
          booleanExpression(0);
        }
        break;
        case USING:
          enterOuterAlt(_localctx, 2);
        {
          setState(2101);
          match(USING);
          setState(2102);
          identifierList();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SampleContext extends ParserRuleContext {
    public TerminalNode TABLESAMPLE() {
      return getToken(CarbonSqlBaseParser.TABLESAMPLE, 0);
    }

    public SampleMethodContext sampleMethod() {
      return getRuleContext(SampleMethodContext.class, 0);
    }

    public SampleContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_sample;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSample(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SampleContext sample() throws RecognitionException {
    SampleContext _localctx = new SampleContext(_ctx, getState());
    enterRule(_localctx, 146, RULE_sample);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2105);
        match(TABLESAMPLE);
        setState(2106);
        match(T__1);
        setState(2108);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 260, _ctx)) {
          case 1: {
            setState(2107);
            sampleMethod();
          }
          break;
        }
        setState(2110);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class SampleMethodContext extends ParserRuleContext {
    public SampleMethodContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_sampleMethod;
    }

    public SampleMethodContext() {
    }

    public void copyFrom(SampleMethodContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class SampleByRowsContext extends SampleMethodContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode ROWS() {
      return getToken(CarbonSqlBaseParser.ROWS, 0);
    }

    public SampleByRowsContext(SampleMethodContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSampleByRows(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SampleByPercentileContext extends SampleMethodContext {
    public Token negativeSign;
    public Token percentage;

    public TerminalNode PERCENTLIT() {
      return getToken(CarbonSqlBaseParser.PERCENTLIT, 0);
    }

    public TerminalNode INTEGER_VALUE() {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode DECIMAL_VALUE() {
      return getToken(CarbonSqlBaseParser.DECIMAL_VALUE, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public SampleByPercentileContext(SampleMethodContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSampleByPercentile(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SampleByBucketContext extends SampleMethodContext {
    public Token sampleType;
    public Token numerator;
    public Token denominator;

    public TerminalNode OUT() {
      return getToken(CarbonSqlBaseParser.OUT, 0);
    }

    public TerminalNode OF() {
      return getToken(CarbonSqlBaseParser.OF, 0);
    }

    public TerminalNode BUCKET() {
      return getToken(CarbonSqlBaseParser.BUCKET, 0);
    }

    public List<TerminalNode> INTEGER_VALUE() {
      return getTokens(CarbonSqlBaseParser.INTEGER_VALUE);
    }

    public TerminalNode INTEGER_VALUE(int i) {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, i);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public SampleByBucketContext(SampleMethodContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSampleByBucket(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SampleByBytesContext extends SampleMethodContext {
    public ExpressionContext bytes;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public SampleByBytesContext(SampleMethodContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSampleByBytes(this);
      else return visitor.visitChildren(this);
    }
  }

  public final SampleMethodContext sampleMethod() throws RecognitionException {
    SampleMethodContext _localctx = new SampleMethodContext(_ctx, getState());
    enterRule(_localctx, 148, RULE_sampleMethod);
    int _la;
    try {
      setState(2136);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 264, _ctx)) {
        case 1:
          _localctx = new SampleByPercentileContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2113);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(2112);
              ((SampleByPercentileContext) _localctx).negativeSign = match(MINUS);
            }
          }

          setState(2115);
          ((SampleByPercentileContext) _localctx).percentage = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == INTEGER_VALUE || _la == DECIMAL_VALUE)) {
            ((SampleByPercentileContext) _localctx).percentage =
                (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(2116);
          match(PERCENTLIT);
        }
        break;
        case 2:
          _localctx = new SampleByRowsContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2117);
          expression();
          setState(2118);
          match(ROWS);
        }
        break;
        case 3:
          _localctx = new SampleByBucketContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2120);
          ((SampleByBucketContext) _localctx).sampleType = match(BUCKET);
          setState(2121);
          ((SampleByBucketContext) _localctx).numerator = match(INTEGER_VALUE);
          setState(2122);
          match(OUT);
          setState(2123);
          match(OF);
          setState(2124);
          ((SampleByBucketContext) _localctx).denominator = match(INTEGER_VALUE);
          setState(2133);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == ON) {
            {
              setState(2125);
              match(ON);
              setState(2131);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 262, _ctx)) {
                case 1: {
                  setState(2126);
                  identifier();
                }
                break;
                case 2: {
                  setState(2127);
                  qualifiedName();
                  setState(2128);
                  match(T__1);
                  setState(2129);
                  match(T__2);
                }
                break;
              }
            }
          }

        }
        break;
        case 4:
          _localctx = new SampleByBytesContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(2135);
          ((SampleByBytesContext) _localctx).bytes = expression();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierListContext extends ParserRuleContext {
    public IdentifierSeqContext identifierSeq() {
      return getRuleContext(IdentifierSeqContext.class, 0);
    }

    public IdentifierListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifierList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentifierList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierListContext identifierList() throws RecognitionException {
    IdentifierListContext _localctx = new IdentifierListContext(_ctx, getState());
    enterRule(_localctx, 150, RULE_identifierList);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2138);
        match(T__1);
        setState(2139);
        identifierSeq();
        setState(2140);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierSeqContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext errorCapturingIdentifier;
    public List<ErrorCapturingIdentifierContext> ident =
        new ArrayList<ErrorCapturingIdentifierContext>();

    public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
      return getRuleContexts(ErrorCapturingIdentifierContext.class);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
      return getRuleContext(ErrorCapturingIdentifierContext.class, i);
    }

    public IdentifierSeqContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifierSeq;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentifierSeq(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierSeqContext identifierSeq() throws RecognitionException {
    IdentifierSeqContext _localctx = new IdentifierSeqContext(_ctx, getState());
    enterRule(_localctx, 152, RULE_identifierSeq);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2142);
        ((IdentifierSeqContext) _localctx).errorCapturingIdentifier = errorCapturingIdentifier();
        ((IdentifierSeqContext) _localctx).ident
            .add(((IdentifierSeqContext) _localctx).errorCapturingIdentifier);
        setState(2147);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 265, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2143);
                match(T__3);
                setState(2144);
                ((IdentifierSeqContext) _localctx).errorCapturingIdentifier =
                    errorCapturingIdentifier();
                ((IdentifierSeqContext) _localctx).ident
                    .add(((IdentifierSeqContext) _localctx).errorCapturingIdentifier);
              }
            }
          }
          setState(2149);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 265, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class OrderedIdentifierListContext extends ParserRuleContext {
    public List<OrderedIdentifierContext> orderedIdentifier() {
      return getRuleContexts(OrderedIdentifierContext.class);
    }

    public OrderedIdentifierContext orderedIdentifier(int i) {
      return getRuleContext(OrderedIdentifierContext.class, i);
    }

    public OrderedIdentifierListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_orderedIdentifierList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitOrderedIdentifierList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderedIdentifierListContext orderedIdentifierList() throws RecognitionException {
    OrderedIdentifierListContext _localctx = new OrderedIdentifierListContext(_ctx, getState());
    enterRule(_localctx, 154, RULE_orderedIdentifierList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2150);
        match(T__1);
        setState(2151);
        orderedIdentifier();
        setState(2156);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2152);
              match(T__3);
              setState(2153);
              orderedIdentifier();
            }
          }
          setState(2158);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(2159);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class OrderedIdentifierContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext ident;
    public Token ordering;

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public TerminalNode ASC() {
      return getToken(CarbonSqlBaseParser.ASC, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public OrderedIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_orderedIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitOrderedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final OrderedIdentifierContext orderedIdentifier() throws RecognitionException {
    OrderedIdentifierContext _localctx = new OrderedIdentifierContext(_ctx, getState());
    enterRule(_localctx, 156, RULE_orderedIdentifier);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2161);
        ((OrderedIdentifierContext) _localctx).ident = errorCapturingIdentifier();
        setState(2163);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == ASC || _la == DESC) {
          {
            setState(2162);
            ((OrderedIdentifierContext) _localctx).ordering = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == ASC || _la == DESC)) {
              ((OrderedIdentifierContext) _localctx).ordering =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierCommentListContext extends ParserRuleContext {
    public List<IdentifierCommentContext> identifierComment() {
      return getRuleContexts(IdentifierCommentContext.class);
    }

    public IdentifierCommentContext identifierComment(int i) {
      return getRuleContext(IdentifierCommentContext.class, i);
    }

    public IdentifierCommentListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifierCommentList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentifierCommentList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierCommentListContext identifierCommentList() throws RecognitionException {
    IdentifierCommentListContext _localctx = new IdentifierCommentListContext(_ctx, getState());
    enterRule(_localctx, 158, RULE_identifierCommentList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2165);
        match(T__1);
        setState(2166);
        identifierComment();
        setState(2171);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2167);
              match(T__3);
              setState(2168);
              identifierComment();
            }
          }
          setState(2173);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(2174);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierCommentContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public CommentSpecContext commentSpec() {
      return getRuleContext(CommentSpecContext.class, 0);
    }

    public IdentifierCommentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifierComment;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentifierComment(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierCommentContext identifierComment() throws RecognitionException {
    IdentifierCommentContext _localctx = new IdentifierCommentContext(_ctx, getState());
    enterRule(_localctx, 160, RULE_identifierComment);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2176);
        identifier();
        setState(2178);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == COMMENT) {
          {
            setState(2177);
            commentSpec();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class RelationPrimaryContext extends ParserRuleContext {
    public RelationPrimaryContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_relationPrimary;
    }

    public RelationPrimaryContext() {
    }

    public void copyFrom(RelationPrimaryContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class TableValuedFunctionContext extends RelationPrimaryContext {
    public FunctionTableContext functionTable() {
      return getRuleContext(FunctionTableContext.class, 0);
    }

    public TableValuedFunctionContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableValuedFunction(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class InlineTableDefault2Context extends RelationPrimaryContext {
    public InlineTableContext inlineTable() {
      return getRuleContext(InlineTableContext.class, 0);
    }

    public InlineTableDefault2Context(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInlineTableDefault2(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AliasedRelationContext extends RelationPrimaryContext {
    public RelationContext relation() {
      return getRuleContext(RelationContext.class, 0);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public SampleContext sample() {
      return getRuleContext(SampleContext.class, 0);
    }

    public AliasedRelationContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAliasedRelation(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class AliasedQueryContext extends RelationPrimaryContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public SampleContext sample() {
      return getRuleContext(SampleContext.class, 0);
    }

    public AliasedQueryContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAliasedQuery(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TableNameContext extends RelationPrimaryContext {
    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public SampleContext sample() {
      return getRuleContext(SampleContext.class, 0);
    }

    public TableNameContext(RelationPrimaryContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RelationPrimaryContext relationPrimary() throws RecognitionException {
    RelationPrimaryContext _localctx = new RelationPrimaryContext(_ctx, getState());
    enterRule(_localctx, 162, RULE_relationPrimary);
    try {
      setState(2204);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 273, _ctx)) {
        case 1:
          _localctx = new TableNameContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2180);
          multipartIdentifier();
          setState(2182);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 270, _ctx)) {
            case 1: {
              setState(2181);
              sample();
            }
            break;
          }
          setState(2184);
          tableAlias();
        }
        break;
        case 2:
          _localctx = new AliasedQueryContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2186);
          match(T__1);
          setState(2187);
          query();
          setState(2188);
          match(T__2);
          setState(2190);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 271, _ctx)) {
            case 1: {
              setState(2189);
              sample();
            }
            break;
          }
          setState(2192);
          tableAlias();
        }
        break;
        case 3:
          _localctx = new AliasedRelationContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2194);
          match(T__1);
          setState(2195);
          relation();
          setState(2196);
          match(T__2);
          setState(2198);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 272, _ctx)) {
            case 1: {
              setState(2197);
              sample();
            }
            break;
          }
          setState(2200);
          tableAlias();
        }
        break;
        case 4:
          _localctx = new InlineTableDefault2Context(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(2202);
          inlineTable();
        }
        break;
        case 5:
          _localctx = new TableValuedFunctionContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(2203);
          functionTable();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class InlineTableContext extends ParserRuleContext {
    public TerminalNode VALUES() {
      return getToken(CarbonSqlBaseParser.VALUES, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public InlineTableContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_inlineTable;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInlineTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final InlineTableContext inlineTable() throws RecognitionException {
    InlineTableContext _localctx = new InlineTableContext(_ctx, getState());
    enterRule(_localctx, 164, RULE_inlineTable);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2206);
        match(VALUES);
        setState(2207);
        expression();
        setState(2212);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 274, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2208);
                match(T__3);
                setState(2209);
                expression();
              }
            }
          }
          setState(2214);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 274, _ctx);
        }
        setState(2215);
        tableAlias();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionTableContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext funcName;

    public TableAliasContext tableAlias() {
      return getRuleContext(TableAliasContext.class, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public FunctionTableContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionTable;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFunctionTable(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionTableContext functionTable() throws RecognitionException {
    FunctionTableContext _localctx = new FunctionTableContext(_ctx, getState());
    enterRule(_localctx, 166, RULE_functionTable);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2217);
        ((FunctionTableContext) _localctx).funcName = errorCapturingIdentifier();
        setState(2218);
        match(T__1);
        setState(2227);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 276, _ctx)) {
          case 1: {
            setState(2219);
            expression();
            setState(2224);
            _errHandler.sync(this);
            _la = _input.LA(1);
            while (_la == T__3) {
              {
                {
                  setState(2220);
                  match(T__3);
                  setState(2221);
                  expression();
                }
              }
              setState(2226);
              _errHandler.sync(this);
              _la = _input.LA(1);
            }
          }
          break;
        }
        setState(2229);
        match(T__2);
        setState(2230);
        tableAlias();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TableAliasContext extends ParserRuleContext {
    public StrictIdentifierContext strictIdentifier() {
      return getRuleContext(StrictIdentifierContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public TableAliasContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableAlias;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableAlias(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TableAliasContext tableAlias() throws RecognitionException {
    TableAliasContext _localctx = new TableAliasContext(_ctx, getState());
    enterRule(_localctx, 168, RULE_tableAlias);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2239);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 279, _ctx)) {
          case 1: {
            setState(2233);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 277, _ctx)) {
              case 1: {
                setState(2232);
                match(AS);
              }
              break;
            }
            setState(2235);
            strictIdentifier();
            setState(2237);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 278, _ctx)) {
              case 1: {
                setState(2236);
                identifierList();
              }
              break;
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class RowFormatContext extends ParserRuleContext {
    public RowFormatContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_rowFormat;
    }

    public RowFormatContext() {
    }

    public void copyFrom(RowFormatContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class RowFormatSerdeContext extends RowFormatContext {
    public Token name;
    public TablePropertyListContext props;

    public TerminalNode ROW() {
      return getToken(CarbonSqlBaseParser.ROW, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(CarbonSqlBaseParser.FORMAT, 0);
    }

    public TerminalNode SERDE() {
      return getToken(CarbonSqlBaseParser.SERDE, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public TerminalNode SERDEPROPERTIES() {
      return getToken(CarbonSqlBaseParser.SERDEPROPERTIES, 0);
    }

    public TablePropertyListContext tablePropertyList() {
      return getRuleContext(TablePropertyListContext.class, 0);
    }

    public RowFormatSerdeContext(RowFormatContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRowFormatSerde(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RowFormatDelimitedContext extends RowFormatContext {
    public Token fieldsTerminatedBy;
    public Token escapedBy;
    public Token collectionItemsTerminatedBy;
    public Token keysTerminatedBy;
    public Token linesSeparatedBy;
    public Token nullDefinedAs;

    public TerminalNode ROW() {
      return getToken(CarbonSqlBaseParser.ROW, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(CarbonSqlBaseParser.FORMAT, 0);
    }

    public TerminalNode DELIMITED() {
      return getToken(CarbonSqlBaseParser.DELIMITED, 0);
    }

    public TerminalNode FIELDS() {
      return getToken(CarbonSqlBaseParser.FIELDS, 0);
    }

    public List<TerminalNode> TERMINATED() {
      return getTokens(CarbonSqlBaseParser.TERMINATED);
    }

    public TerminalNode TERMINATED(int i) {
      return getToken(CarbonSqlBaseParser.TERMINATED, i);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public TerminalNode COLLECTION() {
      return getToken(CarbonSqlBaseParser.COLLECTION, 0);
    }

    public TerminalNode ITEMS() {
      return getToken(CarbonSqlBaseParser.ITEMS, 0);
    }

    public TerminalNode MAP() {
      return getToken(CarbonSqlBaseParser.MAP, 0);
    }

    public TerminalNode KEYS() {
      return getToken(CarbonSqlBaseParser.KEYS, 0);
    }

    public TerminalNode LINES() {
      return getToken(CarbonSqlBaseParser.LINES, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public TerminalNode DEFINED() {
      return getToken(CarbonSqlBaseParser.DEFINED, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public List<TerminalNode> STRING() {
      return getTokens(CarbonSqlBaseParser.STRING);
    }

    public TerminalNode STRING(int i) {
      return getToken(CarbonSqlBaseParser.STRING, i);
    }

    public TerminalNode ESCAPED() {
      return getToken(CarbonSqlBaseParser.ESCAPED, 0);
    }

    public RowFormatDelimitedContext(RowFormatContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRowFormatDelimited(this);
      else return visitor.visitChildren(this);
    }
  }

  public final RowFormatContext rowFormat() throws RecognitionException {
    RowFormatContext _localctx = new RowFormatContext(_ctx, getState());
    enterRule(_localctx, 170, RULE_rowFormat);
    try {
      setState(2290);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 287, _ctx)) {
        case 1:
          _localctx = new RowFormatSerdeContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2241);
          match(ROW);
          setState(2242);
          match(FORMAT);
          setState(2243);
          match(SERDE);
          setState(2244);
          ((RowFormatSerdeContext) _localctx).name = match(STRING);
          setState(2248);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 280, _ctx)) {
            case 1: {
              setState(2245);
              match(WITH);
              setState(2246);
              match(SERDEPROPERTIES);
              setState(2247);
              ((RowFormatSerdeContext) _localctx).props = tablePropertyList();
            }
            break;
          }
        }
        break;
        case 2:
          _localctx = new RowFormatDelimitedContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2250);
          match(ROW);
          setState(2251);
          match(FORMAT);
          setState(2252);
          match(DELIMITED);
          setState(2262);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 282, _ctx)) {
            case 1: {
              setState(2253);
              match(FIELDS);
              setState(2254);
              match(TERMINATED);
              setState(2255);
              match(BY);
              setState(2256);
              ((RowFormatDelimitedContext) _localctx).fieldsTerminatedBy = match(STRING);
              setState(2260);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 281, _ctx)) {
                case 1: {
                  setState(2257);
                  match(ESCAPED);
                  setState(2258);
                  match(BY);
                  setState(2259);
                  ((RowFormatDelimitedContext) _localctx).escapedBy = match(STRING);
                }
                break;
              }
            }
            break;
          }
          setState(2269);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 283, _ctx)) {
            case 1: {
              setState(2264);
              match(COLLECTION);
              setState(2265);
              match(ITEMS);
              setState(2266);
              match(TERMINATED);
              setState(2267);
              match(BY);
              setState(2268);
              ((RowFormatDelimitedContext) _localctx).collectionItemsTerminatedBy = match(STRING);
            }
            break;
          }
          setState(2276);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 284, _ctx)) {
            case 1: {
              setState(2271);
              match(MAP);
              setState(2272);
              match(KEYS);
              setState(2273);
              match(TERMINATED);
              setState(2274);
              match(BY);
              setState(2275);
              ((RowFormatDelimitedContext) _localctx).keysTerminatedBy = match(STRING);
            }
            break;
          }
          setState(2282);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 285, _ctx)) {
            case 1: {
              setState(2278);
              match(LINES);
              setState(2279);
              match(TERMINATED);
              setState(2280);
              match(BY);
              setState(2281);
              ((RowFormatDelimitedContext) _localctx).linesSeparatedBy = match(STRING);
            }
            break;
          }
          setState(2288);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 286, _ctx)) {
            case 1: {
              setState(2284);
              match(NULL);
              setState(2285);
              match(DEFINED);
              setState(2286);
              match(AS);
              setState(2287);
              ((RowFormatDelimitedContext) _localctx).nullDefinedAs = match(STRING);
            }
            break;
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MultipartIdentifierListContext extends ParserRuleContext {
    public List<MultipartIdentifierContext> multipartIdentifier() {
      return getRuleContexts(MultipartIdentifierContext.class);
    }

    public MultipartIdentifierContext multipartIdentifier(int i) {
      return getRuleContext(MultipartIdentifierContext.class, i);
    }

    public MultipartIdentifierListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_multipartIdentifierList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMultipartIdentifierList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MultipartIdentifierListContext multipartIdentifierList()
      throws RecognitionException {
    MultipartIdentifierListContext _localctx = new MultipartIdentifierListContext(_ctx, getState());
    enterRule(_localctx, 172, RULE_multipartIdentifierList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2292);
        multipartIdentifier();
        setState(2297);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2293);
              match(T__3);
              setState(2294);
              multipartIdentifier();
            }
          }
          setState(2299);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MultipartIdentifierContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext errorCapturingIdentifier;
    public List<ErrorCapturingIdentifierContext> parts =
        new ArrayList<ErrorCapturingIdentifierContext>();

    public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
      return getRuleContexts(ErrorCapturingIdentifierContext.class);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
      return getRuleContext(ErrorCapturingIdentifierContext.class, i);
    }

    public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_multipartIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMultipartIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
    MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
    enterRule(_localctx, 174, RULE_multipartIdentifier);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2300);
        ((MultipartIdentifierContext) _localctx).errorCapturingIdentifier =
            errorCapturingIdentifier();
        ((MultipartIdentifierContext) _localctx).parts
            .add(((MultipartIdentifierContext) _localctx).errorCapturingIdentifier);
        setState(2305);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 289, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2301);
                match(T__4);
                setState(2302);
                ((MultipartIdentifierContext) _localctx).errorCapturingIdentifier =
                    errorCapturingIdentifier();
                ((MultipartIdentifierContext) _localctx).parts
                    .add(((MultipartIdentifierContext) _localctx).errorCapturingIdentifier);
              }
            }
          }
          setState(2307);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 289, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TableIdentifierContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext db;
    public ErrorCapturingIdentifierContext table;

    public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
      return getRuleContexts(ErrorCapturingIdentifierContext.class);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
      return getRuleContext(ErrorCapturingIdentifierContext.class, i);
    }

    public TableIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_tableIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTableIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TableIdentifierContext tableIdentifier() throws RecognitionException {
    TableIdentifierContext _localctx = new TableIdentifierContext(_ctx, getState());
    enterRule(_localctx, 176, RULE_tableIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2311);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 290, _ctx)) {
          case 1: {
            setState(2308);
            ((TableIdentifierContext) _localctx).db = errorCapturingIdentifier();
            setState(2309);
            match(T__4);
          }
          break;
        }
        setState(2313);
        ((TableIdentifierContext) _localctx).table = errorCapturingIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionIdentifierContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext db;
    public ErrorCapturingIdentifierContext function;

    public List<ErrorCapturingIdentifierContext> errorCapturingIdentifier() {
      return getRuleContexts(ErrorCapturingIdentifierContext.class);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier(int i) {
      return getRuleContext(ErrorCapturingIdentifierContext.class, i);
    }

    public FunctionIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFunctionIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionIdentifierContext functionIdentifier() throws RecognitionException {
    FunctionIdentifierContext _localctx = new FunctionIdentifierContext(_ctx, getState());
    enterRule(_localctx, 178, RULE_functionIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2318);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 291, _ctx)) {
          case 1: {
            setState(2315);
            ((FunctionIdentifierContext) _localctx).db = errorCapturingIdentifier();
            setState(2316);
            match(T__4);
          }
          break;
        }
        setState(2320);
        ((FunctionIdentifierContext) _localctx).function = errorCapturingIdentifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamedExpressionContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext name;

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public IdentifierListContext identifierList() {
      return getRuleContext(IdentifierListContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public NamedExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namedExpression;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNamedExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedExpressionContext namedExpression() throws RecognitionException {
    NamedExpressionContext _localctx = new NamedExpressionContext(_ctx, getState());
    enterRule(_localctx, 180, RULE_namedExpression);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2322);
        expression();
        setState(2330);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 294, _ctx)) {
          case 1: {
            setState(2324);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 292, _ctx)) {
              case 1: {
                setState(2323);
                match(AS);
              }
              break;
            }
            setState(2328);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 293, _ctx)) {
              case 1: {
                setState(2326);
                ((NamedExpressionContext) _localctx).name = errorCapturingIdentifier();
              }
              break;
              case 2: {
                setState(2327);
                identifierList();
              }
              break;
            }
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamedExpressionSeqContext extends ParserRuleContext {
    public List<NamedExpressionContext> namedExpression() {
      return getRuleContexts(NamedExpressionContext.class);
    }

    public NamedExpressionContext namedExpression(int i) {
      return getRuleContext(NamedExpressionContext.class, i);
    }

    public NamedExpressionSeqContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namedExpressionSeq;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNamedExpressionSeq(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedExpressionSeqContext namedExpressionSeq() throws RecognitionException {
    NamedExpressionSeqContext _localctx = new NamedExpressionSeqContext(_ctx, getState());
    enterRule(_localctx, 182, RULE_namedExpressionSeq);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2332);
        namedExpression();
        setState(2337);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 295, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2333);
                match(T__3);
                setState(2334);
                namedExpression();
              }
            }
          }
          setState(2339);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 295, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TransformListContext extends ParserRuleContext {
    public TransformContext transform;
    public List<TransformContext> transforms = new ArrayList<TransformContext>();

    public List<TransformContext> transform() {
      return getRuleContexts(TransformContext.class);
    }

    public TransformContext transform(int i) {
      return getRuleContext(TransformContext.class, i);
    }

    public TransformListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transformList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTransformList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TransformListContext transformList() throws RecognitionException {
    TransformListContext _localctx = new TransformListContext(_ctx, getState());
    enterRule(_localctx, 184, RULE_transformList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2340);
        match(T__1);
        setState(2341);
        ((TransformListContext) _localctx).transform = transform();
        ((TransformListContext) _localctx).transforms
            .add(((TransformListContext) _localctx).transform);
        setState(2346);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2342);
              match(T__3);
              setState(2343);
              ((TransformListContext) _localctx).transform = transform();
              ((TransformListContext) _localctx).transforms
                  .add(((TransformListContext) _localctx).transform);
            }
          }
          setState(2348);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
        setState(2349);
        match(T__2);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TransformContext extends ParserRuleContext {
    public TransformContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transform;
    }

    public TransformContext() {
    }

    public void copyFrom(TransformContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class IdentityTransformContext extends TransformContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public IdentityTransformContext(TransformContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentityTransform(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ApplyTransformContext extends TransformContext {
    public IdentifierContext transformName;
    public TransformArgumentContext transformArgument;
    public List<TransformArgumentContext> argument = new ArrayList<TransformArgumentContext>();

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public List<TransformArgumentContext> transformArgument() {
      return getRuleContexts(TransformArgumentContext.class);
    }

    public TransformArgumentContext transformArgument(int i) {
      return getRuleContext(TransformArgumentContext.class, i);
    }

    public ApplyTransformContext(TransformContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitApplyTransform(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TransformContext transform() throws RecognitionException {
    TransformContext _localctx = new TransformContext(_ctx, getState());
    enterRule(_localctx, 186, RULE_transform);
    int _la;
    try {
      setState(2364);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 298, _ctx)) {
        case 1:
          _localctx = new IdentityTransformContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2351);
          qualifiedName();
        }
        break;
        case 2:
          _localctx = new ApplyTransformContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2352);
          ((ApplyTransformContext) _localctx).transformName = identifier();
          setState(2353);
          match(T__1);
          setState(2354);
          ((ApplyTransformContext) _localctx).transformArgument = transformArgument();
          ((ApplyTransformContext) _localctx).argument
              .add(((ApplyTransformContext) _localctx).transformArgument);
          setState(2359);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(2355);
                match(T__3);
                setState(2356);
                ((ApplyTransformContext) _localctx).transformArgument = transformArgument();
                ((ApplyTransformContext) _localctx).argument
                    .add(((ApplyTransformContext) _localctx).transformArgument);
              }
            }
            setState(2361);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(2362);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class TransformArgumentContext extends ParserRuleContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class, 0);
    }

    public TransformArgumentContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_transformArgument;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTransformArgument(this);
      else return visitor.visitChildren(this);
    }
  }

  public final TransformArgumentContext transformArgument() throws RecognitionException {
    TransformArgumentContext _localctx = new TransformArgumentContext(_ctx, getState());
    enterRule(_localctx, 188, RULE_transformArgument);
    try {
      setState(2368);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 299, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2366);
          qualifiedName();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2367);
          constant();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ExpressionContext extends ParserRuleContext {
    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public ExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_expression;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ExpressionContext expression() throws RecognitionException {
    ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
    enterRule(_localctx, 190, RULE_expression);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2370);
        booleanExpression(0);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BooleanExpressionContext extends ParserRuleContext {
    public BooleanExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_booleanExpression;
    }

    public BooleanExpressionContext() {
    }

    public void copyFrom(BooleanExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class LogicalNotContext extends BooleanExpressionContext {
    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public LogicalNotContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLogicalNot(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class PredicatedContext extends BooleanExpressionContext {
    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public PredicateContext predicate() {
      return getRuleContext(PredicateContext.class, 0);
    }

    public PredicatedContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPredicated(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ExistsContext extends BooleanExpressionContext {
    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public ExistsContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitExists(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LogicalBinaryContext extends BooleanExpressionContext {
    public BooleanExpressionContext left;
    public Token operator;
    public BooleanExpressionContext right;

    public List<BooleanExpressionContext> booleanExpression() {
      return getRuleContexts(BooleanExpressionContext.class);
    }

    public BooleanExpressionContext booleanExpression(int i) {
      return getRuleContext(BooleanExpressionContext.class, i);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public LogicalBinaryContext(BooleanExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLogicalBinary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanExpressionContext booleanExpression() throws RecognitionException {
    return booleanExpression(0);
  }

  private BooleanExpressionContext booleanExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    BooleanExpressionContext _localctx = new BooleanExpressionContext(_ctx, _parentState);
    BooleanExpressionContext _prevctx = _localctx;
    int _startState = 192;
    enterRecursionRule(_localctx, 192, RULE_booleanExpression, _p);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2384);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 301, _ctx)) {
          case 1: {
            _localctx = new LogicalNotContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(2373);
            match(NOT);
            setState(2374);
            booleanExpression(5);
          }
          break;
          case 2: {
            _localctx = new ExistsContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2375);
            match(EXISTS);
            setState(2376);
            match(T__1);
            setState(2377);
            query();
            setState(2378);
            match(T__2);
          }
          break;
          case 3: {
            _localctx = new PredicatedContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2380);
            valueExpression(0);
            setState(2382);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 300, _ctx)) {
              case 1: {
                setState(2381);
                predicate();
              }
              break;
            }
          }
          break;
        }
        _ctx.stop = _input.LT(-1);
        setState(2394);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 303, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(2392);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 302, _ctx)) {
                case 1: {
                  _localctx = new LogicalBinaryContext(
                      new BooleanExpressionContext(_parentctx, _parentState));
                  ((LogicalBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                  setState(2386);
                  if (!(precpred(_ctx, 2)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  setState(2387);
                  ((LogicalBinaryContext) _localctx).operator = match(AND);
                  setState(2388);
                  ((LogicalBinaryContext) _localctx).right = booleanExpression(3);
                }
                break;
                case 2: {
                  _localctx = new LogicalBinaryContext(
                      new BooleanExpressionContext(_parentctx, _parentState));
                  ((LogicalBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_booleanExpression);
                  setState(2389);
                  if (!(precpred(_ctx, 1)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  setState(2390);
                  ((LogicalBinaryContext) _localctx).operator = match(OR);
                  setState(2391);
                  ((LogicalBinaryContext) _localctx).right = booleanExpression(2);
                }
                break;
              }
            }
          }
          setState(2396);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 303, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PredicateContext extends ParserRuleContext {
    public Token kind;
    public ValueExpressionContext lower;
    public ValueExpressionContext upper;
    public ValueExpressionContext pattern;
    public Token quantifier;
    public Token escapeChar;
    public ValueExpressionContext right;

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public TerminalNode BETWEEN() {
      return getToken(CarbonSqlBaseParser.BETWEEN, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public TerminalNode RLIKE() {
      return getToken(CarbonSqlBaseParser.RLIKE, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public TerminalNode ANY() {
      return getToken(CarbonSqlBaseParser.ANY, 0);
    }

    public TerminalNode SOME() {
      return getToken(CarbonSqlBaseParser.SOME, 0);
    }

    public TerminalNode ALL() {
      return getToken(CarbonSqlBaseParser.ALL, 0);
    }

    public TerminalNode ESCAPE() {
      return getToken(CarbonSqlBaseParser.ESCAPE, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TerminalNode IS() {
      return getToken(CarbonSqlBaseParser.IS, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public TerminalNode TRUE() {
      return getToken(CarbonSqlBaseParser.TRUE, 0);
    }

    public TerminalNode FALSE() {
      return getToken(CarbonSqlBaseParser.FALSE, 0);
    }

    public TerminalNode UNKNOWN() {
      return getToken(CarbonSqlBaseParser.UNKNOWN, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode DISTINCT() {
      return getToken(CarbonSqlBaseParser.DISTINCT, 0);
    }

    public PredicateContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_predicate;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPredicate(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicateContext predicate() throws RecognitionException {
    PredicateContext _localctx = new PredicateContext(_ctx, getState());
    enterRule(_localctx, 194, RULE_predicate);
    int _la;
    try {
      setState(2479);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 317, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2398);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2397);
              match(NOT);
            }
          }

          setState(2400);
          ((PredicateContext) _localctx).kind = match(BETWEEN);
          setState(2401);
          ((PredicateContext) _localctx).lower = valueExpression(0);
          setState(2402);
          match(AND);
          setState(2403);
          ((PredicateContext) _localctx).upper = valueExpression(0);
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2406);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2405);
              match(NOT);
            }
          }

          setState(2408);
          ((PredicateContext) _localctx).kind = match(IN);
          setState(2409);
          match(T__1);
          setState(2410);
          expression();
          setState(2415);
          _errHandler.sync(this);
          _la = _input.LA(1);
          while (_la == T__3) {
            {
              {
                setState(2411);
                match(T__3);
                setState(2412);
                expression();
              }
            }
            setState(2417);
            _errHandler.sync(this);
            _la = _input.LA(1);
          }
          setState(2418);
          match(T__2);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(2421);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2420);
              match(NOT);
            }
          }

          setState(2423);
          ((PredicateContext) _localctx).kind = match(IN);
          setState(2424);
          match(T__1);
          setState(2425);
          query();
          setState(2426);
          match(T__2);
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(2429);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2428);
              match(NOT);
            }
          }

          setState(2431);
          ((PredicateContext) _localctx).kind = match(RLIKE);
          setState(2432);
          ((PredicateContext) _localctx).pattern = valueExpression(0);
        }
        break;
        case 5:
          enterOuterAlt(_localctx, 5);
        {
          setState(2434);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2433);
              match(NOT);
            }
          }

          setState(2436);
          ((PredicateContext) _localctx).kind = match(LIKE);
          setState(2437);
          ((PredicateContext) _localctx).quantifier = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == ALL || _la == ANY || _la == SOME)) {
            ((PredicateContext) _localctx).quantifier = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(2451);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 311, _ctx)) {
            case 1: {
              setState(2438);
              match(T__1);
              setState(2439);
              match(T__2);
            }
            break;
            case 2: {
              setState(2440);
              match(T__1);
              setState(2441);
              expression();
              setState(2446);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(2442);
                    match(T__3);
                    setState(2443);
                    expression();
                  }
                }
                setState(2448);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
              setState(2449);
              match(T__2);
            }
            break;
          }
        }
        break;
        case 6:
          enterOuterAlt(_localctx, 6);
        {
          setState(2454);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2453);
              match(NOT);
            }
          }

          setState(2456);
          ((PredicateContext) _localctx).kind = match(LIKE);
          setState(2457);
          ((PredicateContext) _localctx).pattern = valueExpression(0);
          setState(2460);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 313, _ctx)) {
            case 1: {
              setState(2458);
              match(ESCAPE);
              setState(2459);
              ((PredicateContext) _localctx).escapeChar = match(STRING);
            }
            break;
          }
        }
        break;
        case 7:
          enterOuterAlt(_localctx, 7);
        {
          setState(2462);
          match(IS);
          setState(2464);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2463);
              match(NOT);
            }
          }

          setState(2466);
          ((PredicateContext) _localctx).kind = match(NULL);
        }
        break;
        case 8:
          enterOuterAlt(_localctx, 8);
        {
          setState(2467);
          match(IS);
          setState(2469);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2468);
              match(NOT);
            }
          }

          setState(2471);
          ((PredicateContext) _localctx).kind = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == FALSE || _la == TRUE || _la == UNKNOWN)) {
            ((PredicateContext) _localctx).kind = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 9:
          enterOuterAlt(_localctx, 9);
        {
          setState(2472);
          match(IS);
          setState(2474);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == NOT) {
            {
              setState(2473);
              match(NOT);
            }
          }

          setState(2476);
          ((PredicateContext) _localctx).kind = match(DISTINCT);
          setState(2477);
          match(FROM);
          setState(2478);
          ((PredicateContext) _localctx).right = valueExpression(0);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ValueExpressionContext extends ParserRuleContext {
    public ValueExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_valueExpression;
    }

    public ValueExpressionContext() {
    }

    public void copyFrom(ValueExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ValueExpressionDefaultContext extends ValueExpressionContext {
    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public ValueExpressionDefaultContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitValueExpressionDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ComparisonContext extends ValueExpressionContext {
    public ValueExpressionContext left;
    public ValueExpressionContext right;

    public ComparisonOperatorContext comparisonOperator() {
      return getRuleContext(ComparisonOperatorContext.class, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public ComparisonContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitComparison(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ArithmeticBinaryContext extends ValueExpressionContext {
    public ValueExpressionContext left;
    public Token operator;
    public ValueExpressionContext right;

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode ASTERISK() {
      return getToken(CarbonSqlBaseParser.ASTERISK, 0);
    }

    public TerminalNode SLASH() {
      return getToken(CarbonSqlBaseParser.SLASH, 0);
    }

    public TerminalNode PERCENT() {
      return getToken(CarbonSqlBaseParser.PERCENT, 0);
    }

    public TerminalNode DIV() {
      return getToken(CarbonSqlBaseParser.DIV, 0);
    }

    public TerminalNode PLUS() {
      return getToken(CarbonSqlBaseParser.PLUS, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public TerminalNode CONCAT_PIPE() {
      return getToken(CarbonSqlBaseParser.CONCAT_PIPE, 0);
    }

    public TerminalNode AMPERSAND() {
      return getToken(CarbonSqlBaseParser.AMPERSAND, 0);
    }

    public TerminalNode HAT() {
      return getToken(CarbonSqlBaseParser.HAT, 0);
    }

    public TerminalNode PIPE() {
      return getToken(CarbonSqlBaseParser.PIPE, 0);
    }

    public ArithmeticBinaryContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitArithmeticBinary(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ArithmeticUnaryContext extends ValueExpressionContext {
    public Token operator;

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public TerminalNode PLUS() {
      return getToken(CarbonSqlBaseParser.PLUS, 0);
    }

    public TerminalNode TILDE() {
      return getToken(CarbonSqlBaseParser.TILDE, 0);
    }

    public ArithmeticUnaryContext(ValueExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitArithmeticUnary(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ValueExpressionContext valueExpression() throws RecognitionException {
    return valueExpression(0);
  }

  private ValueExpressionContext valueExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    ValueExpressionContext _localctx = new ValueExpressionContext(_ctx, _parentState);
    ValueExpressionContext _prevctx = _localctx;
    int _startState = 196;
    enterRecursionRule(_localctx, 196, RULE_valueExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2485);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 318, _ctx)) {
          case 1: {
            _localctx = new ValueExpressionDefaultContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(2482);
            primaryExpression(0);
          }
          break;
          case 2: {
            _localctx = new ArithmeticUnaryContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2483);
            ((ArithmeticUnaryContext) _localctx).operator = _input.LT(1);
            _la = _input.LA(1);
            if (!(((((_la - 269)) & ~0x3f) == 0 &&
                ((1L << (_la - 269)) & ((1L << (PLUS - 269)) | (1L << (MINUS - 269)) | (1L << (TILDE
                    - 269)))) != 0))) {
              ((ArithmeticUnaryContext) _localctx).operator =
                  (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(2484);
            valueExpression(7);
          }
          break;
        }
        _ctx.stop = _input.LT(-1);
        setState(2508);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 320, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(2506);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 319, _ctx)) {
                case 1: {
                  _localctx = new ArithmeticBinaryContext(
                      new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2487);
                  if (!(precpred(_ctx, 6)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                  setState(2488);
                  ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(_la == DIV || ((((_la - 271)) & ~0x3f) == 0 &&
                      ((1L << (_la - 271)) & ((1L << (ASTERISK - 271)) | (1L << (SLASH - 271)) | (1L
                          << (PERCENT - 271)))) != 0))) {
                    ((ArithmeticBinaryContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(2489);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(7);
                }
                break;
                case 2: {
                  _localctx = new ArithmeticBinaryContext(
                      new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2490);
                  if (!(precpred(_ctx, 5)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 5)");
                  setState(2491);
                  ((ArithmeticBinaryContext) _localctx).operator = _input.LT(1);
                  _la = _input.LA(1);
                  if (!(((((_la - 269)) & ~0x3f) == 0 &&
                      ((1L << (_la - 269)) & ((1L << (PLUS - 269)) | (1L << (MINUS - 269)) | (1L
                          << (CONCAT_PIPE - 269)))) != 0))) {
                    ((ArithmeticBinaryContext) _localctx).operator =
                        (Token) _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(2492);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(6);
                }
                break;
                case 3: {
                  _localctx = new ArithmeticBinaryContext(
                      new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2493);
                  if (!(precpred(_ctx, 4)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 4)");
                  setState(2494);
                  ((ArithmeticBinaryContext) _localctx).operator = match(AMPERSAND);
                  setState(2495);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(5);
                }
                break;
                case 4: {
                  _localctx = new ArithmeticBinaryContext(
                      new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2496);
                  if (!(precpred(_ctx, 3)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 3)");
                  setState(2497);
                  ((ArithmeticBinaryContext) _localctx).operator = match(HAT);
                  setState(2498);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(4);
                }
                break;
                case 5: {
                  _localctx = new ArithmeticBinaryContext(
                      new ValueExpressionContext(_parentctx, _parentState));
                  ((ArithmeticBinaryContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2499);
                  if (!(precpred(_ctx, 2)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 2)");
                  setState(2500);
                  ((ArithmeticBinaryContext) _localctx).operator = match(PIPE);
                  setState(2501);
                  ((ArithmeticBinaryContext) _localctx).right = valueExpression(3);
                }
                break;
                case 6: {
                  _localctx =
                      new ComparisonContext(new ValueExpressionContext(_parentctx, _parentState));
                  ((ComparisonContext) _localctx).left = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_valueExpression);
                  setState(2502);
                  if (!(precpred(_ctx, 1)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 1)");
                  setState(2503);
                  comparisonOperator();
                  setState(2504);
                  ((ComparisonContext) _localctx).right = valueExpression(2);
                }
                break;
              }
            }
          }
          setState(2510);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 320, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class PrimaryExpressionContext extends ParserRuleContext {
    public PrimaryExpressionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_primaryExpression;
    }

    public PrimaryExpressionContext() {
    }

    public void copyFrom(PrimaryExpressionContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class StructContext extends PrimaryExpressionContext {
    public NamedExpressionContext namedExpression;
    public List<NamedExpressionContext> argument = new ArrayList<NamedExpressionContext>();

    public TerminalNode STRUCT() {
      return getToken(CarbonSqlBaseParser.STRUCT, 0);
    }

    public List<NamedExpressionContext> namedExpression() {
      return getRuleContexts(NamedExpressionContext.class);
    }

    public NamedExpressionContext namedExpression(int i) {
      return getRuleContext(NamedExpressionContext.class, i);
    }

    public StructContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStruct(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DereferenceContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext base;
    public IdentifierContext fieldName;

    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public DereferenceContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDereference(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SimpleCaseContext extends PrimaryExpressionContext {
    public ExpressionContext value;
    public ExpressionContext elseExpression;

    public TerminalNode CASE() {
      return getToken(CarbonSqlBaseParser.CASE, 0);
    }

    public TerminalNode END() {
      return getToken(CarbonSqlBaseParser.END, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public List<WhenClauseContext> whenClause() {
      return getRuleContexts(WhenClauseContext.class);
    }

    public WhenClauseContext whenClause(int i) {
      return getRuleContext(WhenClauseContext.class, i);
    }

    public TerminalNode ELSE() {
      return getToken(CarbonSqlBaseParser.ELSE, 0);
    }

    public SimpleCaseContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSimpleCase(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ColumnReferenceContext extends PrimaryExpressionContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ColumnReferenceContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitColumnReference(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RowConstructorContext extends PrimaryExpressionContext {
    public List<NamedExpressionContext> namedExpression() {
      return getRuleContexts(NamedExpressionContext.class);
    }

    public NamedExpressionContext namedExpression(int i) {
      return getRuleContext(NamedExpressionContext.class, i);
    }

    public RowConstructorContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRowConstructor(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LastContext extends PrimaryExpressionContext {
    public TerminalNode LAST() {
      return getToken(CarbonSqlBaseParser.LAST, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode IGNORE() {
      return getToken(CarbonSqlBaseParser.IGNORE, 0);
    }

    public TerminalNode NULLS() {
      return getToken(CarbonSqlBaseParser.NULLS, 0);
    }

    public LastContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLast(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class StarContext extends PrimaryExpressionContext {
    public TerminalNode ASTERISK() {
      return getToken(CarbonSqlBaseParser.ASTERISK, 0);
    }

    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public StarContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStar(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class OverlayContext extends PrimaryExpressionContext {
    public ValueExpressionContext input;
    public ValueExpressionContext replace;
    public ValueExpressionContext position;
    public ValueExpressionContext length;

    public TerminalNode OVERLAY() {
      return getToken(CarbonSqlBaseParser.OVERLAY, 0);
    }

    public TerminalNode PLACING() {
      return getToken(CarbonSqlBaseParser.PLACING, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode FOR() {
      return getToken(CarbonSqlBaseParser.FOR, 0);
    }

    public OverlayContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitOverlay(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SubscriptContext extends PrimaryExpressionContext {
    public PrimaryExpressionContext value;
    public ValueExpressionContext index;

    public PrimaryExpressionContext primaryExpression() {
      return getRuleContext(PrimaryExpressionContext.class, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public SubscriptContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSubscript(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SubqueryExpressionContext extends PrimaryExpressionContext {
    public QueryContext query() {
      return getRuleContext(QueryContext.class, 0);
    }

    public SubqueryExpressionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSubqueryExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SubstringContext extends PrimaryExpressionContext {
    public ValueExpressionContext str;
    public ValueExpressionContext pos;
    public ValueExpressionContext len;

    public TerminalNode SUBSTR() {
      return getToken(CarbonSqlBaseParser.SUBSTR, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(CarbonSqlBaseParser.SUBSTRING, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode FOR() {
      return getToken(CarbonSqlBaseParser.FOR, 0);
    }

    public SubstringContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSubstring(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CurrentDatetimeContext extends PrimaryExpressionContext {
    public Token name;

    public TerminalNode CURRENT_DATE() {
      return getToken(CarbonSqlBaseParser.CURRENT_DATE, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(CarbonSqlBaseParser.CURRENT_TIMESTAMP, 0);
    }

    public CurrentDatetimeContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCurrentDatetime(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class CastContext extends PrimaryExpressionContext {
    public TerminalNode CAST() {
      return getToken(CarbonSqlBaseParser.CAST, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public CastContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitCast(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ConstantDefaultContext extends PrimaryExpressionContext {
    public ConstantContext constant() {
      return getRuleContext(ConstantContext.class, 0);
    }

    public ConstantDefaultContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitConstantDefault(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LambdaContext extends PrimaryExpressionContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public LambdaContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLambda(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ParenthesizedExpressionContext extends PrimaryExpressionContext {
    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public ParenthesizedExpressionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitParenthesizedExpression(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ExtractContext extends PrimaryExpressionContext {
    public IdentifierContext field;
    public ValueExpressionContext source;

    public TerminalNode EXTRACT() {
      return getToken(CarbonSqlBaseParser.EXTRACT, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ValueExpressionContext valueExpression() {
      return getRuleContext(ValueExpressionContext.class, 0);
    }

    public ExtractContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitExtract(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TrimContext extends PrimaryExpressionContext {
    public Token trimOption;
    public ValueExpressionContext trimStr;
    public ValueExpressionContext srcStr;

    public TerminalNode TRIM() {
      return getToken(CarbonSqlBaseParser.TRIM, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public TerminalNode BOTH() {
      return getToken(CarbonSqlBaseParser.BOTH, 0);
    }

    public TerminalNode LEADING() {
      return getToken(CarbonSqlBaseParser.LEADING, 0);
    }

    public TerminalNode TRAILING() {
      return getToken(CarbonSqlBaseParser.TRAILING, 0);
    }

    public TrimContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTrim(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FunctionCallContext extends PrimaryExpressionContext {
    public ExpressionContext expression;
    public List<ExpressionContext> argument = new ArrayList<ExpressionContext>();
    public BooleanExpressionContext where;

    public FunctionNameContext functionName() {
      return getRuleContext(FunctionNameContext.class, 0);
    }

    public TerminalNode FILTER() {
      return getToken(CarbonSqlBaseParser.FILTER, 0);
    }

    public TerminalNode WHERE() {
      return getToken(CarbonSqlBaseParser.WHERE, 0);
    }

    public TerminalNode OVER() {
      return getToken(CarbonSqlBaseParser.OVER, 0);
    }

    public WindowSpecContext windowSpec() {
      return getRuleContext(WindowSpecContext.class, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public BooleanExpressionContext booleanExpression() {
      return getRuleContext(BooleanExpressionContext.class, 0);
    }

    public SetQuantifierContext setQuantifier() {
      return getRuleContext(SetQuantifierContext.class, 0);
    }

    public FunctionCallContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFunctionCall(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SearchedCaseContext extends PrimaryExpressionContext {
    public ExpressionContext elseExpression;

    public TerminalNode CASE() {
      return getToken(CarbonSqlBaseParser.CASE, 0);
    }

    public TerminalNode END() {
      return getToken(CarbonSqlBaseParser.END, 0);
    }

    public List<WhenClauseContext> whenClause() {
      return getRuleContexts(WhenClauseContext.class);
    }

    public WhenClauseContext whenClause(int i) {
      return getRuleContext(WhenClauseContext.class, i);
    }

    public TerminalNode ELSE() {
      return getToken(CarbonSqlBaseParser.ELSE, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public SearchedCaseContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSearchedCase(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class PositionContext extends PrimaryExpressionContext {
    public ValueExpressionContext substr;
    public ValueExpressionContext str;

    public TerminalNode POSITION() {
      return getToken(CarbonSqlBaseParser.POSITION, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public List<ValueExpressionContext> valueExpression() {
      return getRuleContexts(ValueExpressionContext.class);
    }

    public ValueExpressionContext valueExpression(int i) {
      return getRuleContext(ValueExpressionContext.class, i);
    }

    public PositionContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPosition(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FirstContext extends PrimaryExpressionContext {
    public TerminalNode FIRST() {
      return getToken(CarbonSqlBaseParser.FIRST, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public TerminalNode IGNORE() {
      return getToken(CarbonSqlBaseParser.IGNORE, 0);
    }

    public TerminalNode NULLS() {
      return getToken(CarbonSqlBaseParser.NULLS, 0);
    }

    public FirstContext(PrimaryExpressionContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFirst(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PrimaryExpressionContext primaryExpression() throws RecognitionException {
    return primaryExpression(0);
  }

  private PrimaryExpressionContext primaryExpression(int _p) throws RecognitionException {
    ParserRuleContext _parentctx = _ctx;
    int _parentState = getState();
    PrimaryExpressionContext _localctx = new PrimaryExpressionContext(_ctx, _parentState);
    PrimaryExpressionContext _prevctx = _localctx;
    int _startState = 198;
    enterRecursionRule(_localctx, 198, RULE_primaryExpression, _p);
    int _la;
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2695);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 340, _ctx)) {
          case 1: {
            _localctx = new CurrentDatetimeContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;

            setState(2512);
            ((CurrentDatetimeContext) _localctx).name = _input.LT(1);
            _la = _input.LA(1);
            if (!(_la == CURRENT_DATE || _la == CURRENT_TIMESTAMP)) {
              ((CurrentDatetimeContext) _localctx).name = (Token) _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
          }
          break;
          case 2: {
            _localctx = new SearchedCaseContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2513);
            match(CASE);
            setState(2515);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(2514);
                  whenClause();
                }
              }
              setState(2517);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(2521);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(2519);
                match(ELSE);
                setState(2520);
                ((SearchedCaseContext) _localctx).elseExpression = expression();
              }
            }

            setState(2523);
            match(END);
          }
          break;
          case 3: {
            _localctx = new SimpleCaseContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2525);
            match(CASE);
            setState(2526);
            ((SimpleCaseContext) _localctx).value = expression();
            setState(2528);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(2527);
                  whenClause();
                }
              }
              setState(2530);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == WHEN);
            setState(2534);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == ELSE) {
              {
                setState(2532);
                match(ELSE);
                setState(2533);
                ((SimpleCaseContext) _localctx).elseExpression = expression();
              }
            }

            setState(2536);
            match(END);
          }
          break;
          case 4: {
            _localctx = new CastContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2538);
            match(CAST);
            setState(2539);
            match(T__1);
            setState(2540);
            expression();
            setState(2541);
            match(AS);
            setState(2542);
            dataType();
            setState(2543);
            match(T__2);
          }
          break;
          case 5: {
            _localctx = new StructContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2545);
            match(STRUCT);
            setState(2546);
            match(T__1);
            setState(2555);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 326, _ctx)) {
              case 1: {
                setState(2547);
                ((StructContext) _localctx).namedExpression = namedExpression();
                ((StructContext) _localctx).argument
                    .add(((StructContext) _localctx).namedExpression);
                setState(2552);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == T__3) {
                  {
                    {
                      setState(2548);
                      match(T__3);
                      setState(2549);
                      ((StructContext) _localctx).namedExpression = namedExpression();
                      ((StructContext) _localctx).argument
                          .add(((StructContext) _localctx).namedExpression);
                    }
                  }
                  setState(2554);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                }
              }
              break;
            }
            setState(2557);
            match(T__2);
          }
          break;
          case 6: {
            _localctx = new FirstContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2558);
            match(FIRST);
            setState(2559);
            match(T__1);
            setState(2560);
            expression();
            setState(2563);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == IGNORE) {
              {
                setState(2561);
                match(IGNORE);
                setState(2562);
                match(NULLS);
              }
            }

            setState(2565);
            match(T__2);
          }
          break;
          case 7: {
            _localctx = new LastContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2567);
            match(LAST);
            setState(2568);
            match(T__1);
            setState(2569);
            expression();
            setState(2572);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == IGNORE) {
              {
                setState(2570);
                match(IGNORE);
                setState(2571);
                match(NULLS);
              }
            }

            setState(2574);
            match(T__2);
          }
          break;
          case 8: {
            _localctx = new PositionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2576);
            match(POSITION);
            setState(2577);
            match(T__1);
            setState(2578);
            ((PositionContext) _localctx).substr = valueExpression(0);
            setState(2579);
            match(IN);
            setState(2580);
            ((PositionContext) _localctx).str = valueExpression(0);
            setState(2581);
            match(T__2);
          }
          break;
          case 9: {
            _localctx = new ConstantDefaultContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2583);
            constant();
          }
          break;
          case 10: {
            _localctx = new StarContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2584);
            match(ASTERISK);
          }
          break;
          case 11: {
            _localctx = new StarContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2585);
            qualifiedName();
            setState(2586);
            match(T__4);
            setState(2587);
            match(ASTERISK);
          }
          break;
          case 12: {
            _localctx = new RowConstructorContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2589);
            match(T__1);
            setState(2590);
            namedExpression();
            setState(2593);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(2591);
                  match(T__3);
                  setState(2592);
                  namedExpression();
                }
              }
              setState(2595);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == T__3);
            setState(2597);
            match(T__2);
          }
          break;
          case 13: {
            _localctx = new SubqueryExpressionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2599);
            match(T__1);
            setState(2600);
            query();
            setState(2601);
            match(T__2);
          }
          break;
          case 14: {
            _localctx = new FunctionCallContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2603);
            functionName();
            setState(2604);
            match(T__1);
            setState(2616);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 332, _ctx)) {
              case 1: {
                setState(2606);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 330, _ctx)) {
                  case 1: {
                    setState(2605);
                    setQuantifier();
                  }
                  break;
                }
                setState(2608);
                ((FunctionCallContext) _localctx).expression = expression();
                ((FunctionCallContext) _localctx).argument
                    .add(((FunctionCallContext) _localctx).expression);
                setState(2613);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == T__3) {
                  {
                    {
                      setState(2609);
                      match(T__3);
                      setState(2610);
                      ((FunctionCallContext) _localctx).expression = expression();
                      ((FunctionCallContext) _localctx).argument
                          .add(((FunctionCallContext) _localctx).expression);
                    }
                  }
                  setState(2615);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                }
              }
              break;
            }
            setState(2618);
            match(T__2);
            setState(2625);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 333, _ctx)) {
              case 1: {
                setState(2619);
                match(FILTER);
                setState(2620);
                match(T__1);
                setState(2621);
                match(WHERE);
                setState(2622);
                ((FunctionCallContext) _localctx).where = booleanExpression(0);
                setState(2623);
                match(T__2);
              }
              break;
            }
            setState(2629);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 334, _ctx)) {
              case 1: {
                setState(2627);
                match(OVER);
                setState(2628);
                windowSpec();
              }
              break;
            }
          }
          break;
          case 15: {
            _localctx = new LambdaContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2631);
            identifier();
            setState(2632);
            match(T__7);
            setState(2633);
            expression();
          }
          break;
          case 16: {
            _localctx = new LambdaContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2635);
            match(T__1);
            setState(2636);
            identifier();
            setState(2639);
            _errHandler.sync(this);
            _la = _input.LA(1);
            do {
              {
                {
                  setState(2637);
                  match(T__3);
                  setState(2638);
                  identifier();
                }
              }
              setState(2641);
              _errHandler.sync(this);
              _la = _input.LA(1);
            } while (_la == T__3);
            setState(2643);
            match(T__2);
            setState(2644);
            match(T__7);
            setState(2645);
            expression();
          }
          break;
          case 17: {
            _localctx = new ColumnReferenceContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2647);
            identifier();
          }
          break;
          case 18: {
            _localctx = new ParenthesizedExpressionContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2648);
            match(T__1);
            setState(2649);
            expression();
            setState(2650);
            match(T__2);
          }
          break;
          case 19: {
            _localctx = new ExtractContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2652);
            match(EXTRACT);
            setState(2653);
            match(T__1);
            setState(2654);
            ((ExtractContext) _localctx).field = identifier();
            setState(2655);
            match(FROM);
            setState(2656);
            ((ExtractContext) _localctx).source = valueExpression(0);
            setState(2657);
            match(T__2);
          }
          break;
          case 20: {
            _localctx = new SubstringContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2659);
            _la = _input.LA(1);
            if (!(_la == SUBSTR || _la == SUBSTRING)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(2660);
            match(T__1);
            setState(2661);
            ((SubstringContext) _localctx).str = valueExpression(0);
            setState(2662);
            _la = _input.LA(1);
            if (!(_la == T__3 || _la == FROM)) {
              _errHandler.recoverInline(this);
            } else {
              if (_input.LA(1) == Token.EOF) matchedEOF = true;
              _errHandler.reportMatch(this);
              consume();
            }
            setState(2663);
            ((SubstringContext) _localctx).pos = valueExpression(0);
            setState(2666);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == T__3 || _la == FOR) {
              {
                setState(2664);
                _la = _input.LA(1);
                if (!(_la == T__3 || _la == FOR)) {
                  _errHandler.recoverInline(this);
                } else {
                  if (_input.LA(1) == Token.EOF) matchedEOF = true;
                  _errHandler.reportMatch(this);
                  consume();
                }
                setState(2665);
                ((SubstringContext) _localctx).len = valueExpression(0);
              }
            }

            setState(2668);
            match(T__2);
          }
          break;
          case 21: {
            _localctx = new TrimContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2670);
            match(TRIM);
            setState(2671);
            match(T__1);
            setState(2673);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 337, _ctx)) {
              case 1: {
                setState(2672);
                ((TrimContext) _localctx).trimOption = _input.LT(1);
                _la = _input.LA(1);
                if (!(_la == BOTH || _la == LEADING || _la == TRAILING)) {
                  ((TrimContext) _localctx).trimOption = (Token) _errHandler.recoverInline(this);
                } else {
                  if (_input.LA(1) == Token.EOF) matchedEOF = true;
                  _errHandler.reportMatch(this);
                  consume();
                }
              }
              break;
            }
            setState(2676);
            _errHandler.sync(this);
            switch (getInterpreter().adaptivePredict(_input, 338, _ctx)) {
              case 1: {
                setState(2675);
                ((TrimContext) _localctx).trimStr = valueExpression(0);
              }
              break;
            }
            setState(2678);
            match(FROM);
            setState(2679);
            ((TrimContext) _localctx).srcStr = valueExpression(0);
            setState(2680);
            match(T__2);
          }
          break;
          case 22: {
            _localctx = new OverlayContext(_localctx);
            _ctx = _localctx;
            _prevctx = _localctx;
            setState(2682);
            match(OVERLAY);
            setState(2683);
            match(T__1);
            setState(2684);
            ((OverlayContext) _localctx).input = valueExpression(0);
            setState(2685);
            match(PLACING);
            setState(2686);
            ((OverlayContext) _localctx).replace = valueExpression(0);
            setState(2687);
            match(FROM);
            setState(2688);
            ((OverlayContext) _localctx).position = valueExpression(0);
            setState(2691);
            _errHandler.sync(this);
            _la = _input.LA(1);
            if (_la == FOR) {
              {
                setState(2689);
                match(FOR);
                setState(2690);
                ((OverlayContext) _localctx).length = valueExpression(0);
              }
            }

            setState(2693);
            match(T__2);
          }
          break;
        }
        _ctx.stop = _input.LT(-1);
        setState(2707);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 342, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            if (_parseListeners != null) triggerExitRuleEvent();
            _prevctx = _localctx;
            {
              setState(2705);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 341, _ctx)) {
                case 1: {
                  _localctx =
                      new SubscriptContext(new PrimaryExpressionContext(_parentctx, _parentState));
                  ((SubscriptContext) _localctx).value = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
                  setState(2697);
                  if (!(precpred(_ctx, 8)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 8)");
                  setState(2698);
                  match(T__8);
                  setState(2699);
                  ((SubscriptContext) _localctx).index = valueExpression(0);
                  setState(2700);
                  match(T__9);
                }
                break;
                case 2: {
                  _localctx = new DereferenceContext(
                      new PrimaryExpressionContext(_parentctx, _parentState));
                  ((DereferenceContext) _localctx).base = _prevctx;
                  pushNewRecursionContext(_localctx, _startState, RULE_primaryExpression);
                  setState(2702);
                  if (!(precpred(_ctx, 6)))
                    throw new FailedPredicateException(this, "precpred(_ctx, 6)");
                  setState(2703);
                  match(T__4);
                  setState(2704);
                  ((DereferenceContext) _localctx).fieldName = identifier();
                }
                break;
              }
            }
          }
          setState(2709);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 342, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      unrollRecursionContexts(_parentctx);
    }
    return _localctx;
  }

  public static class ConstantContext extends ParserRuleContext {
    public ConstantContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_constant;
    }

    public ConstantContext() {
    }

    public void copyFrom(ConstantContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class NullLiteralContext extends ConstantContext {
    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public NullLiteralContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNullLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class StringLiteralContext extends ConstantContext {
    public List<TerminalNode> STRING() {
      return getTokens(CarbonSqlBaseParser.STRING);
    }

    public TerminalNode STRING(int i) {
      return getToken(CarbonSqlBaseParser.STRING, i);
    }

    public StringLiteralContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStringLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TypeConstructorContext extends ConstantContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public TypeConstructorContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTypeConstructor(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class IntervalLiteralContext extends ConstantContext {
    public IntervalContext interval() {
      return getRuleContext(IntervalContext.class, 0);
    }

    public IntervalLiteralContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIntervalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class NumericLiteralContext extends ConstantContext {
    public NumberContext number() {
      return getRuleContext(NumberContext.class, 0);
    }

    public NumericLiteralContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNumericLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BooleanLiteralContext extends ConstantContext {
    public BooleanValueContext booleanValue() {
      return getRuleContext(BooleanValueContext.class, 0);
    }

    public BooleanLiteralContext(ConstantContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitBooleanLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ConstantContext constant() throws RecognitionException {
    ConstantContext _localctx = new ConstantContext(_ctx, getState());
    enterRule(_localctx, 200, RULE_constant);
    try {
      int _alt;
      setState(2722);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 344, _ctx)) {
        case 1:
          _localctx = new NullLiteralContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2710);
          match(NULL);
        }
        break;
        case 2:
          _localctx = new IntervalLiteralContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2711);
          interval();
        }
        break;
        case 3:
          _localctx = new TypeConstructorContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2712);
          identifier();
          setState(2713);
          match(STRING);
        }
        break;
        case 4:
          _localctx = new NumericLiteralContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(2715);
          number();
        }
        break;
        case 5:
          _localctx = new BooleanLiteralContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(2716);
          booleanValue();
        }
        break;
        case 6:
          _localctx = new StringLiteralContext(_localctx);
          enterOuterAlt(_localctx, 6);
        {
          setState(2718);
          _errHandler.sync(this);
          _alt = 1;
          do {
            switch (_alt) {
              case 1: {
                {
                  setState(2717);
                  match(STRING);
                }
              }
              break;
              default:
                throw new NoViableAltException(this);
            }
            setState(2720);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 343, _ctx);
          } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ComparisonOperatorContext extends ParserRuleContext {
    public TerminalNode EQ() {
      return getToken(CarbonSqlBaseParser.EQ, 0);
    }

    public TerminalNode NEQ() {
      return getToken(CarbonSqlBaseParser.NEQ, 0);
    }

    public TerminalNode NEQJ() {
      return getToken(CarbonSqlBaseParser.NEQJ, 0);
    }

    public TerminalNode LT() {
      return getToken(CarbonSqlBaseParser.LT, 0);
    }

    public TerminalNode LTE() {
      return getToken(CarbonSqlBaseParser.LTE, 0);
    }

    public TerminalNode GT() {
      return getToken(CarbonSqlBaseParser.GT, 0);
    }

    public TerminalNode GTE() {
      return getToken(CarbonSqlBaseParser.GTE, 0);
    }

    public TerminalNode NSEQ() {
      return getToken(CarbonSqlBaseParser.NSEQ, 0);
    }

    public ComparisonOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_comparisonOperator;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitComparisonOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComparisonOperatorContext comparisonOperator() throws RecognitionException {
    ComparisonOperatorContext _localctx = new ComparisonOperatorContext(_ctx, getState());
    enterRule(_localctx, 202, RULE_comparisonOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2724);
        _la = _input.LA(1);
        if (!(((((_la - 261)) & ~0x3f) == 0 &&
            ((1L << (_la - 261)) & ((1L << (EQ - 261)) | (1L << (NSEQ - 261)) | (1L << (NEQ - 261))
                | (1L << (NEQJ - 261)) | (1L << (LT - 261)) | (1L << (LTE - 261)) | (1L << (GT
                - 261)) | (1L << (GTE - 261)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ArithmeticOperatorContext extends ParserRuleContext {
    public TerminalNode PLUS() {
      return getToken(CarbonSqlBaseParser.PLUS, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public TerminalNode ASTERISK() {
      return getToken(CarbonSqlBaseParser.ASTERISK, 0);
    }

    public TerminalNode SLASH() {
      return getToken(CarbonSqlBaseParser.SLASH, 0);
    }

    public TerminalNode PERCENT() {
      return getToken(CarbonSqlBaseParser.PERCENT, 0);
    }

    public TerminalNode DIV() {
      return getToken(CarbonSqlBaseParser.DIV, 0);
    }

    public TerminalNode TILDE() {
      return getToken(CarbonSqlBaseParser.TILDE, 0);
    }

    public TerminalNode AMPERSAND() {
      return getToken(CarbonSqlBaseParser.AMPERSAND, 0);
    }

    public TerminalNode PIPE() {
      return getToken(CarbonSqlBaseParser.PIPE, 0);
    }

    public TerminalNode CONCAT_PIPE() {
      return getToken(CarbonSqlBaseParser.CONCAT_PIPE, 0);
    }

    public TerminalNode HAT() {
      return getToken(CarbonSqlBaseParser.HAT, 0);
    }

    public ArithmeticOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_arithmeticOperator;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitArithmeticOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ArithmeticOperatorContext arithmeticOperator() throws RecognitionException {
    ArithmeticOperatorContext _localctx = new ArithmeticOperatorContext(_ctx, getState());
    enterRule(_localctx, 204, RULE_arithmeticOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2726);
        _la = _input.LA(1);
        if (!(_la == DIV || ((((_la - 269)) & ~0x3f) == 0 &&
            ((1L << (_la - 269)) & ((1L << (PLUS - 269)) | (1L << (MINUS - 269)) | (1L << (ASTERISK
                - 269)) | (1L << (SLASH - 269)) | (1L << (PERCENT - 269)) | (1L << (TILDE - 269))
                | (1L << (AMPERSAND - 269)) | (1L << (PIPE - 269)) | (1L << (CONCAT_PIPE - 269)) | (
                1L << (HAT - 269)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class PredicateOperatorContext extends ParserRuleContext {
    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public PredicateOperatorContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_predicateOperator;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPredicateOperator(this);
      else return visitor.visitChildren(this);
    }
  }

  public final PredicateOperatorContext predicateOperator() throws RecognitionException {
    PredicateOperatorContext _localctx = new PredicateOperatorContext(_ctx, getState());
    enterRule(_localctx, 206, RULE_predicateOperator);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2728);
        _la = _input.LA(1);
        if (!(_la == AND || ((((_la - 112)) & ~0x3f) == 0 &&
            ((1L << (_la - 112)) & ((1L << (IN - 112)) | (1L << (NOT - 112)) | (1L << (OR - 112))))
                != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class BooleanValueContext extends ParserRuleContext {
    public TerminalNode TRUE() {
      return getToken(CarbonSqlBaseParser.TRUE, 0);
    }

    public TerminalNode FALSE() {
      return getToken(CarbonSqlBaseParser.FALSE, 0);
    }

    public BooleanValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_booleanValue;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitBooleanValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final BooleanValueContext booleanValue() throws RecognitionException {
    BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
    enterRule(_localctx, 208, RULE_booleanValue);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2730);
        _la = _input.LA(1);
        if (!(_la == FALSE || _la == TRUE)) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IntervalContext extends ParserRuleContext {
    public TerminalNode INTERVAL() {
      return getToken(CarbonSqlBaseParser.INTERVAL, 0);
    }

    public ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval() {
      return getRuleContext(ErrorCapturingMultiUnitsIntervalContext.class, 0);
    }

    public ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval() {
      return getRuleContext(ErrorCapturingUnitToUnitIntervalContext.class, 0);
    }

    public IntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_interval;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalContext interval() throws RecognitionException {
    IntervalContext _localctx = new IntervalContext(_ctx, getState());
    enterRule(_localctx, 210, RULE_interval);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2732);
        match(INTERVAL);
        setState(2735);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 345, _ctx)) {
          case 1: {
            setState(2733);
            errorCapturingMultiUnitsInterval();
          }
          break;
          case 2: {
            setState(2734);
            errorCapturingUnitToUnitInterval();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ErrorCapturingMultiUnitsIntervalContext extends ParserRuleContext {
    public MultiUnitsIntervalContext multiUnitsInterval() {
      return getRuleContext(MultiUnitsIntervalContext.class, 0);
    }

    public UnitToUnitIntervalContext unitToUnitInterval() {
      return getRuleContext(UnitToUnitIntervalContext.class, 0);
    }

    public ErrorCapturingMultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_errorCapturingMultiUnitsInterval;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor)
            .visitErrorCapturingMultiUnitsInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ErrorCapturingMultiUnitsIntervalContext errorCapturingMultiUnitsInterval()
      throws RecognitionException {
    ErrorCapturingMultiUnitsIntervalContext _localctx =
        new ErrorCapturingMultiUnitsIntervalContext(_ctx, getState());
    enterRule(_localctx, 212, RULE_errorCapturingMultiUnitsInterval);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2737);
        multiUnitsInterval();
        setState(2739);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 346, _ctx)) {
          case 1: {
            setState(2738);
            unitToUnitInterval();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class MultiUnitsIntervalContext extends ParserRuleContext {
    public IdentifierContext identifier;
    public List<IdentifierContext> unit = new ArrayList<IdentifierContext>();

    public List<IntervalValueContext> intervalValue() {
      return getRuleContexts(IntervalValueContext.class);
    }

    public IntervalValueContext intervalValue(int i) {
      return getRuleContext(IntervalValueContext.class, i);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public MultiUnitsIntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_multiUnitsInterval;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitMultiUnitsInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final MultiUnitsIntervalContext multiUnitsInterval() throws RecognitionException {
    MultiUnitsIntervalContext _localctx = new MultiUnitsIntervalContext(_ctx, getState());
    enterRule(_localctx, 214, RULE_multiUnitsInterval);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2744);
        _errHandler.sync(this);
        _alt = 1;
        do {
          switch (_alt) {
            case 1: {
              {
                setState(2741);
                intervalValue();
                setState(2742);
                ((MultiUnitsIntervalContext) _localctx).identifier = identifier();
                ((MultiUnitsIntervalContext) _localctx).unit
                    .add(((MultiUnitsIntervalContext) _localctx).identifier);
              }
            }
            break;
            default:
              throw new NoViableAltException(this);
          }
          setState(2746);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 347, _ctx);
        } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ErrorCapturingUnitToUnitIntervalContext extends ParserRuleContext {
    public UnitToUnitIntervalContext body;
    public MultiUnitsIntervalContext error1;
    public UnitToUnitIntervalContext error2;

    public List<UnitToUnitIntervalContext> unitToUnitInterval() {
      return getRuleContexts(UnitToUnitIntervalContext.class);
    }

    public UnitToUnitIntervalContext unitToUnitInterval(int i) {
      return getRuleContext(UnitToUnitIntervalContext.class, i);
    }

    public MultiUnitsIntervalContext multiUnitsInterval() {
      return getRuleContext(MultiUnitsIntervalContext.class, 0);
    }

    public ErrorCapturingUnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_errorCapturingUnitToUnitInterval;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor)
            .visitErrorCapturingUnitToUnitInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ErrorCapturingUnitToUnitIntervalContext errorCapturingUnitToUnitInterval()
      throws RecognitionException {
    ErrorCapturingUnitToUnitIntervalContext _localctx =
        new ErrorCapturingUnitToUnitIntervalContext(_ctx, getState());
    enterRule(_localctx, 216, RULE_errorCapturingUnitToUnitInterval);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2748);
        ((ErrorCapturingUnitToUnitIntervalContext) _localctx).body = unitToUnitInterval();
        setState(2751);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 348, _ctx)) {
          case 1: {
            setState(2749);
            ((ErrorCapturingUnitToUnitIntervalContext) _localctx).error1 = multiUnitsInterval();
          }
          break;
          case 2: {
            setState(2750);
            ((ErrorCapturingUnitToUnitIntervalContext) _localctx).error2 = unitToUnitInterval();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class UnitToUnitIntervalContext extends ParserRuleContext {
    public IntervalValueContext value;
    public IdentifierContext from;
    public IdentifierContext to;

    public TerminalNode TO() {
      return getToken(CarbonSqlBaseParser.TO, 0);
    }

    public IntervalValueContext intervalValue() {
      return getRuleContext(IntervalValueContext.class, 0);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public UnitToUnitIntervalContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_unitToUnitInterval;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUnitToUnitInterval(this);
      else return visitor.visitChildren(this);
    }
  }

  public final UnitToUnitIntervalContext unitToUnitInterval() throws RecognitionException {
    UnitToUnitIntervalContext _localctx = new UnitToUnitIntervalContext(_ctx, getState());
    enterRule(_localctx, 218, RULE_unitToUnitInterval);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2753);
        ((UnitToUnitIntervalContext) _localctx).value = intervalValue();
        setState(2754);
        ((UnitToUnitIntervalContext) _localctx).from = identifier();
        setState(2755);
        match(TO);
        setState(2756);
        ((UnitToUnitIntervalContext) _localctx).to = identifier();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IntervalValueContext extends ParserRuleContext {
    public TerminalNode INTEGER_VALUE() {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode DECIMAL_VALUE() {
      return getToken(CarbonSqlBaseParser.DECIMAL_VALUE, 0);
    }

    public TerminalNode PLUS() {
      return getToken(CarbonSqlBaseParser.PLUS, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public TerminalNode STRING() {
      return getToken(CarbonSqlBaseParser.STRING, 0);
    }

    public IntervalValueContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_intervalValue;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIntervalValue(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IntervalValueContext intervalValue() throws RecognitionException {
    IntervalValueContext _localctx = new IntervalValueContext(_ctx, getState());
    enterRule(_localctx, 220, RULE_intervalValue);
    int _la;
    try {
      setState(2763);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case PLUS:
        case MINUS:
        case INTEGER_VALUE:
        case DECIMAL_VALUE:
          enterOuterAlt(_localctx, 1);
        {
          setState(2759);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == PLUS || _la == MINUS) {
            {
              setState(2758);
              _la = _input.LA(1);
              if (!(_la == PLUS || _la == MINUS)) {
                _errHandler.recoverInline(this);
              } else {
                if (_input.LA(1) == Token.EOF) matchedEOF = true;
                _errHandler.reportMatch(this);
                consume();
              }
            }
          }

          setState(2761);
          _la = _input.LA(1);
          if (!(_la == INTEGER_VALUE || _la == DECIMAL_VALUE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case STRING:
          enterOuterAlt(_localctx, 2);
        {
          setState(2762);
          match(STRING);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ColPositionContext extends ParserRuleContext {
    public Token position;
    public ErrorCapturingIdentifierContext afterCol;

    public TerminalNode FIRST() {
      return getToken(CarbonSqlBaseParser.FIRST, 0);
    }

    public TerminalNode AFTER() {
      return getToken(CarbonSqlBaseParser.AFTER, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public ColPositionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_colPosition;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitColPosition(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ColPositionContext colPosition() throws RecognitionException {
    ColPositionContext _localctx = new ColPositionContext(_ctx, getState());
    enterRule(_localctx, 222, RULE_colPosition);
    try {
      setState(2768);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case FIRST:
          enterOuterAlt(_localctx, 1);
        {
          setState(2765);
          ((ColPositionContext) _localctx).position = match(FIRST);
        }
        break;
        case AFTER:
          enterOuterAlt(_localctx, 2);
        {
          setState(2766);
          ((ColPositionContext) _localctx).position = match(AFTER);
          setState(2767);
          ((ColPositionContext) _localctx).afterCol = errorCapturingIdentifier();
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class DataTypeContext extends ParserRuleContext {
    public DataTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_dataType;
    }

    public DataTypeContext() {
    }

    public void copyFrom(DataTypeContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ComplexDataTypeContext extends DataTypeContext {
    public Token complex;

    public TerminalNode LT() {
      return getToken(CarbonSqlBaseParser.LT, 0);
    }

    public List<DataTypeContext> dataType() {
      return getRuleContexts(DataTypeContext.class);
    }

    public DataTypeContext dataType(int i) {
      return getRuleContext(DataTypeContext.class, i);
    }

    public TerminalNode GT() {
      return getToken(CarbonSqlBaseParser.GT, 0);
    }

    public TerminalNode ARRAY() {
      return getToken(CarbonSqlBaseParser.ARRAY, 0);
    }

    public TerminalNode MAP() {
      return getToken(CarbonSqlBaseParser.MAP, 0);
    }

    public TerminalNode STRUCT() {
      return getToken(CarbonSqlBaseParser.STRUCT, 0);
    }

    public TerminalNode NEQ() {
      return getToken(CarbonSqlBaseParser.NEQ, 0);
    }

    public ComplexColTypeListContext complexColTypeList() {
      return getRuleContext(ComplexColTypeListContext.class, 0);
    }

    public ComplexDataTypeContext(DataTypeContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitComplexDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class PrimitiveDataTypeContext extends DataTypeContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public List<TerminalNode> INTEGER_VALUE() {
      return getTokens(CarbonSqlBaseParser.INTEGER_VALUE);
    }

    public TerminalNode INTEGER_VALUE(int i) {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, i);
    }

    public PrimitiveDataTypeContext(DataTypeContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitPrimitiveDataType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final DataTypeContext dataType() throws RecognitionException {
    DataTypeContext _localctx = new DataTypeContext(_ctx, getState());
    enterRule(_localctx, 224, RULE_dataType);
    int _la;
    try {
      setState(2804);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 356, _ctx)) {
        case 1:
          _localctx = new ComplexDataTypeContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2770);
          ((ComplexDataTypeContext) _localctx).complex = match(ARRAY);
          setState(2771);
          match(LT);
          setState(2772);
          dataType();
          setState(2773);
          match(GT);
        }
        break;
        case 2:
          _localctx = new ComplexDataTypeContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2775);
          ((ComplexDataTypeContext) _localctx).complex = match(MAP);
          setState(2776);
          match(LT);
          setState(2777);
          dataType();
          setState(2778);
          match(T__3);
          setState(2779);
          dataType();
          setState(2780);
          match(GT);
        }
        break;
        case 3:
          _localctx = new ComplexDataTypeContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2782);
          ((ComplexDataTypeContext) _localctx).complex = match(STRUCT);
          setState(2789);
          _errHandler.sync(this);
          switch (_input.LA(1)) {
            case LT: {
              setState(2783);
              match(LT);
              setState(2785);
              _errHandler.sync(this);
              switch (getInterpreter().adaptivePredict(_input, 352, _ctx)) {
                case 1: {
                  setState(2784);
                  complexColTypeList();
                }
                break;
              }
              setState(2787);
              match(GT);
            }
            break;
            case NEQ: {
              setState(2788);
              match(NEQ);
            }
            break;
            default:
              throw new NoViableAltException(this);
          }
        }
        break;
        case 4:
          _localctx = new PrimitiveDataTypeContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(2791);
          identifier();
          setState(2802);
          _errHandler.sync(this);
          switch (getInterpreter().adaptivePredict(_input, 355, _ctx)) {
            case 1: {
              setState(2792);
              match(T__1);
              setState(2793);
              match(INTEGER_VALUE);
              setState(2798);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(2794);
                    match(T__3);
                    setState(2795);
                    match(INTEGER_VALUE);
                  }
                }
                setState(2800);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
              setState(2801);
              match(T__2);
            }
            break;
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QualifiedColTypeWithPositionListContext extends ParserRuleContext {
    public List<QualifiedColTypeWithPositionContext> qualifiedColTypeWithPosition() {
      return getRuleContexts(QualifiedColTypeWithPositionContext.class);
    }

    public QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition(int i) {
      return getRuleContext(QualifiedColTypeWithPositionContext.class, i);
    }

    public QualifiedColTypeWithPositionListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_qualifiedColTypeWithPositionList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor)
            .visitQualifiedColTypeWithPositionList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedColTypeWithPositionListContext qualifiedColTypeWithPositionList()
      throws RecognitionException {
    QualifiedColTypeWithPositionListContext _localctx =
        new QualifiedColTypeWithPositionListContext(_ctx, getState());
    enterRule(_localctx, 226, RULE_qualifiedColTypeWithPositionList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2806);
        qualifiedColTypeWithPosition();
        setState(2811);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2807);
              match(T__3);
              setState(2808);
              qualifiedColTypeWithPosition();
            }
          }
          setState(2813);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QualifiedColTypeWithPositionContext extends ParserRuleContext {
    public MultipartIdentifierContext name;

    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public MultipartIdentifierContext multipartIdentifier() {
      return getRuleContext(MultipartIdentifierContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public CommentSpecContext commentSpec() {
      return getRuleContext(CommentSpecContext.class, 0);
    }

    public ColPositionContext colPosition() {
      return getRuleContext(ColPositionContext.class, 0);
    }

    public QualifiedColTypeWithPositionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_qualifiedColTypeWithPosition;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor)
            .visitQualifiedColTypeWithPosition(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedColTypeWithPositionContext qualifiedColTypeWithPosition()
      throws RecognitionException {
    QualifiedColTypeWithPositionContext _localctx =
        new QualifiedColTypeWithPositionContext(_ctx, getState());
    enterRule(_localctx, 228, RULE_qualifiedColTypeWithPosition);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2814);
        ((QualifiedColTypeWithPositionContext) _localctx).name = multipartIdentifier();
        setState(2815);
        dataType();
        setState(2818);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == NOT) {
          {
            setState(2816);
            match(NOT);
            setState(2817);
            match(NULL);
          }
        }

        setState(2821);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == COMMENT) {
          {
            setState(2820);
            commentSpec();
          }
        }

        setState(2824);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == AFTER || _la == FIRST) {
          {
            setState(2823);
            colPosition();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ColTypeListContext extends ParserRuleContext {
    public List<ColTypeContext> colType() {
      return getRuleContexts(ColTypeContext.class);
    }

    public ColTypeContext colType(int i) {
      return getRuleContext(ColTypeContext.class, i);
    }

    public ColTypeListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_colTypeList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitColTypeList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ColTypeListContext colTypeList() throws RecognitionException {
    ColTypeListContext _localctx = new ColTypeListContext(_ctx, getState());
    enterRule(_localctx, 230, RULE_colTypeList);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2826);
        colType();
        setState(2831);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 361, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2827);
                match(T__3);
                setState(2828);
                colType();
              }
            }
          }
          setState(2833);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 361, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ColTypeContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext colName;

    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public CommentSpecContext commentSpec() {
      return getRuleContext(CommentSpecContext.class, 0);
    }

    public ColTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_colType;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitColType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ColTypeContext colType() throws RecognitionException {
    ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
    enterRule(_localctx, 232, RULE_colType);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2834);
        ((ColTypeContext) _localctx).colName = errorCapturingIdentifier();
        setState(2835);
        dataType();
        setState(2838);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 362, _ctx)) {
          case 1: {
            setState(2836);
            match(NOT);
            setState(2837);
            match(NULL);
          }
          break;
        }
        setState(2841);
        _errHandler.sync(this);
        switch (getInterpreter().adaptivePredict(_input, 363, _ctx)) {
          case 1: {
            setState(2840);
            commentSpec();
          }
          break;
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ComplexColTypeListContext extends ParserRuleContext {
    public List<ComplexColTypeContext> complexColType() {
      return getRuleContexts(ComplexColTypeContext.class);
    }

    public ComplexColTypeContext complexColType(int i) {
      return getRuleContext(ComplexColTypeContext.class, i);
    }

    public ComplexColTypeListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_complexColTypeList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitComplexColTypeList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComplexColTypeListContext complexColTypeList() throws RecognitionException {
    ComplexColTypeListContext _localctx = new ComplexColTypeListContext(_ctx, getState());
    enterRule(_localctx, 234, RULE_complexColTypeList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2843);
        complexColType();
        setState(2848);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2844);
              match(T__3);
              setState(2845);
              complexColType();
            }
          }
          setState(2850);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ComplexColTypeContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public CommentSpecContext commentSpec() {
      return getRuleContext(CommentSpecContext.class, 0);
    }

    public ComplexColTypeContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_complexColType;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitComplexColType(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ComplexColTypeContext complexColType() throws RecognitionException {
    ComplexColTypeContext _localctx = new ComplexColTypeContext(_ctx, getState());
    enterRule(_localctx, 236, RULE_complexColType);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2851);
        identifier();
        setState(2852);
        match(T__10);
        setState(2853);
        dataType();
        setState(2856);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == NOT) {
          {
            setState(2854);
            match(NOT);
            setState(2855);
            match(NULL);
          }
        }

        setState(2859);
        _errHandler.sync(this);
        _la = _input.LA(1);
        if (_la == COMMENT) {
          {
            setState(2858);
            commentSpec();
          }
        }

      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WhenClauseContext extends ParserRuleContext {
    public ExpressionContext condition;
    public ExpressionContext result;

    public TerminalNode WHEN() {
      return getToken(CarbonSqlBaseParser.WHEN, 0);
    }

    public TerminalNode THEN() {
      return getToken(CarbonSqlBaseParser.THEN, 0);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public WhenClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_whenClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWhenClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WhenClauseContext whenClause() throws RecognitionException {
    WhenClauseContext _localctx = new WhenClauseContext(_ctx, getState());
    enterRule(_localctx, 238, RULE_whenClause);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2861);
        match(WHEN);
        setState(2862);
        ((WhenClauseContext) _localctx).condition = expression();
        setState(2863);
        match(THEN);
        setState(2864);
        ((WhenClauseContext) _localctx).result = expression();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WindowClauseContext extends ParserRuleContext {
    public TerminalNode WINDOW() {
      return getToken(CarbonSqlBaseParser.WINDOW, 0);
    }

    public List<NamedWindowContext> namedWindow() {
      return getRuleContexts(NamedWindowContext.class);
    }

    public NamedWindowContext namedWindow(int i) {
      return getRuleContext(NamedWindowContext.class, i);
    }

    public WindowClauseContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_windowClause;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWindowClause(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WindowClauseContext windowClause() throws RecognitionException {
    WindowClauseContext _localctx = new WindowClauseContext(_ctx, getState());
    enterRule(_localctx, 240, RULE_windowClause);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2866);
        match(WINDOW);
        setState(2867);
        namedWindow();
        setState(2872);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 367, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2868);
                match(T__3);
                setState(2869);
                namedWindow();
              }
            }
          }
          setState(2874);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 367, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NamedWindowContext extends ParserRuleContext {
    public ErrorCapturingIdentifierContext name;

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public WindowSpecContext windowSpec() {
      return getRuleContext(WindowSpecContext.class, 0);
    }

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public NamedWindowContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_namedWindow;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNamedWindow(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NamedWindowContext namedWindow() throws RecognitionException {
    NamedWindowContext _localctx = new NamedWindowContext(_ctx, getState());
    enterRule(_localctx, 242, RULE_namedWindow);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2875);
        ((NamedWindowContext) _localctx).name = errorCapturingIdentifier();
        setState(2876);
        match(AS);
        setState(2877);
        windowSpec();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WindowSpecContext extends ParserRuleContext {
    public WindowSpecContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_windowSpec;
    }

    public WindowSpecContext() {
    }

    public void copyFrom(WindowSpecContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class WindowRefContext extends WindowSpecContext {
    public ErrorCapturingIdentifierContext name;

    public ErrorCapturingIdentifierContext errorCapturingIdentifier() {
      return getRuleContext(ErrorCapturingIdentifierContext.class, 0);
    }

    public WindowRefContext(WindowSpecContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWindowRef(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class WindowDefContext extends WindowSpecContext {
    public ExpressionContext expression;
    public List<ExpressionContext> partition = new ArrayList<ExpressionContext>();

    public TerminalNode CLUSTER() {
      return getToken(CarbonSqlBaseParser.CLUSTER, 0);
    }

    public List<TerminalNode> BY() {
      return getTokens(CarbonSqlBaseParser.BY);
    }

    public TerminalNode BY(int i) {
      return getToken(CarbonSqlBaseParser.BY, i);
    }

    public List<ExpressionContext> expression() {
      return getRuleContexts(ExpressionContext.class);
    }

    public ExpressionContext expression(int i) {
      return getRuleContext(ExpressionContext.class, i);
    }

    public WindowFrameContext windowFrame() {
      return getRuleContext(WindowFrameContext.class, 0);
    }

    public List<SortItemContext> sortItem() {
      return getRuleContexts(SortItemContext.class);
    }

    public SortItemContext sortItem(int i) {
      return getRuleContext(SortItemContext.class, i);
    }

    public TerminalNode PARTITION() {
      return getToken(CarbonSqlBaseParser.PARTITION, 0);
    }

    public TerminalNode DISTRIBUTE() {
      return getToken(CarbonSqlBaseParser.DISTRIBUTE, 0);
    }

    public TerminalNode ORDER() {
      return getToken(CarbonSqlBaseParser.ORDER, 0);
    }

    public TerminalNode SORT() {
      return getToken(CarbonSqlBaseParser.SORT, 0);
    }

    public WindowDefContext(WindowSpecContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWindowDef(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WindowSpecContext windowSpec() throws RecognitionException {
    WindowSpecContext _localctx = new WindowSpecContext(_ctx, getState());
    enterRule(_localctx, 244, RULE_windowSpec);
    int _la;
    try {
      setState(2925);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 375, _ctx)) {
        case 1:
          _localctx = new WindowRefContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2879);
          ((WindowRefContext) _localctx).name = errorCapturingIdentifier();
        }
        break;
        case 2:
          _localctx = new WindowRefContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2880);
          match(T__1);
          setState(2881);
          ((WindowRefContext) _localctx).name = errorCapturingIdentifier();
          setState(2882);
          match(T__2);
        }
        break;
        case 3:
          _localctx = new WindowDefContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2884);
          match(T__1);
          setState(2919);
          _errHandler.sync(this);
          switch (_input.LA(1)) {
            case CLUSTER: {
              setState(2885);
              match(CLUSTER);
              setState(2886);
              match(BY);
              setState(2887);
              ((WindowDefContext) _localctx).expression = expression();
              ((WindowDefContext) _localctx).partition
                  .add(((WindowDefContext) _localctx).expression);
              setState(2892);
              _errHandler.sync(this);
              _la = _input.LA(1);
              while (_la == T__3) {
                {
                  {
                    setState(2888);
                    match(T__3);
                    setState(2889);
                    ((WindowDefContext) _localctx).expression = expression();
                    ((WindowDefContext) _localctx).partition
                        .add(((WindowDefContext) _localctx).expression);
                  }
                }
                setState(2894);
                _errHandler.sync(this);
                _la = _input.LA(1);
              }
            }
            break;
            case T__2:
            case DISTRIBUTE:
            case ORDER:
            case PARTITION:
            case RANGE:
            case ROWS:
            case SORT: {
              setState(2905);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == DISTRIBUTE || _la == PARTITION) {
                {
                  setState(2895);
                  _la = _input.LA(1);
                  if (!(_la == DISTRIBUTE || _la == PARTITION)) {
                    _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(2896);
                  match(BY);
                  setState(2897);
                  ((WindowDefContext) _localctx).expression = expression();
                  ((WindowDefContext) _localctx).partition
                      .add(((WindowDefContext) _localctx).expression);
                  setState(2902);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  while (_la == T__3) {
                    {
                      {
                        setState(2898);
                        match(T__3);
                        setState(2899);
                        ((WindowDefContext) _localctx).expression = expression();
                        ((WindowDefContext) _localctx).partition
                            .add(((WindowDefContext) _localctx).expression);
                      }
                    }
                    setState(2904);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                  }
                }
              }

              setState(2917);
              _errHandler.sync(this);
              _la = _input.LA(1);
              if (_la == ORDER || _la == SORT) {
                {
                  setState(2907);
                  _la = _input.LA(1);
                  if (!(_la == ORDER || _la == SORT)) {
                    _errHandler.recoverInline(this);
                  } else {
                    if (_input.LA(1) == Token.EOF) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                  }
                  setState(2908);
                  match(BY);
                  setState(2909);
                  sortItem();
                  setState(2914);
                  _errHandler.sync(this);
                  _la = _input.LA(1);
                  while (_la == T__3) {
                    {
                      {
                        setState(2910);
                        match(T__3);
                        setState(2911);
                        sortItem();
                      }
                    }
                    setState(2916);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                  }
                }
              }

            }
            break;
            default:
              throw new NoViableAltException(this);
          }
          setState(2922);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == RANGE || _la == ROWS) {
            {
              setState(2921);
              windowFrame();
            }
          }

          setState(2924);
          match(T__2);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class WindowFrameContext extends ParserRuleContext {
    public Token frameType;
    public FrameBoundContext start;
    public FrameBoundContext end;

    public TerminalNode RANGE() {
      return getToken(CarbonSqlBaseParser.RANGE, 0);
    }

    public List<FrameBoundContext> frameBound() {
      return getRuleContexts(FrameBoundContext.class);
    }

    public FrameBoundContext frameBound(int i) {
      return getRuleContext(FrameBoundContext.class, i);
    }

    public TerminalNode ROWS() {
      return getToken(CarbonSqlBaseParser.ROWS, 0);
    }

    public TerminalNode BETWEEN() {
      return getToken(CarbonSqlBaseParser.BETWEEN, 0);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public WindowFrameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_windowFrame;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitWindowFrame(this);
      else return visitor.visitChildren(this);
    }
  }

  public final WindowFrameContext windowFrame() throws RecognitionException {
    WindowFrameContext _localctx = new WindowFrameContext(_ctx, getState());
    enterRule(_localctx, 246, RULE_windowFrame);
    try {
      setState(2943);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 376, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2927);
          ((WindowFrameContext) _localctx).frameType = match(RANGE);
          setState(2928);
          ((WindowFrameContext) _localctx).start = frameBound();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2929);
          ((WindowFrameContext) _localctx).frameType = match(ROWS);
          setState(2930);
          ((WindowFrameContext) _localctx).start = frameBound();
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(2931);
          ((WindowFrameContext) _localctx).frameType = match(RANGE);
          setState(2932);
          match(BETWEEN);
          setState(2933);
          ((WindowFrameContext) _localctx).start = frameBound();
          setState(2934);
          match(AND);
          setState(2935);
          ((WindowFrameContext) _localctx).end = frameBound();
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(2937);
          ((WindowFrameContext) _localctx).frameType = match(ROWS);
          setState(2938);
          match(BETWEEN);
          setState(2939);
          ((WindowFrameContext) _localctx).start = frameBound();
          setState(2940);
          match(AND);
          setState(2941);
          ((WindowFrameContext) _localctx).end = frameBound();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FrameBoundContext extends ParserRuleContext {
    public Token boundType;

    public TerminalNode UNBOUNDED() {
      return getToken(CarbonSqlBaseParser.UNBOUNDED, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(CarbonSqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(CarbonSqlBaseParser.FOLLOWING, 0);
    }

    public TerminalNode ROW() {
      return getToken(CarbonSqlBaseParser.ROW, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(CarbonSqlBaseParser.CURRENT, 0);
    }

    public ExpressionContext expression() {
      return getRuleContext(ExpressionContext.class, 0);
    }

    public FrameBoundContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_frameBound;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFrameBound(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FrameBoundContext frameBound() throws RecognitionException {
    FrameBoundContext _localctx = new FrameBoundContext(_ctx, getState());
    enterRule(_localctx, 248, RULE_frameBound);
    int _la;
    try {
      setState(2952);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 377, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2945);
          match(UNBOUNDED);
          setState(2946);
          ((FrameBoundContext) _localctx).boundType = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == FOLLOWING || _la == PRECEDING)) {
            ((FrameBoundContext) _localctx).boundType = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2947);
          ((FrameBoundContext) _localctx).boundType = match(CURRENT);
          setState(2948);
          match(ROW);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(2949);
          expression();
          setState(2950);
          ((FrameBoundContext) _localctx).boundType = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == FOLLOWING || _la == PRECEDING)) {
            ((FrameBoundContext) _localctx).boundType = (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QualifiedNameListContext extends ParserRuleContext {
    public List<QualifiedNameContext> qualifiedName() {
      return getRuleContexts(QualifiedNameContext.class);
    }

    public QualifiedNameContext qualifiedName(int i) {
      return getRuleContext(QualifiedNameContext.class, i);
    }

    public QualifiedNameListContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_qualifiedNameList;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQualifiedNameList(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameListContext qualifiedNameList() throws RecognitionException {
    QualifiedNameListContext _localctx = new QualifiedNameListContext(_ctx, getState());
    enterRule(_localctx, 250, RULE_qualifiedNameList);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2954);
        qualifiedName();
        setState(2959);
        _errHandler.sync(this);
        _la = _input.LA(1);
        while (_la == T__3) {
          {
            {
              setState(2955);
              match(T__3);
              setState(2956);
              qualifiedName();
            }
          }
          setState(2961);
          _errHandler.sync(this);
          _la = _input.LA(1);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class FunctionNameContext extends ParserRuleContext {
    public QualifiedNameContext qualifiedName() {
      return getRuleContext(QualifiedNameContext.class, 0);
    }

    public TerminalNode FILTER() {
      return getToken(CarbonSqlBaseParser.FILTER, 0);
    }

    public TerminalNode LEFT() {
      return getToken(CarbonSqlBaseParser.LEFT, 0);
    }

    public TerminalNode RIGHT() {
      return getToken(CarbonSqlBaseParser.RIGHT, 0);
    }

    public FunctionNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_functionName;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFunctionName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final FunctionNameContext functionName() throws RecognitionException {
    FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
    enterRule(_localctx, 252, RULE_functionName);
    try {
      setState(2966);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 379, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2962);
          qualifiedName();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2963);
          match(FILTER);
        }
        break;
        case 3:
          enterOuterAlt(_localctx, 3);
        {
          setState(2964);
          match(LEFT);
        }
        break;
        case 4:
          enterOuterAlt(_localctx, 4);
        {
          setState(2965);
          match(RIGHT);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QualifiedNameContext extends ParserRuleContext {
    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public QualifiedNameContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_qualifiedName;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQualifiedName(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QualifiedNameContext qualifiedName() throws RecognitionException {
    QualifiedNameContext _localctx = new QualifiedNameContext(_ctx, getState());
    enterRule(_localctx, 254, RULE_qualifiedName);
    try {
      int _alt;
      enterOuterAlt(_localctx, 1);
      {
        setState(2968);
        identifier();
        setState(2973);
        _errHandler.sync(this);
        _alt = getInterpreter().adaptivePredict(_input, 380, _ctx);
        while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
          if (_alt == 1) {
            {
              {
                setState(2969);
                match(T__4);
                setState(2970);
                identifier();
              }
            }
          }
          setState(2975);
          _errHandler.sync(this);
          _alt = getInterpreter().adaptivePredict(_input, 380, _ctx);
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ErrorCapturingIdentifierContext extends ParserRuleContext {
    public IdentifierContext identifier() {
      return getRuleContext(IdentifierContext.class, 0);
    }

    public ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra() {
      return getRuleContext(ErrorCapturingIdentifierExtraContext.class, 0);
    }

    public ErrorCapturingIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_errorCapturingIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitErrorCapturingIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ErrorCapturingIdentifierContext errorCapturingIdentifier()
      throws RecognitionException {
    ErrorCapturingIdentifierContext _localctx =
        new ErrorCapturingIdentifierContext(_ctx, getState());
    enterRule(_localctx, 256, RULE_errorCapturingIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(2976);
        identifier();
        setState(2977);
        errorCapturingIdentifierExtra();
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class ErrorCapturingIdentifierExtraContext extends ParserRuleContext {
    public ErrorCapturingIdentifierExtraContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_errorCapturingIdentifierExtra;
    }

    public ErrorCapturingIdentifierExtraContext() {
    }

    public void copyFrom(ErrorCapturingIdentifierExtraContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class ErrorIdentContext extends ErrorCapturingIdentifierExtraContext {
    public List<TerminalNode> MINUS() {
      return getTokens(CarbonSqlBaseParser.MINUS);
    }

    public TerminalNode MINUS(int i) {
      return getToken(CarbonSqlBaseParser.MINUS, i);
    }

    public List<IdentifierContext> identifier() {
      return getRuleContexts(IdentifierContext.class);
    }

    public IdentifierContext identifier(int i) {
      return getRuleContext(IdentifierContext.class, i);
    }

    public ErrorIdentContext(ErrorCapturingIdentifierExtraContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitErrorIdent(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class RealIdentContext extends ErrorCapturingIdentifierExtraContext {
    public RealIdentContext(ErrorCapturingIdentifierExtraContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitRealIdent(this);
      else return visitor.visitChildren(this);
    }
  }

  public final ErrorCapturingIdentifierExtraContext errorCapturingIdentifierExtra()
      throws RecognitionException {
    ErrorCapturingIdentifierExtraContext _localctx =
        new ErrorCapturingIdentifierExtraContext(_ctx, getState());
    enterRule(_localctx, 258, RULE_errorCapturingIdentifierExtra);
    try {
      int _alt;
      setState(2986);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 382, _ctx)) {
        case 1:
          _localctx = new ErrorIdentContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2981);
          _errHandler.sync(this);
          _alt = 1;
          do {
            switch (_alt) {
              case 1: {
                {
                  setState(2979);
                  match(MINUS);
                  setState(2980);
                  identifier();
                }
              }
              break;
              default:
                throw new NoViableAltException(this);
            }
            setState(2983);
            _errHandler.sync(this);
            _alt = getInterpreter().adaptivePredict(_input, 381, _ctx);
          } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
        }
        break;
        case 2:
          _localctx = new RealIdentContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class IdentifierContext extends ParserRuleContext {
    public StrictIdentifierContext strictIdentifier() {
      return getRuleContext(StrictIdentifierContext.class, 0);
    }

    public StrictNonReservedContext strictNonReserved() {
      return getRuleContext(StrictNonReservedContext.class, 0);
    }

    public IdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_identifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final IdentifierContext identifier() throws RecognitionException {
    IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
    enterRule(_localctx, 260, RULE_identifier);
    try {
      setState(2991);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 383, _ctx)) {
        case 1:
          enterOuterAlt(_localctx, 1);
        {
          setState(2988);
          strictIdentifier();
        }
        break;
        case 2:
          enterOuterAlt(_localctx, 2);
        {
          setState(2989);
          if (!(!SQL_standard_keyword_behavior))
            throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
          setState(2990);
          strictNonReserved();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StrictIdentifierContext extends ParserRuleContext {
    public StrictIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_strictIdentifier;
    }

    public StrictIdentifierContext() {
    }

    public void copyFrom(StrictIdentifierContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class QuotedIdentifierAlternativeContext extends StrictIdentifierContext {
    public QuotedIdentifierContext quotedIdentifier() {
      return getRuleContext(QuotedIdentifierContext.class, 0);
    }

    public QuotedIdentifierAlternativeContext(StrictIdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQuotedIdentifierAlternative(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class UnquotedIdentifierContext extends StrictIdentifierContext {
    public TerminalNode IDENTIFIER() {
      return getToken(CarbonSqlBaseParser.IDENTIFIER, 0);
    }

    public AnsiNonReservedContext ansiNonReserved() {
      return getRuleContext(AnsiNonReservedContext.class, 0);
    }

    public NonReservedContext nonReserved() {
      return getRuleContext(NonReservedContext.class, 0);
    }

    public UnquotedIdentifierContext(StrictIdentifierContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitUnquotedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StrictIdentifierContext strictIdentifier() throws RecognitionException {
    StrictIdentifierContext _localctx = new StrictIdentifierContext(_ctx, getState());
    enterRule(_localctx, 262, RULE_strictIdentifier);
    try {
      setState(2999);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 384, _ctx)) {
        case 1:
          _localctx = new UnquotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(2993);
          match(IDENTIFIER);
        }
        break;
        case 2:
          _localctx = new QuotedIdentifierAlternativeContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(2994);
          quotedIdentifier();
        }
        break;
        case 3:
          _localctx = new UnquotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(2995);
          if (!(SQL_standard_keyword_behavior))
            throw new FailedPredicateException(this, "SQL_standard_keyword_behavior");
          setState(2996);
          ansiNonReserved();
        }
        break;
        case 4:
          _localctx = new UnquotedIdentifierContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(2997);
          if (!(!SQL_standard_keyword_behavior))
            throw new FailedPredicateException(this, "!SQL_standard_keyword_behavior");
          setState(2998);
          nonReserved();
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class QuotedIdentifierContext extends ParserRuleContext {
    public TerminalNode BACKQUOTED_IDENTIFIER() {
      return getToken(CarbonSqlBaseParser.BACKQUOTED_IDENTIFIER, 0);
    }

    public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_quotedIdentifier;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitQuotedIdentifier(this);
      else return visitor.visitChildren(this);
    }
  }

  public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
    QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
    enterRule(_localctx, 264, RULE_quotedIdentifier);
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(3001);
        match(BACKQUOTED_IDENTIFIER);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NumberContext extends ParserRuleContext {
    public NumberContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_number;
    }

    public NumberContext() {
    }

    public void copyFrom(NumberContext ctx) {
      super.copyFrom(ctx);
    }
  }

  public static class DecimalLiteralContext extends NumberContext {
    public TerminalNode DECIMAL_VALUE() {
      return getToken(CarbonSqlBaseParser.DECIMAL_VALUE, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public DecimalLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BigIntLiteralContext extends NumberContext {
    public TerminalNode BIGINT_LITERAL() {
      return getToken(CarbonSqlBaseParser.BIGINT_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public BigIntLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitBigIntLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class TinyIntLiteralContext extends NumberContext {
    public TerminalNode TINYINT_LITERAL() {
      return getToken(CarbonSqlBaseParser.TINYINT_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public TinyIntLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitTinyIntLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class LegacyDecimalLiteralContext extends NumberContext {
    public TerminalNode EXPONENT_VALUE() {
      return getToken(CarbonSqlBaseParser.EXPONENT_VALUE, 0);
    }

    public TerminalNode DECIMAL_VALUE() {
      return getToken(CarbonSqlBaseParser.DECIMAL_VALUE, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public LegacyDecimalLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitLegacyDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class BigDecimalLiteralContext extends NumberContext {
    public TerminalNode BIGDECIMAL_LITERAL() {
      return getToken(CarbonSqlBaseParser.BIGDECIMAL_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public BigDecimalLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitBigDecimalLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class ExponentLiteralContext extends NumberContext {
    public TerminalNode EXPONENT_VALUE() {
      return getToken(CarbonSqlBaseParser.EXPONENT_VALUE, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public ExponentLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitExponentLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class DoubleLiteralContext extends NumberContext {
    public TerminalNode DOUBLE_LITERAL() {
      return getToken(CarbonSqlBaseParser.DOUBLE_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public DoubleLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitDoubleLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class IntegerLiteralContext extends NumberContext {
    public TerminalNode INTEGER_VALUE() {
      return getToken(CarbonSqlBaseParser.INTEGER_VALUE, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public IntegerLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitIntegerLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class FloatLiteralContext extends NumberContext {
    public TerminalNode FLOAT_LITERAL() {
      return getToken(CarbonSqlBaseParser.FLOAT_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public FloatLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitFloatLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public static class SmallIntLiteralContext extends NumberContext {
    public TerminalNode SMALLINT_LITERAL() {
      return getToken(CarbonSqlBaseParser.SMALLINT_LITERAL, 0);
    }

    public TerminalNode MINUS() {
      return getToken(CarbonSqlBaseParser.MINUS, 0);
    }

    public SmallIntLiteralContext(NumberContext ctx) {
      copyFrom(ctx);
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitSmallIntLiteral(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NumberContext number() throws RecognitionException {
    NumberContext _localctx = new NumberContext(_ctx, getState());
    enterRule(_localctx, 266, RULE_number);
    int _la;
    try {
      setState(3046);
      _errHandler.sync(this);
      switch (getInterpreter().adaptivePredict(_input, 395, _ctx)) {
        case 1:
          _localctx = new ExponentLiteralContext(_localctx);
          enterOuterAlt(_localctx, 1);
        {
          setState(3003);
          if (!(!legacy_exponent_literal_as_decimal_enabled))
            throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
          setState(3005);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3004);
              match(MINUS);
            }
          }

          setState(3007);
          match(EXPONENT_VALUE);
        }
        break;
        case 2:
          _localctx = new DecimalLiteralContext(_localctx);
          enterOuterAlt(_localctx, 2);
        {
          setState(3008);
          if (!(!legacy_exponent_literal_as_decimal_enabled))
            throw new FailedPredicateException(this, "!legacy_exponent_literal_as_decimal_enabled");
          setState(3010);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3009);
              match(MINUS);
            }
          }

          setState(3012);
          match(DECIMAL_VALUE);
        }
        break;
        case 3:
          _localctx = new LegacyDecimalLiteralContext(_localctx);
          enterOuterAlt(_localctx, 3);
        {
          setState(3013);
          if (!(legacy_exponent_literal_as_decimal_enabled))
            throw new FailedPredicateException(this, "legacy_exponent_literal_as_decimal_enabled");
          setState(3015);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3014);
              match(MINUS);
            }
          }

          setState(3017);
          _la = _input.LA(1);
          if (!(_la == EXPONENT_VALUE || _la == DECIMAL_VALUE)) {
            _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
        }
        break;
        case 4:
          _localctx = new IntegerLiteralContext(_localctx);
          enterOuterAlt(_localctx, 4);
        {
          setState(3019);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3018);
              match(MINUS);
            }
          }

          setState(3021);
          match(INTEGER_VALUE);
        }
        break;
        case 5:
          _localctx = new BigIntLiteralContext(_localctx);
          enterOuterAlt(_localctx, 5);
        {
          setState(3023);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3022);
              match(MINUS);
            }
          }

          setState(3025);
          match(BIGINT_LITERAL);
        }
        break;
        case 6:
          _localctx = new SmallIntLiteralContext(_localctx);
          enterOuterAlt(_localctx, 6);
        {
          setState(3027);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3026);
              match(MINUS);
            }
          }

          setState(3029);
          match(SMALLINT_LITERAL);
        }
        break;
        case 7:
          _localctx = new TinyIntLiteralContext(_localctx);
          enterOuterAlt(_localctx, 7);
        {
          setState(3031);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3030);
              match(MINUS);
            }
          }

          setState(3033);
          match(TINYINT_LITERAL);
        }
        break;
        case 8:
          _localctx = new DoubleLiteralContext(_localctx);
          enterOuterAlt(_localctx, 8);
        {
          setState(3035);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3034);
              match(MINUS);
            }
          }

          setState(3037);
          match(DOUBLE_LITERAL);
        }
        break;
        case 9:
          _localctx = new FloatLiteralContext(_localctx);
          enterOuterAlt(_localctx, 9);
        {
          setState(3039);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3038);
              match(MINUS);
            }
          }

          setState(3041);
          match(FLOAT_LITERAL);
        }
        break;
        case 10:
          _localctx = new BigDecimalLiteralContext(_localctx);
          enterOuterAlt(_localctx, 10);
        {
          setState(3043);
          _errHandler.sync(this);
          _la = _input.LA(1);
          if (_la == MINUS) {
            {
              setState(3042);
              match(MINUS);
            }
          }

          setState(3045);
          match(BIGDECIMAL_LITERAL);
        }
        break;
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AlterColumnActionContext extends ParserRuleContext {
    public Token setOrDrop;

    public TerminalNode TYPE() {
      return getToken(CarbonSqlBaseParser.TYPE, 0);
    }

    public DataTypeContext dataType() {
      return getRuleContext(DataTypeContext.class, 0);
    }

    public CommentSpecContext commentSpec() {
      return getRuleContext(CommentSpecContext.class, 0);
    }

    public ColPositionContext colPosition() {
      return getRuleContext(ColPositionContext.class, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public AlterColumnActionContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_alterColumnAction;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAlterColumnAction(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AlterColumnActionContext alterColumnAction() throws RecognitionException {
    AlterColumnActionContext _localctx = new AlterColumnActionContext(_ctx, getState());
    enterRule(_localctx, 268, RULE_alterColumnAction);
    int _la;
    try {
      setState(3055);
      _errHandler.sync(this);
      switch (_input.LA(1)) {
        case TYPE:
          enterOuterAlt(_localctx, 1);
        {
          setState(3048);
          match(TYPE);
          setState(3049);
          dataType();
        }
        break;
        case COMMENT:
          enterOuterAlt(_localctx, 2);
        {
          setState(3050);
          commentSpec();
        }
        break;
        case AFTER:
        case FIRST:
          enterOuterAlt(_localctx, 3);
        {
          setState(3051);
          colPosition();
        }
        break;
        case DROP:
        case SET:
          enterOuterAlt(_localctx, 4);
        {
          setState(3052);
          ((AlterColumnActionContext) _localctx).setOrDrop = _input.LT(1);
          _la = _input.LA(1);
          if (!(_la == DROP || _la == SET)) {
            ((AlterColumnActionContext) _localctx).setOrDrop =
                (Token) _errHandler.recoverInline(this);
          } else {
            if (_input.LA(1) == Token.EOF) matchedEOF = true;
            _errHandler.reportMatch(this);
            consume();
          }
          setState(3053);
          match(NOT);
          setState(3054);
          match(NULL);
        }
        break;
        default:
          throw new NoViableAltException(this);
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class AnsiNonReservedContext extends ParserRuleContext {
    public TerminalNode ADD() {
      return getToken(CarbonSqlBaseParser.ADD, 0);
    }

    public TerminalNode AFTER() {
      return getToken(CarbonSqlBaseParser.AFTER, 0);
    }

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode ANALYZE() {
      return getToken(CarbonSqlBaseParser.ANALYZE, 0);
    }

    public TerminalNode ANTI() {
      return getToken(CarbonSqlBaseParser.ANTI, 0);
    }

    public TerminalNode ARCHIVE() {
      return getToken(CarbonSqlBaseParser.ARCHIVE, 0);
    }

    public TerminalNode ARRAY() {
      return getToken(CarbonSqlBaseParser.ARRAY, 0);
    }

    public TerminalNode ASC() {
      return getToken(CarbonSqlBaseParser.ASC, 0);
    }

    public TerminalNode AT() {
      return getToken(CarbonSqlBaseParser.AT, 0);
    }

    public TerminalNode BETWEEN() {
      return getToken(CarbonSqlBaseParser.BETWEEN, 0);
    }

    public TerminalNode BUCKET() {
      return getToken(CarbonSqlBaseParser.BUCKET, 0);
    }

    public TerminalNode BUCKETS() {
      return getToken(CarbonSqlBaseParser.BUCKETS, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public TerminalNode CACHE() {
      return getToken(CarbonSqlBaseParser.CACHE, 0);
    }

    public TerminalNode CASCADE() {
      return getToken(CarbonSqlBaseParser.CASCADE, 0);
    }

    public TerminalNode CHANGE() {
      return getToken(CarbonSqlBaseParser.CHANGE, 0);
    }

    public TerminalNode CLEAR() {
      return getToken(CarbonSqlBaseParser.CLEAR, 0);
    }

    public TerminalNode CLUSTER() {
      return getToken(CarbonSqlBaseParser.CLUSTER, 0);
    }

    public TerminalNode CLUSTERED() {
      return getToken(CarbonSqlBaseParser.CLUSTERED, 0);
    }

    public TerminalNode CODEGEN() {
      return getToken(CarbonSqlBaseParser.CODEGEN, 0);
    }

    public TerminalNode COLLECTION() {
      return getToken(CarbonSqlBaseParser.COLLECTION, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public TerminalNode COMMENT() {
      return getToken(CarbonSqlBaseParser.COMMENT, 0);
    }

    public TerminalNode COMMIT() {
      return getToken(CarbonSqlBaseParser.COMMIT, 0);
    }

    public TerminalNode COMPACT() {
      return getToken(CarbonSqlBaseParser.COMPACT, 0);
    }

    public TerminalNode COMPACTIONS() {
      return getToken(CarbonSqlBaseParser.COMPACTIONS, 0);
    }

    public TerminalNode COMPUTE() {
      return getToken(CarbonSqlBaseParser.COMPUTE, 0);
    }

    public TerminalNode CONCATENATE() {
      return getToken(CarbonSqlBaseParser.CONCATENATE, 0);
    }

    public TerminalNode COST() {
      return getToken(CarbonSqlBaseParser.COST, 0);
    }

    public TerminalNode CUBE() {
      return getToken(CarbonSqlBaseParser.CUBE, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(CarbonSqlBaseParser.CURRENT, 0);
    }

    public TerminalNode DATA() {
      return getToken(CarbonSqlBaseParser.DATA, 0);
    }

    public TerminalNode DATABASE() {
      return getToken(CarbonSqlBaseParser.DATABASE, 0);
    }

    public TerminalNode DATABASES() {
      return getToken(CarbonSqlBaseParser.DATABASES, 0);
    }

    public TerminalNode DBPROPERTIES() {
      return getToken(CarbonSqlBaseParser.DBPROPERTIES, 0);
    }

    public TerminalNode DEFINED() {
      return getToken(CarbonSqlBaseParser.DEFINED, 0);
    }

    public TerminalNode DELETE() {
      return getToken(CarbonSqlBaseParser.DELETE, 0);
    }

    public TerminalNode DELIMITED() {
      return getToken(CarbonSqlBaseParser.DELIMITED, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode DFS() {
      return getToken(CarbonSqlBaseParser.DFS, 0);
    }

    public TerminalNode DIRECTORIES() {
      return getToken(CarbonSqlBaseParser.DIRECTORIES, 0);
    }

    public TerminalNode DIRECTORY() {
      return getToken(CarbonSqlBaseParser.DIRECTORY, 0);
    }

    public TerminalNode DISTRIBUTE() {
      return getToken(CarbonSqlBaseParser.DISTRIBUTE, 0);
    }

    public TerminalNode DIV() {
      return getToken(CarbonSqlBaseParser.DIV, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode ESCAPED() {
      return getToken(CarbonSqlBaseParser.ESCAPED, 0);
    }

    public TerminalNode EXCHANGE() {
      return getToken(CarbonSqlBaseParser.EXCHANGE, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode EXPLAIN() {
      return getToken(CarbonSqlBaseParser.EXPLAIN, 0);
    }

    public TerminalNode EXPORT() {
      return getToken(CarbonSqlBaseParser.EXPORT, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public TerminalNode EXTERNAL() {
      return getToken(CarbonSqlBaseParser.EXTERNAL, 0);
    }

    public TerminalNode EXTRACT() {
      return getToken(CarbonSqlBaseParser.EXTRACT, 0);
    }

    public TerminalNode FIELDS() {
      return getToken(CarbonSqlBaseParser.FIELDS, 0);
    }

    public TerminalNode FILEFORMAT() {
      return getToken(CarbonSqlBaseParser.FILEFORMAT, 0);
    }

    public TerminalNode FIRST() {
      return getToken(CarbonSqlBaseParser.FIRST, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(CarbonSqlBaseParser.FOLLOWING, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(CarbonSqlBaseParser.FORMAT, 0);
    }

    public TerminalNode FORMATTED() {
      return getToken(CarbonSqlBaseParser.FORMATTED, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public TerminalNode FUNCTIONS() {
      return getToken(CarbonSqlBaseParser.FUNCTIONS, 0);
    }

    public TerminalNode GLOBAL() {
      return getToken(CarbonSqlBaseParser.GLOBAL, 0);
    }

    public TerminalNode GROUPING() {
      return getToken(CarbonSqlBaseParser.GROUPING, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode IGNORE() {
      return getToken(CarbonSqlBaseParser.IGNORE, 0);
    }

    public TerminalNode IMPORT() {
      return getToken(CarbonSqlBaseParser.IMPORT, 0);
    }

    public TerminalNode INDEX() {
      return getToken(CarbonSqlBaseParser.INDEX, 0);
    }

    public TerminalNode INDEXES() {
      return getToken(CarbonSqlBaseParser.INDEXES, 0);
    }

    public TerminalNode INPATH() {
      return getToken(CarbonSqlBaseParser.INPATH, 0);
    }

    public TerminalNode INPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.INPUTFORMAT, 0);
    }

    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode INTERVAL() {
      return getToken(CarbonSqlBaseParser.INTERVAL, 0);
    }

    public TerminalNode ITEMS() {
      return getToken(CarbonSqlBaseParser.ITEMS, 0);
    }

    public TerminalNode KEYS() {
      return getToken(CarbonSqlBaseParser.KEYS, 0);
    }

    public TerminalNode LAST() {
      return getToken(CarbonSqlBaseParser.LAST, 0);
    }

    public TerminalNode LATERAL() {
      return getToken(CarbonSqlBaseParser.LATERAL, 0);
    }

    public TerminalNode LAZY() {
      return getToken(CarbonSqlBaseParser.LAZY, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public TerminalNode LIMIT() {
      return getToken(CarbonSqlBaseParser.LIMIT, 0);
    }

    public TerminalNode LINES() {
      return getToken(CarbonSqlBaseParser.LINES, 0);
    }

    public TerminalNode LIST() {
      return getToken(CarbonSqlBaseParser.LIST, 0);
    }

    public TerminalNode LOAD() {
      return getToken(CarbonSqlBaseParser.LOAD, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public TerminalNode LOCATION() {
      return getToken(CarbonSqlBaseParser.LOCATION, 0);
    }

    public TerminalNode LOCK() {
      return getToken(CarbonSqlBaseParser.LOCK, 0);
    }

    public TerminalNode LOCKS() {
      return getToken(CarbonSqlBaseParser.LOCKS, 0);
    }

    public TerminalNode LOGICAL() {
      return getToken(CarbonSqlBaseParser.LOGICAL, 0);
    }

    public TerminalNode MACRO() {
      return getToken(CarbonSqlBaseParser.MACRO, 0);
    }

    public TerminalNode MAP() {
      return getToken(CarbonSqlBaseParser.MAP, 0);
    }

    public TerminalNode MATCHED() {
      return getToken(CarbonSqlBaseParser.MATCHED, 0);
    }

    public TerminalNode MERGE() {
      return getToken(CarbonSqlBaseParser.MERGE, 0);
    }

    public TerminalNode MSCK() {
      return getToken(CarbonSqlBaseParser.MSCK, 0);
    }

    public TerminalNode NAMESPACE() {
      return getToken(CarbonSqlBaseParser.NAMESPACE, 0);
    }

    public TerminalNode NAMESPACES() {
      return getToken(CarbonSqlBaseParser.NAMESPACES, 0);
    }

    public TerminalNode NO() {
      return getToken(CarbonSqlBaseParser.NO, 0);
    }

    public TerminalNode NULLS() {
      return getToken(CarbonSqlBaseParser.NULLS, 0);
    }

    public TerminalNode OF() {
      return getToken(CarbonSqlBaseParser.OF, 0);
    }

    public TerminalNode OPTION() {
      return getToken(CarbonSqlBaseParser.OPTION, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(CarbonSqlBaseParser.OPTIONS, 0);
    }

    public TerminalNode OUT() {
      return getToken(CarbonSqlBaseParser.OUT, 0);
    }

    public TerminalNode OUTPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.OUTPUTFORMAT, 0);
    }

    public TerminalNode OVER() {
      return getToken(CarbonSqlBaseParser.OVER, 0);
    }

    public TerminalNode OVERLAY() {
      return getToken(CarbonSqlBaseParser.OVERLAY, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public TerminalNode PARTITION() {
      return getToken(CarbonSqlBaseParser.PARTITION, 0);
    }

    public TerminalNode PARTITIONED() {
      return getToken(CarbonSqlBaseParser.PARTITIONED, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(CarbonSqlBaseParser.PARTITIONS, 0);
    }

    public TerminalNode PERCENTLIT() {
      return getToken(CarbonSqlBaseParser.PERCENTLIT, 0);
    }

    public TerminalNode PIVOT() {
      return getToken(CarbonSqlBaseParser.PIVOT, 0);
    }

    public TerminalNode PLACING() {
      return getToken(CarbonSqlBaseParser.PLACING, 0);
    }

    public TerminalNode POSITION() {
      return getToken(CarbonSqlBaseParser.POSITION, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(CarbonSqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode PRINCIPALS() {
      return getToken(CarbonSqlBaseParser.PRINCIPALS, 0);
    }

    public TerminalNode PROPERTIES() {
      return getToken(CarbonSqlBaseParser.PROPERTIES, 0);
    }

    public TerminalNode PURGE() {
      return getToken(CarbonSqlBaseParser.PURGE, 0);
    }

    public TerminalNode QUERY() {
      return getToken(CarbonSqlBaseParser.QUERY, 0);
    }

    public TerminalNode RANGE() {
      return getToken(CarbonSqlBaseParser.RANGE, 0);
    }

    public TerminalNode RECORDREADER() {
      return getToken(CarbonSqlBaseParser.RECORDREADER, 0);
    }

    public TerminalNode RECORDWRITER() {
      return getToken(CarbonSqlBaseParser.RECORDWRITER, 0);
    }

    public TerminalNode RECOVER() {
      return getToken(CarbonSqlBaseParser.RECOVER, 0);
    }

    public TerminalNode REDUCE() {
      return getToken(CarbonSqlBaseParser.REDUCE, 0);
    }

    public TerminalNode REFRESH() {
      return getToken(CarbonSqlBaseParser.REFRESH, 0);
    }

    public TerminalNode RENAME() {
      return getToken(CarbonSqlBaseParser.RENAME, 0);
    }

    public TerminalNode REPAIR() {
      return getToken(CarbonSqlBaseParser.REPAIR, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode RESET() {
      return getToken(CarbonSqlBaseParser.RESET, 0);
    }

    public TerminalNode RESTRICT() {
      return getToken(CarbonSqlBaseParser.RESTRICT, 0);
    }

    public TerminalNode REVOKE() {
      return getToken(CarbonSqlBaseParser.REVOKE, 0);
    }

    public TerminalNode RLIKE() {
      return getToken(CarbonSqlBaseParser.RLIKE, 0);
    }

    public TerminalNode ROLE() {
      return getToken(CarbonSqlBaseParser.ROLE, 0);
    }

    public TerminalNode ROLES() {
      return getToken(CarbonSqlBaseParser.ROLES, 0);
    }

    public TerminalNode ROLLBACK() {
      return getToken(CarbonSqlBaseParser.ROLLBACK, 0);
    }

    public TerminalNode ROLLUP() {
      return getToken(CarbonSqlBaseParser.ROLLUP, 0);
    }

    public TerminalNode ROW() {
      return getToken(CarbonSqlBaseParser.ROW, 0);
    }

    public TerminalNode ROWS() {
      return getToken(CarbonSqlBaseParser.ROWS, 0);
    }

    public TerminalNode SCHEMA() {
      return getToken(CarbonSqlBaseParser.SCHEMA, 0);
    }

    public TerminalNode SEMI() {
      return getToken(CarbonSqlBaseParser.SEMI, 0);
    }

    public TerminalNode SEPARATED() {
      return getToken(CarbonSqlBaseParser.SEPARATED, 0);
    }

    public TerminalNode SERDE() {
      return getToken(CarbonSqlBaseParser.SERDE, 0);
    }

    public TerminalNode SERDEPROPERTIES() {
      return getToken(CarbonSqlBaseParser.SERDEPROPERTIES, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode SETMINUS() {
      return getToken(CarbonSqlBaseParser.SETMINUS, 0);
    }

    public TerminalNode SETS() {
      return getToken(CarbonSqlBaseParser.SETS, 0);
    }

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode SKEWED() {
      return getToken(CarbonSqlBaseParser.SKEWED, 0);
    }

    public TerminalNode SORT() {
      return getToken(CarbonSqlBaseParser.SORT, 0);
    }

    public TerminalNode SORTED() {
      return getToken(CarbonSqlBaseParser.SORTED, 0);
    }

    public TerminalNode START() {
      return getToken(CarbonSqlBaseParser.START, 0);
    }

    public TerminalNode STATISTICS() {
      return getToken(CarbonSqlBaseParser.STATISTICS, 0);
    }

    public TerminalNode STORED() {
      return getToken(CarbonSqlBaseParser.STORED, 0);
    }

    public TerminalNode STRATIFY() {
      return getToken(CarbonSqlBaseParser.STRATIFY, 0);
    }

    public TerminalNode STRUCT() {
      return getToken(CarbonSqlBaseParser.STRUCT, 0);
    }

    public TerminalNode SUBSTR() {
      return getToken(CarbonSqlBaseParser.SUBSTR, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(CarbonSqlBaseParser.SUBSTRING, 0);
    }

    public TerminalNode TABLES() {
      return getToken(CarbonSqlBaseParser.TABLES, 0);
    }

    public TerminalNode TABLESAMPLE() {
      return getToken(CarbonSqlBaseParser.TABLESAMPLE, 0);
    }

    public TerminalNode TBLPROPERTIES() {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode TERMINATED() {
      return getToken(CarbonSqlBaseParser.TERMINATED, 0);
    }

    public TerminalNode TOUCH() {
      return getToken(CarbonSqlBaseParser.TOUCH, 0);
    }

    public TerminalNode TRANSACTION() {
      return getToken(CarbonSqlBaseParser.TRANSACTION, 0);
    }

    public TerminalNode TRANSACTIONS() {
      return getToken(CarbonSqlBaseParser.TRANSACTIONS, 0);
    }

    public TerminalNode TRANSFORM() {
      return getToken(CarbonSqlBaseParser.TRANSFORM, 0);
    }

    public TerminalNode TRIM() {
      return getToken(CarbonSqlBaseParser.TRIM, 0);
    }

    public TerminalNode TRUE() {
      return getToken(CarbonSqlBaseParser.TRUE, 0);
    }

    public TerminalNode TRUNCATE() {
      return getToken(CarbonSqlBaseParser.TRUNCATE, 0);
    }

    public TerminalNode TYPE() {
      return getToken(CarbonSqlBaseParser.TYPE, 0);
    }

    public TerminalNode UNARCHIVE() {
      return getToken(CarbonSqlBaseParser.UNARCHIVE, 0);
    }

    public TerminalNode UNBOUNDED() {
      return getToken(CarbonSqlBaseParser.UNBOUNDED, 0);
    }

    public TerminalNode UNCACHE() {
      return getToken(CarbonSqlBaseParser.UNCACHE, 0);
    }

    public TerminalNode UNLOCK() {
      return getToken(CarbonSqlBaseParser.UNLOCK, 0);
    }

    public TerminalNode UNSET() {
      return getToken(CarbonSqlBaseParser.UNSET, 0);
    }

    public TerminalNode UPDATE() {
      return getToken(CarbonSqlBaseParser.UPDATE, 0);
    }

    public TerminalNode USE() {
      return getToken(CarbonSqlBaseParser.USE, 0);
    }

    public TerminalNode VALUES() {
      return getToken(CarbonSqlBaseParser.VALUES, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TerminalNode VIEWS() {
      return getToken(CarbonSqlBaseParser.VIEWS, 0);
    }

    public TerminalNode WINDOW() {
      return getToken(CarbonSqlBaseParser.WINDOW, 0);
    }

    public TerminalNode ZONE() {
      return getToken(CarbonSqlBaseParser.ZONE, 0);
    }

    public AnsiNonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_ansiNonReserved;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitAnsiNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final AnsiNonReservedContext ansiNonReserved() throws RecognitionException {
    AnsiNonReservedContext _localctx = new AnsiNonReservedContext(_ctx, getState());
    enterRule(_localctx, 270, RULE_ansiNonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(3057);
        _la = _input.LA(1);
        if (!(((((_la - 12)) & ~0x3f) == 0 &&
            ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (AFTER - 12)) | (1L << (ALTER - 12))
                | (1L << (ANALYZE - 12)) | (1L << (ANTI - 12)) | (1L << (ARCHIVE - 12)) | (1L << (
                ARRAY - 12)) | (1L << (ASC - 12)) | (1L << (AT - 12)) | (1L << (BETWEEN - 12)) | (1L
                << (BUCKET - 12)) | (1L << (BUCKETS - 12)) | (1L << (BY - 12)) | (1L << (CACHE
                - 12)) | (1L << (CASCADE - 12)) | (1L << (CHANGE - 12)) | (1L << (CLEAR - 12)) | (1L
                << (CLUSTER - 12)) | (1L << (CLUSTERED - 12)) | (1L << (CODEGEN - 12)) | (1L << (
                COLLECTION - 12)) | (1L << (COLUMNS - 12)) | (1L << (COMMENT - 12)) | (1L << (COMMIT
                - 12)) | (1L << (COMPACT - 12)) | (1L << (COMPACTIONS - 12)) | (1L << (COMPUTE
                - 12)) | (1L << (CONCATENATE - 12)) | (1L << (COST - 12)) | (1L << (CUBE - 12)) | (
                1L << (CURRENT - 12)) | (1L << (DATA - 12)) | (1L << (DATABASE - 12)) | (1L << (
                DATABASES - 12)) | (1L << (DBPROPERTIES - 12)) | (1L << (DEFINED - 12)) | (1L << (
                DELETE - 12)) | (1L << (DELIMITED - 12)) | (1L << (DESC - 12)) | (1L << (DESCRIBE
                - 12)) | (1L << (DFS - 12)) | (1L << (DIRECTORIES - 12)) | (1L << (DIRECTORY - 12))
                | (1L << (DISTRIBUTE - 12)) | (1L << (DIV - 12)))) != 0) || (
            (((_la - 76)) & ~0x3f) == 0 &&
                ((1L << (_la - 76)) & ((1L << (DROP - 76)) | (1L << (ESCAPED - 76)) | (1L << (
                    EXCHANGE - 76)) | (1L << (EXISTS - 76)) | (1L << (EXPLAIN - 76)) | (1L << (
                    EXPORT - 76)) | (1L << (EXTENDED - 76)) | (1L << (EXTERNAL - 76)) | (1L << (
                    EXTRACT - 76)) | (1L << (FIELDS - 76)) | (1L << (FILEFORMAT - 76)) | (1L << (
                    FIRST - 76)) | (1L << (FOLLOWING - 76)) | (1L << (FORMAT - 76)) | (1L << (
                    FORMATTED - 76)) | (1L << (FUNCTION - 76)) | (1L << (FUNCTIONS - 76)) | (1L << (
                    GLOBAL - 76)) | (1L << (GROUPING - 76)) | (1L << (IF - 76)) | (1L << (IGNORE
                    - 76)) | (1L << (IMPORT - 76)) | (1L << (INDEX - 76)) | (1L << (INDEXES - 76))
                    | (1L << (INPATH - 76)) | (1L << (INPUTFORMAT - 76)) | (1L << (INSERT - 76)) | (
                    1L << (INTERVAL - 76)) | (1L << (ITEMS - 76)) | (1L << (KEYS - 76)) | (1L << (
                    LAST - 76)) | (1L << (LATERAL - 76)) | (1L << (LAZY - 76)) | (1L << (LIKE - 76))
                    | (1L << (LIMIT - 76)) | (1L << (LINES - 76)) | (1L << (LIST - 76)) | (1L << (
                    LOAD - 76)) | (1L << (LOCAL - 76)) | (1L << (LOCATION - 76)) | (1L << (LOCK
                    - 76)) | (1L << (LOCKS - 76)))) != 0) || ((((_la - 140)) & ~0x3f) == 0 &&
            ((1L << (_la - 140)) & ((1L << (LOGICAL - 140)) | (1L << (MACRO - 140)) | (1L << (MAP
                - 140)) | (1L << (MATCHED - 140)) | (1L << (MERGE - 140)) | (1L << (MSCK - 140)) | (
                1L << (NAMESPACE - 140)) | (1L << (NAMESPACES - 140)) | (1L << (NO - 140)) | (1L
                << (NULLS - 140)) | (1L << (OF - 140)) | (1L << (OPTION - 140)) | (1L << (OPTIONS
                - 140)) | (1L << (OUT - 140)) | (1L << (OUTPUTFORMAT - 140)) | (1L << (OVER - 140))
                | (1L << (OVERLAY - 140)) | (1L << (OVERWRITE - 140)) | (1L << (PARTITION - 140))
                | (1L << (PARTITIONED - 140)) | (1L << (PARTITIONS - 140)) | (1L << (PERCENTLIT
                - 140)) | (1L << (PIVOT - 140)) | (1L << (PLACING - 140)) | (1L << (POSITION - 140))
                | (1L << (PRECEDING - 140)) | (1L << (PRINCIPALS - 140)) | (1L << (PROPERTIES
                - 140)) | (1L << (PURGE - 140)) | (1L << (QUERY - 140)) | (1L << (RANGE - 140)) | (
                1L << (RECORDREADER - 140)) | (1L << (RECORDWRITER - 140)) | (1L << (RECOVER - 140))
                | (1L << (REDUCE - 140)) | (1L << (REFRESH - 140)) | (1L << (RENAME - 140)) | (1L
                << (REPAIR - 140)) | (1L << (REPLACE - 140)) | (1L << (RESET - 140)) | (1L << (
                RESTRICT - 140)) | (1L << (REVOKE - 140)) | (1L << (RLIKE - 140)) | (1L << (ROLE
                - 140)) | (1L << (ROLES - 140)) | (1L << (ROLLBACK - 140)) | (1L << (ROLLUP - 140))
                | (1L << (ROW - 140)) | (1L << (ROWS - 140)) | (1L << (SCHEMA - 140)) | (1L << (SEMI
                - 140)))) != 0) || ((((_la - 204)) & ~0x3f) == 0 &&
            ((1L << (_la - 204)) & ((1L << (SEPARATED - 204)) | (1L << (SERDE - 204)) | (1L << (
                SERDEPROPERTIES - 204)) | (1L << (SET - 204)) | (1L << (SETMINUS - 204)) | (1L << (
                SETS - 204)) | (1L << (SHOW - 204)) | (1L << (SKEWED - 204)) | (1L << (SORT - 204))
                | (1L << (SORTED - 204)) | (1L << (START - 204)) | (1L << (STATISTICS - 204)) | (1L
                << (STORED - 204)) | (1L << (STRATIFY - 204)) | (1L << (STRUCT - 204)) | (1L << (
                SUBSTR - 204)) | (1L << (SUBSTRING - 204)) | (1L << (TABLES - 204)) | (1L << (
                TABLESAMPLE - 204)) | (1L << (TBLPROPERTIES - 204)) | (1L << (TEMPORARY - 204)) | (
                1L << (TERMINATED - 204)) | (1L << (TOUCH - 204)) | (1L << (TRANSACTION - 204)) | (
                1L << (TRANSACTIONS - 204)) | (1L << (TRANSFORM - 204)) | (1L << (TRIM - 204)) | (1L
                << (TRUE - 204)) | (1L << (TRUNCATE - 204)) | (1L << (TYPE - 204)) | (1L << (
                UNARCHIVE - 204)) | (1L << (UNBOUNDED - 204)) | (1L << (UNCACHE - 204)) | (1L << (
                UNLOCK - 204)) | (1L << (UNSET - 204)) | (1L << (UPDATE - 204)) | (1L << (USE
                - 204)) | (1L << (VALUES - 204)) | (1L << (VIEW - 204)) | (1L << (VIEWS - 204)) | (
                1L << (WINDOW - 204)) | (1L << (ZONE - 204)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class StrictNonReservedContext extends ParserRuleContext {
    public TerminalNode ANTI() {
      return getToken(CarbonSqlBaseParser.ANTI, 0);
    }

    public TerminalNode CROSS() {
      return getToken(CarbonSqlBaseParser.CROSS, 0);
    }

    public TerminalNode EXCEPT() {
      return getToken(CarbonSqlBaseParser.EXCEPT, 0);
    }

    public TerminalNode FULL() {
      return getToken(CarbonSqlBaseParser.FULL, 0);
    }

    public TerminalNode INNER() {
      return getToken(CarbonSqlBaseParser.INNER, 0);
    }

    public TerminalNode INTERSECT() {
      return getToken(CarbonSqlBaseParser.INTERSECT, 0);
    }

    public TerminalNode JOIN() {
      return getToken(CarbonSqlBaseParser.JOIN, 0);
    }

    public TerminalNode LEFT() {
      return getToken(CarbonSqlBaseParser.LEFT, 0);
    }

    public TerminalNode NATURAL() {
      return getToken(CarbonSqlBaseParser.NATURAL, 0);
    }

    public TerminalNode ON() {
      return getToken(CarbonSqlBaseParser.ON, 0);
    }

    public TerminalNode RIGHT() {
      return getToken(CarbonSqlBaseParser.RIGHT, 0);
    }

    public TerminalNode SEMI() {
      return getToken(CarbonSqlBaseParser.SEMI, 0);
    }

    public TerminalNode SETMINUS() {
      return getToken(CarbonSqlBaseParser.SETMINUS, 0);
    }

    public TerminalNode UNION() {
      return getToken(CarbonSqlBaseParser.UNION, 0);
    }

    public TerminalNode USING() {
      return getToken(CarbonSqlBaseParser.USING, 0);
    }

    public StrictNonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_strictNonReserved;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitStrictNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final StrictNonReservedContext strictNonReserved() throws RecognitionException {
    StrictNonReservedContext _localctx = new StrictNonReservedContext(_ctx, getState());
    enterRule(_localctx, 272, RULE_strictNonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(3059);
        _la = _input.LA(1);
        if (!(((((_la - 18)) & ~0x3f) == 0 &&
            ((1L << (_la - 18)) & ((1L << (ANTI - 18)) | (1L << (CROSS - 18)) | (1L << (EXCEPT
                - 18)))) != 0) || ((((_la - 101)) & ~0x3f) == 0 &&
            ((1L << (_la - 101)) & ((1L << (FULL - 101)) | (1L << (INNER - 101)) | (1L << (INTERSECT
                - 101)) | (1L << (JOIN - 101)) | (1L << (LEFT - 101)) | (1L << (NATURAL - 101)) | (
                1L << (ON - 101)))) != 0) || ((((_la - 193)) & ~0x3f) == 0 &&
            ((1L << (_la - 193)) & ((1L << (RIGHT - 193)) | (1L << (SEMI - 193)) | (1L << (SETMINUS
                - 193)) | (1L << (UNION - 193)) | (1L << (USING - 193)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public static class NonReservedContext extends ParserRuleContext {
    public TerminalNode ADD() {
      return getToken(CarbonSqlBaseParser.ADD, 0);
    }

    public TerminalNode AFTER() {
      return getToken(CarbonSqlBaseParser.AFTER, 0);
    }

    public TerminalNode ALL() {
      return getToken(CarbonSqlBaseParser.ALL, 0);
    }

    public TerminalNode ALTER() {
      return getToken(CarbonSqlBaseParser.ALTER, 0);
    }

    public TerminalNode ANALYZE() {
      return getToken(CarbonSqlBaseParser.ANALYZE, 0);
    }

    public TerminalNode AND() {
      return getToken(CarbonSqlBaseParser.AND, 0);
    }

    public TerminalNode ANY() {
      return getToken(CarbonSqlBaseParser.ANY, 0);
    }

    public TerminalNode ARCHIVE() {
      return getToken(CarbonSqlBaseParser.ARCHIVE, 0);
    }

    public TerminalNode ARRAY() {
      return getToken(CarbonSqlBaseParser.ARRAY, 0);
    }

    public TerminalNode AS() {
      return getToken(CarbonSqlBaseParser.AS, 0);
    }

    public TerminalNode ASC() {
      return getToken(CarbonSqlBaseParser.ASC, 0);
    }

    public TerminalNode AT() {
      return getToken(CarbonSqlBaseParser.AT, 0);
    }

    public TerminalNode AUTHORIZATION() {
      return getToken(CarbonSqlBaseParser.AUTHORIZATION, 0);
    }

    public TerminalNode BETWEEN() {
      return getToken(CarbonSqlBaseParser.BETWEEN, 0);
    }

    public TerminalNode BOTH() {
      return getToken(CarbonSqlBaseParser.BOTH, 0);
    }

    public TerminalNode BUCKET() {
      return getToken(CarbonSqlBaseParser.BUCKET, 0);
    }

    public TerminalNode BUCKETS() {
      return getToken(CarbonSqlBaseParser.BUCKETS, 0);
    }

    public TerminalNode BY() {
      return getToken(CarbonSqlBaseParser.BY, 0);
    }

    public TerminalNode CACHE() {
      return getToken(CarbonSqlBaseParser.CACHE, 0);
    }

    public TerminalNode CASCADE() {
      return getToken(CarbonSqlBaseParser.CASCADE, 0);
    }

    public TerminalNode CASE() {
      return getToken(CarbonSqlBaseParser.CASE, 0);
    }

    public TerminalNode CAST() {
      return getToken(CarbonSqlBaseParser.CAST, 0);
    }

    public TerminalNode CHANGE() {
      return getToken(CarbonSqlBaseParser.CHANGE, 0);
    }

    public TerminalNode CHECK() {
      return getToken(CarbonSqlBaseParser.CHECK, 0);
    }

    public TerminalNode CLEAR() {
      return getToken(CarbonSqlBaseParser.CLEAR, 0);
    }

    public TerminalNode CLUSTER() {
      return getToken(CarbonSqlBaseParser.CLUSTER, 0);
    }

    public TerminalNode CLUSTERED() {
      return getToken(CarbonSqlBaseParser.CLUSTERED, 0);
    }

    public TerminalNode CODEGEN() {
      return getToken(CarbonSqlBaseParser.CODEGEN, 0);
    }

    public TerminalNode COLLATE() {
      return getToken(CarbonSqlBaseParser.COLLATE, 0);
    }

    public TerminalNode COLLECTION() {
      return getToken(CarbonSqlBaseParser.COLLECTION, 0);
    }

    public TerminalNode COLUMN() {
      return getToken(CarbonSqlBaseParser.COLUMN, 0);
    }

    public TerminalNode COLUMNS() {
      return getToken(CarbonSqlBaseParser.COLUMNS, 0);
    }

    public TerminalNode COMMENT() {
      return getToken(CarbonSqlBaseParser.COMMENT, 0);
    }

    public TerminalNode COMMIT() {
      return getToken(CarbonSqlBaseParser.COMMIT, 0);
    }

    public TerminalNode COMPACT() {
      return getToken(CarbonSqlBaseParser.COMPACT, 0);
    }

    public TerminalNode COMPACTIONS() {
      return getToken(CarbonSqlBaseParser.COMPACTIONS, 0);
    }

    public TerminalNode COMPUTE() {
      return getToken(CarbonSqlBaseParser.COMPUTE, 0);
    }

    public TerminalNode CONCATENATE() {
      return getToken(CarbonSqlBaseParser.CONCATENATE, 0);
    }

    public TerminalNode CONSTRAINT() {
      return getToken(CarbonSqlBaseParser.CONSTRAINT, 0);
    }

    public TerminalNode COST() {
      return getToken(CarbonSqlBaseParser.COST, 0);
    }

    public TerminalNode CREATE() {
      return getToken(CarbonSqlBaseParser.CREATE, 0);
    }

    public TerminalNode CUBE() {
      return getToken(CarbonSqlBaseParser.CUBE, 0);
    }

    public TerminalNode CURRENT() {
      return getToken(CarbonSqlBaseParser.CURRENT, 0);
    }

    public TerminalNode CURRENT_DATE() {
      return getToken(CarbonSqlBaseParser.CURRENT_DATE, 0);
    }

    public TerminalNode CURRENT_TIME() {
      return getToken(CarbonSqlBaseParser.CURRENT_TIME, 0);
    }

    public TerminalNode CURRENT_TIMESTAMP() {
      return getToken(CarbonSqlBaseParser.CURRENT_TIMESTAMP, 0);
    }

    public TerminalNode CURRENT_USER() {
      return getToken(CarbonSqlBaseParser.CURRENT_USER, 0);
    }

    public TerminalNode DATA() {
      return getToken(CarbonSqlBaseParser.DATA, 0);
    }

    public TerminalNode DATABASE() {
      return getToken(CarbonSqlBaseParser.DATABASE, 0);
    }

    public TerminalNode DATABASES() {
      return getToken(CarbonSqlBaseParser.DATABASES, 0);
    }

    public TerminalNode DBPROPERTIES() {
      return getToken(CarbonSqlBaseParser.DBPROPERTIES, 0);
    }

    public TerminalNode DEFINED() {
      return getToken(CarbonSqlBaseParser.DEFINED, 0);
    }

    public TerminalNode DELETE() {
      return getToken(CarbonSqlBaseParser.DELETE, 0);
    }

    public TerminalNode DELIMITED() {
      return getToken(CarbonSqlBaseParser.DELIMITED, 0);
    }

    public TerminalNode DESC() {
      return getToken(CarbonSqlBaseParser.DESC, 0);
    }

    public TerminalNode DESCRIBE() {
      return getToken(CarbonSqlBaseParser.DESCRIBE, 0);
    }

    public TerminalNode DFS() {
      return getToken(CarbonSqlBaseParser.DFS, 0);
    }

    public TerminalNode DIRECTORIES() {
      return getToken(CarbonSqlBaseParser.DIRECTORIES, 0);
    }

    public TerminalNode DIRECTORY() {
      return getToken(CarbonSqlBaseParser.DIRECTORY, 0);
    }

    public TerminalNode DISTINCT() {
      return getToken(CarbonSqlBaseParser.DISTINCT, 0);
    }

    public TerminalNode DISTRIBUTE() {
      return getToken(CarbonSqlBaseParser.DISTRIBUTE, 0);
    }

    public TerminalNode DIV() {
      return getToken(CarbonSqlBaseParser.DIV, 0);
    }

    public TerminalNode DROP() {
      return getToken(CarbonSqlBaseParser.DROP, 0);
    }

    public TerminalNode ELSE() {
      return getToken(CarbonSqlBaseParser.ELSE, 0);
    }

    public TerminalNode END() {
      return getToken(CarbonSqlBaseParser.END, 0);
    }

    public TerminalNode ESCAPE() {
      return getToken(CarbonSqlBaseParser.ESCAPE, 0);
    }

    public TerminalNode ESCAPED() {
      return getToken(CarbonSqlBaseParser.ESCAPED, 0);
    }

    public TerminalNode EXCHANGE() {
      return getToken(CarbonSqlBaseParser.EXCHANGE, 0);
    }

    public TerminalNode EXISTS() {
      return getToken(CarbonSqlBaseParser.EXISTS, 0);
    }

    public TerminalNode EXPLAIN() {
      return getToken(CarbonSqlBaseParser.EXPLAIN, 0);
    }

    public TerminalNode EXPORT() {
      return getToken(CarbonSqlBaseParser.EXPORT, 0);
    }

    public TerminalNode EXTENDED() {
      return getToken(CarbonSqlBaseParser.EXTENDED, 0);
    }

    public TerminalNode EXTERNAL() {
      return getToken(CarbonSqlBaseParser.EXTERNAL, 0);
    }

    public TerminalNode EXTRACT() {
      return getToken(CarbonSqlBaseParser.EXTRACT, 0);
    }

    public TerminalNode FALSE() {
      return getToken(CarbonSqlBaseParser.FALSE, 0);
    }

    public TerminalNode FETCH() {
      return getToken(CarbonSqlBaseParser.FETCH, 0);
    }

    public TerminalNode FILTER() {
      return getToken(CarbonSqlBaseParser.FILTER, 0);
    }

    public TerminalNode FIELDS() {
      return getToken(CarbonSqlBaseParser.FIELDS, 0);
    }

    public TerminalNode FILEFORMAT() {
      return getToken(CarbonSqlBaseParser.FILEFORMAT, 0);
    }

    public TerminalNode FIRST() {
      return getToken(CarbonSqlBaseParser.FIRST, 0);
    }

    public TerminalNode FOLLOWING() {
      return getToken(CarbonSqlBaseParser.FOLLOWING, 0);
    }

    public TerminalNode FOR() {
      return getToken(CarbonSqlBaseParser.FOR, 0);
    }

    public TerminalNode FOREIGN() {
      return getToken(CarbonSqlBaseParser.FOREIGN, 0);
    }

    public TerminalNode FORMAT() {
      return getToken(CarbonSqlBaseParser.FORMAT, 0);
    }

    public TerminalNode FORMATTED() {
      return getToken(CarbonSqlBaseParser.FORMATTED, 0);
    }

    public TerminalNode FROM() {
      return getToken(CarbonSqlBaseParser.FROM, 0);
    }

    public TerminalNode FUNCTION() {
      return getToken(CarbonSqlBaseParser.FUNCTION, 0);
    }

    public TerminalNode FUNCTIONS() {
      return getToken(CarbonSqlBaseParser.FUNCTIONS, 0);
    }

    public TerminalNode GLOBAL() {
      return getToken(CarbonSqlBaseParser.GLOBAL, 0);
    }

    public TerminalNode GRANT() {
      return getToken(CarbonSqlBaseParser.GRANT, 0);
    }

    public TerminalNode GROUP() {
      return getToken(CarbonSqlBaseParser.GROUP, 0);
    }

    public TerminalNode GROUPING() {
      return getToken(CarbonSqlBaseParser.GROUPING, 0);
    }

    public TerminalNode HAVING() {
      return getToken(CarbonSqlBaseParser.HAVING, 0);
    }

    public TerminalNode IF() {
      return getToken(CarbonSqlBaseParser.IF, 0);
    }

    public TerminalNode IGNORE() {
      return getToken(CarbonSqlBaseParser.IGNORE, 0);
    }

    public TerminalNode IMPORT() {
      return getToken(CarbonSqlBaseParser.IMPORT, 0);
    }

    public TerminalNode IN() {
      return getToken(CarbonSqlBaseParser.IN, 0);
    }

    public TerminalNode INDEX() {
      return getToken(CarbonSqlBaseParser.INDEX, 0);
    }

    public TerminalNode INDEXES() {
      return getToken(CarbonSqlBaseParser.INDEXES, 0);
    }

    public TerminalNode INPATH() {
      return getToken(CarbonSqlBaseParser.INPATH, 0);
    }

    public TerminalNode INPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.INPUTFORMAT, 0);
    }

    public TerminalNode INSERT() {
      return getToken(CarbonSqlBaseParser.INSERT, 0);
    }

    public TerminalNode INTERVAL() {
      return getToken(CarbonSqlBaseParser.INTERVAL, 0);
    }

    public TerminalNode INTO() {
      return getToken(CarbonSqlBaseParser.INTO, 0);
    }

    public TerminalNode IS() {
      return getToken(CarbonSqlBaseParser.IS, 0);
    }

    public TerminalNode ITEMS() {
      return getToken(CarbonSqlBaseParser.ITEMS, 0);
    }

    public TerminalNode KEYS() {
      return getToken(CarbonSqlBaseParser.KEYS, 0);
    }

    public TerminalNode LAST() {
      return getToken(CarbonSqlBaseParser.LAST, 0);
    }

    public TerminalNode LATERAL() {
      return getToken(CarbonSqlBaseParser.LATERAL, 0);
    }

    public TerminalNode LAZY() {
      return getToken(CarbonSqlBaseParser.LAZY, 0);
    }

    public TerminalNode LEADING() {
      return getToken(CarbonSqlBaseParser.LEADING, 0);
    }

    public TerminalNode LIKE() {
      return getToken(CarbonSqlBaseParser.LIKE, 0);
    }

    public TerminalNode LIMIT() {
      return getToken(CarbonSqlBaseParser.LIMIT, 0);
    }

    public TerminalNode LINES() {
      return getToken(CarbonSqlBaseParser.LINES, 0);
    }

    public TerminalNode LIST() {
      return getToken(CarbonSqlBaseParser.LIST, 0);
    }

    public TerminalNode LOAD() {
      return getToken(CarbonSqlBaseParser.LOAD, 0);
    }

    public TerminalNode LOCAL() {
      return getToken(CarbonSqlBaseParser.LOCAL, 0);
    }

    public TerminalNode LOCATION() {
      return getToken(CarbonSqlBaseParser.LOCATION, 0);
    }

    public TerminalNode LOCK() {
      return getToken(CarbonSqlBaseParser.LOCK, 0);
    }

    public TerminalNode LOCKS() {
      return getToken(CarbonSqlBaseParser.LOCKS, 0);
    }

    public TerminalNode LOGICAL() {
      return getToken(CarbonSqlBaseParser.LOGICAL, 0);
    }

    public TerminalNode MACRO() {
      return getToken(CarbonSqlBaseParser.MACRO, 0);
    }

    public TerminalNode MAP() {
      return getToken(CarbonSqlBaseParser.MAP, 0);
    }

    public TerminalNode MATCHED() {
      return getToken(CarbonSqlBaseParser.MATCHED, 0);
    }

    public TerminalNode MERGE() {
      return getToken(CarbonSqlBaseParser.MERGE, 0);
    }

    public TerminalNode MSCK() {
      return getToken(CarbonSqlBaseParser.MSCK, 0);
    }

    public TerminalNode NAMESPACE() {
      return getToken(CarbonSqlBaseParser.NAMESPACE, 0);
    }

    public TerminalNode NAMESPACES() {
      return getToken(CarbonSqlBaseParser.NAMESPACES, 0);
    }

    public TerminalNode NO() {
      return getToken(CarbonSqlBaseParser.NO, 0);
    }

    public TerminalNode NOT() {
      return getToken(CarbonSqlBaseParser.NOT, 0);
    }

    public TerminalNode NULL() {
      return getToken(CarbonSqlBaseParser.NULL, 0);
    }

    public TerminalNode NULLS() {
      return getToken(CarbonSqlBaseParser.NULLS, 0);
    }

    public TerminalNode OF() {
      return getToken(CarbonSqlBaseParser.OF, 0);
    }

    public TerminalNode ONLY() {
      return getToken(CarbonSqlBaseParser.ONLY, 0);
    }

    public TerminalNode OPTION() {
      return getToken(CarbonSqlBaseParser.OPTION, 0);
    }

    public TerminalNode OPTIONS() {
      return getToken(CarbonSqlBaseParser.OPTIONS, 0);
    }

    public TerminalNode OR() {
      return getToken(CarbonSqlBaseParser.OR, 0);
    }

    public TerminalNode ORDER() {
      return getToken(CarbonSqlBaseParser.ORDER, 0);
    }

    public TerminalNode OUT() {
      return getToken(CarbonSqlBaseParser.OUT, 0);
    }

    public TerminalNode OUTER() {
      return getToken(CarbonSqlBaseParser.OUTER, 0);
    }

    public TerminalNode OUTPUTFORMAT() {
      return getToken(CarbonSqlBaseParser.OUTPUTFORMAT, 0);
    }

    public TerminalNode OVER() {
      return getToken(CarbonSqlBaseParser.OVER, 0);
    }

    public TerminalNode OVERLAPS() {
      return getToken(CarbonSqlBaseParser.OVERLAPS, 0);
    }

    public TerminalNode OVERLAY() {
      return getToken(CarbonSqlBaseParser.OVERLAY, 0);
    }

    public TerminalNode OVERWRITE() {
      return getToken(CarbonSqlBaseParser.OVERWRITE, 0);
    }

    public TerminalNode PARTITION() {
      return getToken(CarbonSqlBaseParser.PARTITION, 0);
    }

    public TerminalNode PARTITIONED() {
      return getToken(CarbonSqlBaseParser.PARTITIONED, 0);
    }

    public TerminalNode PARTITIONS() {
      return getToken(CarbonSqlBaseParser.PARTITIONS, 0);
    }

    public TerminalNode PERCENTLIT() {
      return getToken(CarbonSqlBaseParser.PERCENTLIT, 0);
    }

    public TerminalNode PIVOT() {
      return getToken(CarbonSqlBaseParser.PIVOT, 0);
    }

    public TerminalNode PLACING() {
      return getToken(CarbonSqlBaseParser.PLACING, 0);
    }

    public TerminalNode POSITION() {
      return getToken(CarbonSqlBaseParser.POSITION, 0);
    }

    public TerminalNode PRECEDING() {
      return getToken(CarbonSqlBaseParser.PRECEDING, 0);
    }

    public TerminalNode PRIMARY() {
      return getToken(CarbonSqlBaseParser.PRIMARY, 0);
    }

    public TerminalNode PRINCIPALS() {
      return getToken(CarbonSqlBaseParser.PRINCIPALS, 0);
    }

    public TerminalNode PROPERTIES() {
      return getToken(CarbonSqlBaseParser.PROPERTIES, 0);
    }

    public TerminalNode PURGE() {
      return getToken(CarbonSqlBaseParser.PURGE, 0);
    }

    public TerminalNode QUERY() {
      return getToken(CarbonSqlBaseParser.QUERY, 0);
    }

    public TerminalNode RANGE() {
      return getToken(CarbonSqlBaseParser.RANGE, 0);
    }

    public TerminalNode RECORDREADER() {
      return getToken(CarbonSqlBaseParser.RECORDREADER, 0);
    }

    public TerminalNode RECORDWRITER() {
      return getToken(CarbonSqlBaseParser.RECORDWRITER, 0);
    }

    public TerminalNode RECOVER() {
      return getToken(CarbonSqlBaseParser.RECOVER, 0);
    }

    public TerminalNode REDUCE() {
      return getToken(CarbonSqlBaseParser.REDUCE, 0);
    }

    public TerminalNode REFERENCES() {
      return getToken(CarbonSqlBaseParser.REFERENCES, 0);
    }

    public TerminalNode REFRESH() {
      return getToken(CarbonSqlBaseParser.REFRESH, 0);
    }

    public TerminalNode RENAME() {
      return getToken(CarbonSqlBaseParser.RENAME, 0);
    }

    public TerminalNode REPAIR() {
      return getToken(CarbonSqlBaseParser.REPAIR, 0);
    }

    public TerminalNode REPLACE() {
      return getToken(CarbonSqlBaseParser.REPLACE, 0);
    }

    public TerminalNode RESET() {
      return getToken(CarbonSqlBaseParser.RESET, 0);
    }

    public TerminalNode RESTRICT() {
      return getToken(CarbonSqlBaseParser.RESTRICT, 0);
    }

    public TerminalNode REVOKE() {
      return getToken(CarbonSqlBaseParser.REVOKE, 0);
    }

    public TerminalNode RLIKE() {
      return getToken(CarbonSqlBaseParser.RLIKE, 0);
    }

    public TerminalNode ROLE() {
      return getToken(CarbonSqlBaseParser.ROLE, 0);
    }

    public TerminalNode ROLES() {
      return getToken(CarbonSqlBaseParser.ROLES, 0);
    }

    public TerminalNode ROLLBACK() {
      return getToken(CarbonSqlBaseParser.ROLLBACK, 0);
    }

    public TerminalNode ROLLUP() {
      return getToken(CarbonSqlBaseParser.ROLLUP, 0);
    }

    public TerminalNode ROW() {
      return getToken(CarbonSqlBaseParser.ROW, 0);
    }

    public TerminalNode ROWS() {
      return getToken(CarbonSqlBaseParser.ROWS, 0);
    }

    public TerminalNode SCHEMA() {
      return getToken(CarbonSqlBaseParser.SCHEMA, 0);
    }

    public TerminalNode SELECT() {
      return getToken(CarbonSqlBaseParser.SELECT, 0);
    }

    public TerminalNode SEPARATED() {
      return getToken(CarbonSqlBaseParser.SEPARATED, 0);
    }

    public TerminalNode SERDE() {
      return getToken(CarbonSqlBaseParser.SERDE, 0);
    }

    public TerminalNode SERDEPROPERTIES() {
      return getToken(CarbonSqlBaseParser.SERDEPROPERTIES, 0);
    }

    public TerminalNode SESSION_USER() {
      return getToken(CarbonSqlBaseParser.SESSION_USER, 0);
    }

    public TerminalNode SET() {
      return getToken(CarbonSqlBaseParser.SET, 0);
    }

    public TerminalNode SETS() {
      return getToken(CarbonSqlBaseParser.SETS, 0);
    }

    public TerminalNode SHOW() {
      return getToken(CarbonSqlBaseParser.SHOW, 0);
    }

    public TerminalNode SKEWED() {
      return getToken(CarbonSqlBaseParser.SKEWED, 0);
    }

    public TerminalNode SOME() {
      return getToken(CarbonSqlBaseParser.SOME, 0);
    }

    public TerminalNode SORT() {
      return getToken(CarbonSqlBaseParser.SORT, 0);
    }

    public TerminalNode SORTED() {
      return getToken(CarbonSqlBaseParser.SORTED, 0);
    }

    public TerminalNode START() {
      return getToken(CarbonSqlBaseParser.START, 0);
    }

    public TerminalNode STATISTICS() {
      return getToken(CarbonSqlBaseParser.STATISTICS, 0);
    }

    public TerminalNode STORED() {
      return getToken(CarbonSqlBaseParser.STORED, 0);
    }

    public TerminalNode STRATIFY() {
      return getToken(CarbonSqlBaseParser.STRATIFY, 0);
    }

    public TerminalNode STRUCT() {
      return getToken(CarbonSqlBaseParser.STRUCT, 0);
    }

    public TerminalNode SUBSTR() {
      return getToken(CarbonSqlBaseParser.SUBSTR, 0);
    }

    public TerminalNode SUBSTRING() {
      return getToken(CarbonSqlBaseParser.SUBSTRING, 0);
    }

    public TerminalNode TABLE() {
      return getToken(CarbonSqlBaseParser.TABLE, 0);
    }

    public TerminalNode TABLES() {
      return getToken(CarbonSqlBaseParser.TABLES, 0);
    }

    public TerminalNode TABLESAMPLE() {
      return getToken(CarbonSqlBaseParser.TABLESAMPLE, 0);
    }

    public TerminalNode TBLPROPERTIES() {
      return getToken(CarbonSqlBaseParser.TBLPROPERTIES, 0);
    }

    public TerminalNode TEMPORARY() {
      return getToken(CarbonSqlBaseParser.TEMPORARY, 0);
    }

    public TerminalNode TERMINATED() {
      return getToken(CarbonSqlBaseParser.TERMINATED, 0);
    }

    public TerminalNode THEN() {
      return getToken(CarbonSqlBaseParser.THEN, 0);
    }

    public TerminalNode TIME() {
      return getToken(CarbonSqlBaseParser.TIME, 0);
    }

    public TerminalNode TO() {
      return getToken(CarbonSqlBaseParser.TO, 0);
    }

    public TerminalNode TOUCH() {
      return getToken(CarbonSqlBaseParser.TOUCH, 0);
    }

    public TerminalNode TRAILING() {
      return getToken(CarbonSqlBaseParser.TRAILING, 0);
    }

    public TerminalNode TRANSACTION() {
      return getToken(CarbonSqlBaseParser.TRANSACTION, 0);
    }

    public TerminalNode TRANSACTIONS() {
      return getToken(CarbonSqlBaseParser.TRANSACTIONS, 0);
    }

    public TerminalNode TRANSFORM() {
      return getToken(CarbonSqlBaseParser.TRANSFORM, 0);
    }

    public TerminalNode TRIM() {
      return getToken(CarbonSqlBaseParser.TRIM, 0);
    }

    public TerminalNode TRUE() {
      return getToken(CarbonSqlBaseParser.TRUE, 0);
    }

    public TerminalNode TRUNCATE() {
      return getToken(CarbonSqlBaseParser.TRUNCATE, 0);
    }

    public TerminalNode TYPE() {
      return getToken(CarbonSqlBaseParser.TYPE, 0);
    }

    public TerminalNode UNARCHIVE() {
      return getToken(CarbonSqlBaseParser.UNARCHIVE, 0);
    }

    public TerminalNode UNBOUNDED() {
      return getToken(CarbonSqlBaseParser.UNBOUNDED, 0);
    }

    public TerminalNode UNCACHE() {
      return getToken(CarbonSqlBaseParser.UNCACHE, 0);
    }

    public TerminalNode UNIQUE() {
      return getToken(CarbonSqlBaseParser.UNIQUE, 0);
    }

    public TerminalNode UNKNOWN() {
      return getToken(CarbonSqlBaseParser.UNKNOWN, 0);
    }

    public TerminalNode UNLOCK() {
      return getToken(CarbonSqlBaseParser.UNLOCK, 0);
    }

    public TerminalNode UNSET() {
      return getToken(CarbonSqlBaseParser.UNSET, 0);
    }

    public TerminalNode UPDATE() {
      return getToken(CarbonSqlBaseParser.UPDATE, 0);
    }

    public TerminalNode USE() {
      return getToken(CarbonSqlBaseParser.USE, 0);
    }

    public TerminalNode USER() {
      return getToken(CarbonSqlBaseParser.USER, 0);
    }

    public TerminalNode VALUES() {
      return getToken(CarbonSqlBaseParser.VALUES, 0);
    }

    public TerminalNode VIEW() {
      return getToken(CarbonSqlBaseParser.VIEW, 0);
    }

    public TerminalNode VIEWS() {
      return getToken(CarbonSqlBaseParser.VIEWS, 0);
    }

    public TerminalNode WHEN() {
      return getToken(CarbonSqlBaseParser.WHEN, 0);
    }

    public TerminalNode WHERE() {
      return getToken(CarbonSqlBaseParser.WHERE, 0);
    }

    public TerminalNode WINDOW() {
      return getToken(CarbonSqlBaseParser.WINDOW, 0);
    }

    public TerminalNode WITH() {
      return getToken(CarbonSqlBaseParser.WITH, 0);
    }

    public TerminalNode ZONE() {
      return getToken(CarbonSqlBaseParser.ZONE, 0);
    }

    public NonReservedContext(ParserRuleContext parent, int invokingState) {
      super(parent, invokingState);
    }

    @Override
    public int getRuleIndex() {
      return RULE_nonReserved;
    }

    @Override
    public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
      if (visitor instanceof CarbonSqlBaseVisitor)
        return ((CarbonSqlBaseVisitor<? extends T>) visitor).visitNonReserved(this);
      else return visitor.visitChildren(this);
    }
  }

  public final NonReservedContext nonReserved() throws RecognitionException {
    NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
    enterRule(_localctx, 274, RULE_nonReserved);
    int _la;
    try {
      enterOuterAlt(_localctx, 1);
      {
        setState(3061);
        _la = _input.LA(1);
        if (!(((((_la - 12)) & ~0x3f) == 0 &&
            ((1L << (_la - 12)) & ((1L << (ADD - 12)) | (1L << (AFTER - 12)) | (1L << (ALL - 12))
                | (1L << (ALTER - 12)) | (1L << (ANALYZE - 12)) | (1L << (AND - 12)) | (1L << (ANY
                - 12)) | (1L << (ARCHIVE - 12)) | (1L << (ARRAY - 12)) | (1L << (AS - 12)) | (1L
                << (ASC - 12)) | (1L << (AT - 12)) | (1L << (AUTHORIZATION - 12)) | (1L << (BETWEEN
                - 12)) | (1L << (BOTH - 12)) | (1L << (BUCKET - 12)) | (1L << (BUCKETS - 12)) | (1L
                << (BY - 12)) | (1L << (CACHE - 12)) | (1L << (CASCADE - 12)) | (1L << (CASE - 12))
                | (1L << (CAST - 12)) | (1L << (CHANGE - 12)) | (1L << (CHECK - 12)) | (1L << (CLEAR
                - 12)) | (1L << (CLUSTER - 12)) | (1L << (CLUSTERED - 12)) | (1L << (CODEGEN - 12))
                | (1L << (COLLATE - 12)) | (1L << (COLLECTION - 12)) | (1L << (COLUMN - 12)) | (1L
                << (COLUMNS - 12)) | (1L << (COMMENT - 12)) | (1L << (COMMIT - 12)) | (1L << (
                COMPACT - 12)) | (1L << (COMPACTIONS - 12)) | (1L << (COMPUTE - 12)) | (1L << (
                CONCATENATE - 12)) | (1L << (CONSTRAINT - 12)) | (1L << (COST - 12)) | (1L << (
                CREATE - 12)) | (1L << (CUBE - 12)) | (1L << (CURRENT - 12)) | (1L << (CURRENT_DATE
                - 12)) | (1L << (CURRENT_TIME - 12)) | (1L << (CURRENT_TIMESTAMP - 12)) | (1L << (
                CURRENT_USER - 12)) | (1L << (DATA - 12)) | (1L << (DATABASE - 12)) | (1L << (
                DATABASES - 12)) | (1L << (DBPROPERTIES - 12)) | (1L << (DEFINED - 12)) | (1L << (
                DELETE - 12)) | (1L << (DELIMITED - 12)) | (1L << (DESC - 12)) | (1L << (DESCRIBE
                - 12)) | (1L << (DFS - 12)) | (1L << (DIRECTORIES - 12)) | (1L << (DIRECTORY - 12))
                | (1L << (DISTINCT - 12)) | (1L << (DISTRIBUTE - 12)) | (1L << (DIV - 12)))) != 0)
            || ((((_la - 76)) & ~0x3f) == 0 &&
            ((1L << (_la - 76)) & ((1L << (DROP - 76)) | (1L << (ELSE - 76)) | (1L << (END - 76))
                | (1L << (ESCAPE - 76)) | (1L << (ESCAPED - 76)) | (1L << (EXCHANGE - 76)) | (1L
                << (EXISTS - 76)) | (1L << (EXPLAIN - 76)) | (1L << (EXPORT - 76)) | (1L << (
                EXTENDED - 76)) | (1L << (EXTERNAL - 76)) | (1L << (EXTRACT - 76)) | (1L << (FALSE
                - 76)) | (1L << (FETCH - 76)) | (1L << (FIELDS - 76)) | (1L << (FILTER - 76)) | (1L
                << (FILEFORMAT - 76)) | (1L << (FIRST - 76)) | (1L << (FOLLOWING - 76)) | (1L << (
                FOR - 76)) | (1L << (FOREIGN - 76)) | (1L << (FORMAT - 76)) | (1L << (FORMATTED
                - 76)) | (1L << (FROM - 76)) | (1L << (FUNCTION - 76)) | (1L << (FUNCTIONS - 76))
                | (1L << (GLOBAL - 76)) | (1L << (GRANT - 76)) | (1L << (GROUP - 76)) | (1L << (
                GROUPING - 76)) | (1L << (HAVING - 76)) | (1L << (IF - 76)) | (1L << (IGNORE - 76))
                | (1L << (IMPORT - 76)) | (1L << (IN - 76)) | (1L << (INDEX - 76)) | (1L << (INDEXES
                - 76)) | (1L << (INPATH - 76)) | (1L << (INPUTFORMAT - 76)) | (1L << (INSERT - 76))
                | (1L << (INTERVAL - 76)) | (1L << (INTO - 76)) | (1L << (IS - 76)) | (1L << (ITEMS
                - 76)) | (1L << (KEYS - 76)) | (1L << (LAST - 76)) | (1L << (LATERAL - 76)) | (1L
                << (LAZY - 76)) | (1L << (LEADING - 76)) | (1L << (LIKE - 76)) | (1L << (LIMIT
                - 76)) | (1L << (LINES - 76)) | (1L << (LIST - 76)) | (1L << (LOAD - 76)) | (1L << (
                LOCAL - 76)) | (1L << (LOCATION - 76)) | (1L << (LOCK - 76)) | (1L << (LOCKS
                - 76)))) != 0) || ((((_la - 140)) & ~0x3f) == 0 &&
            ((1L << (_la - 140)) & ((1L << (LOGICAL - 140)) | (1L << (MACRO - 140)) | (1L << (MAP
                - 140)) | (1L << (MATCHED - 140)) | (1L << (MERGE - 140)) | (1L << (MSCK - 140)) | (
                1L << (NAMESPACE - 140)) | (1L << (NAMESPACES - 140)) | (1L << (NO - 140)) | (1L
                << (NOT - 140)) | (1L << (NULL - 140)) | (1L << (NULLS - 140)) | (1L << (OF - 140))
                | (1L << (ONLY - 140)) | (1L << (OPTION - 140)) | (1L << (OPTIONS - 140)) | (1L << (
                OR - 140)) | (1L << (ORDER - 140)) | (1L << (OUT - 140)) | (1L << (OUTER - 140)) | (
                1L << (OUTPUTFORMAT - 140)) | (1L << (OVER - 140)) | (1L << (OVERLAPS - 140)) | (1L
                << (OVERLAY - 140)) | (1L << (OVERWRITE - 140)) | (1L << (PARTITION - 140)) | (1L
                << (PARTITIONED - 140)) | (1L << (PARTITIONS - 140)) | (1L << (PERCENTLIT - 140))
                | (1L << (PIVOT - 140)) | (1L << (PLACING - 140)) | (1L << (POSITION - 140)) | (1L
                << (PRECEDING - 140)) | (1L << (PRIMARY - 140)) | (1L << (PRINCIPALS - 140)) | (1L
                << (PROPERTIES - 140)) | (1L << (PURGE - 140)) | (1L << (QUERY - 140)) | (1L << (
                RANGE - 140)) | (1L << (RECORDREADER - 140)) | (1L << (RECORDWRITER - 140)) | (1L
                << (RECOVER - 140)) | (1L << (REDUCE - 140)) | (1L << (REFERENCES - 140)) | (1L << (
                REFRESH - 140)) | (1L << (RENAME - 140)) | (1L << (REPAIR - 140)) | (1L << (REPLACE
                - 140)) | (1L << (RESET - 140)) | (1L << (RESTRICT - 140)) | (1L << (REVOKE - 140))
                | (1L << (RLIKE - 140)) | (1L << (ROLE - 140)) | (1L << (ROLES - 140)) | (1L << (
                ROLLBACK - 140)) | (1L << (ROLLUP - 140)) | (1L << (ROW - 140)) | (1L << (ROWS
                - 140)) | (1L << (SCHEMA - 140)) | (1L << (SELECT - 140)))) != 0) || (
            (((_la - 204)) & ~0x3f) == 0 &&
                ((1L << (_la - 204)) & ((1L << (SEPARATED - 204)) | (1L << (SERDE - 204)) | (1L << (
                    SERDEPROPERTIES - 204)) | (1L << (SESSION_USER - 204)) | (1L << (SET - 204)) | (
                    1L << (SETS - 204)) | (1L << (SHOW - 204)) | (1L << (SKEWED - 204)) | (1L << (
                    SOME - 204)) | (1L << (SORT - 204)) | (1L << (SORTED - 204)) | (1L << (START
                    - 204)) | (1L << (STATISTICS - 204)) | (1L << (STORED - 204)) | (1L << (STRATIFY
                    - 204)) | (1L << (STRUCT - 204)) | (1L << (SUBSTR - 204)) | (1L << (SUBSTRING
                    - 204)) | (1L << (TABLE - 204)) | (1L << (TABLES - 204)) | (1L << (TABLESAMPLE
                    - 204)) | (1L << (TBLPROPERTIES - 204)) | (1L << (TEMPORARY - 204)) | (1L << (
                    TERMINATED - 204)) | (1L << (THEN - 204)) | (1L << (TIME - 204)) | (1L << (TO
                    - 204)) | (1L << (TOUCH - 204)) | (1L << (TRAILING - 204)) | (1L << (TRANSACTION
                    - 204)) | (1L << (TRANSACTIONS - 204)) | (1L << (TRANSFORM - 204)) | (1L << (
                    TRIM - 204)) | (1L << (TRUE - 204)) | (1L << (TRUNCATE - 204)) | (1L << (TYPE
                    - 204)) | (1L << (UNARCHIVE - 204)) | (1L << (UNBOUNDED - 204)) | (1L << (
                    UNCACHE - 204)) | (1L << (UNIQUE - 204)) | (1L << (UNKNOWN - 204)) | (1L << (
                    UNLOCK - 204)) | (1L << (UNSET - 204)) | (1L << (UPDATE - 204)) | (1L << (USE
                    - 204)) | (1L << (USER - 204)) | (1L << (VALUES - 204)) | (1L << (VIEW - 204))
                    | (1L << (VIEWS - 204)) | (1L << (WHEN - 204)) | (1L << (WHERE - 204)) | (1L
                    << (WINDOW - 204)) | (1L << (WITH - 204)) | (1L << (ZONE - 204)))) != 0))) {
          _errHandler.recoverInline(this);
        } else {
          if (_input.LA(1) == Token.EOF) matchedEOF = true;
          _errHandler.reportMatch(this);
          consume();
        }
      }
    } catch (RecognitionException re) {
      _localctx.exception = re;
      _errHandler.reportError(this, re);
      _errHandler.recover(this, re);
    } finally {
      exitRule();
    }
    return _localctx;
  }

  public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
    switch (ruleIndex) {
      case 42:
        return queryTerm_sempred((QueryTermContext) _localctx, predIndex);
      case 96:
        return booleanExpression_sempred((BooleanExpressionContext) _localctx, predIndex);
      case 98:
        return valueExpression_sempred((ValueExpressionContext) _localctx, predIndex);
      case 99:
        return primaryExpression_sempred((PrimaryExpressionContext) _localctx, predIndex);
      case 130:
        return identifier_sempred((IdentifierContext) _localctx, predIndex);
      case 131:
        return strictIdentifier_sempred((StrictIdentifierContext) _localctx, predIndex);
      case 133:
        return number_sempred((NumberContext) _localctx, predIndex);
    }
    return true;
  }

  private boolean queryTerm_sempred(QueryTermContext _localctx, int predIndex) {
    switch (predIndex) {
      case 0:
        return precpred(_ctx, 3);
      case 1:
        return legacy_setops_precedence_enbled;
      case 2:
        return precpred(_ctx, 2);
      case 3:
        return !legacy_setops_precedence_enbled;
      case 4:
        return precpred(_ctx, 1);
      case 5:
        return !legacy_setops_precedence_enbled;
    }
    return true;
  }

  private boolean booleanExpression_sempred(BooleanExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 6:
        return precpred(_ctx, 2);
      case 7:
        return precpred(_ctx, 1);
    }
    return true;
  }

  private boolean valueExpression_sempred(ValueExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 8:
        return precpred(_ctx, 6);
      case 9:
        return precpred(_ctx, 5);
      case 10:
        return precpred(_ctx, 4);
      case 11:
        return precpred(_ctx, 3);
      case 12:
        return precpred(_ctx, 2);
      case 13:
        return precpred(_ctx, 1);
    }
    return true;
  }

  private boolean primaryExpression_sempred(PrimaryExpressionContext _localctx, int predIndex) {
    switch (predIndex) {
      case 14:
        return precpred(_ctx, 8);
      case 15:
        return precpred(_ctx, 6);
    }
    return true;
  }

  private boolean identifier_sempred(IdentifierContext _localctx, int predIndex) {
    switch (predIndex) {
      case 16:
        return !SQL_standard_keyword_behavior;
    }
    return true;
  }

  private boolean strictIdentifier_sempred(StrictIdentifierContext _localctx, int predIndex) {
    switch (predIndex) {
      case 17:
        return SQL_standard_keyword_behavior;
      case 18:
        return !SQL_standard_keyword_behavior;
    }
    return true;
  }

  private boolean number_sempred(NumberContext _localctx, int predIndex) {
    switch (predIndex) {
      case 19:
        return !legacy_exponent_literal_as_decimal_enabled;
      case 20:
        return !legacy_exponent_literal_as_decimal_enabled;
      case 21:
        return legacy_exponent_literal_as_decimal_enabled;
    }
    return true;
  }

  private static final int _serializedATNSegments = 2;
  private static final String _serializedATNSegment0 =
      "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u0128\u0bfa\4\2\t"
          + "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"
          + "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"
          + "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"
          + "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"
          + "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"
          + ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"
          + "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="
          + "\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"
          + "\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"
          + "\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"
          + "`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"
          + "k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"
          + "w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"
          + "\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084\4\u0085"
          + "\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089\t\u0089"
          + "\4\u008a\t\u008a\4\u008b\t\u008b\3\2\3\2\7\2\u0119\n\2\f\2\16\2\u011c"
          + "\13\2\3\2\3\2\3\3\3\3\3\3\3\4\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\6\3\7\3\7"
          + "\3\7\3\b\3\b\3\b\3\t\3\t\5\t\u0134\n\t\3\t\3\t\3\t\5\t\u0139\n\t\3\t\3"
          + "\t\3\t\3\t\3\t\3\t\5\t\u0141\n\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u0149\n\t"
          + "\f\t\16\t\u014c\13\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"
          + "\t\3\t\3\t\3\t\3\t\5\t\u015f\n\t\3\t\3\t\5\t\u0163\n\t\3\t\3\t\3\t\3\t"
          + "\5\t\u0169\n\t\3\t\5\t\u016c\n\t\3\t\5\t\u016f\n\t\3\t\3\t\3\t\3\t\3\t"
          + "\5\t\u0176\n\t\3\t\3\t\3\t\5\t\u017b\n\t\3\t\5\t\u017e\n\t\3\t\3\t\3\t"
          + "\3\t\3\t\5\t\u0185\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0191"
          + "\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u019a\n\t\f\t\16\t\u019d\13\t\3\t"
          + "\5\t\u01a0\n\t\3\t\5\t\u01a3\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u01aa\n\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u01b5\n\t\f\t\16\t\u01b8\13\t\3\t"
          + "\3\t\3\t\3\t\3\t\5\t\u01bf\n\t\3\t\3\t\3\t\5\t\u01c4\n\t\3\t\5\t\u01c7"
          + "\n\t\3\t\3\t\3\t\3\t\5\t\u01cd\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"
          + "\5\t\u01d8\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"
          + "\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0218\n\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0221\n\t\3\t\3\t\5\t\u0225\n\t\3\t\3\t\3"
          + "\t\3\t\5\t\u022b\n\t\3\t\3\t\5\t\u022f\n\t\3\t\3\t\3\t\5\t\u0234\n\t\3"
          + "\t\3\t\3\t\3\t\5\t\u023a\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5"
          + "\t\u0246\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u024e\n\t\3\t\3\t\3\t\3\t\5\t"
          + "\u0254\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0261\n\t\3"
          + "\t\6\t\u0264\n\t\r\t\16\t\u0265\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"
          + "\t\3\t\3\t\3\t\3\t\5\t\u0276\n\t\3\t\3\t\3\t\7\t\u027b\n\t\f\t\16\t\u027e"
          + "\13\t\3\t\5\t\u0281\n\t\3\t\3\t\3\t\3\t\5\t\u0287\n\t\3\t\3\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0296\n\t\3\t\3\t\5\t\u029a\n"
          + "\t\3\t\3\t\3\t\3\t\5\t\u02a0\n\t\3\t\3\t\3\t\3\t\5\t\u02a6\n\t\3\t\5\t"
          + "\u02a9\n\t\3\t\5\t\u02ac\n\t\3\t\3\t\3\t\3\t\5\t\u02b2\n\t\3\t\3\t\5\t"
          + "\u02b6\n\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u02be\n\t\f\t\16\t\u02c1\13\t\3"
          + "\t\3\t\3\t\3\t\3\t\3\t\5\t\u02c9\n\t\3\t\5\t\u02cc\n\t\3\t\3\t\3\t\3\t"
          + "\3\t\3\t\3\t\5\t\u02d5\n\t\3\t\3\t\3\t\5\t\u02da\n\t\3\t\3\t\3\t\3\t\5"
          + "\t\u02e0\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u02e7\n\t\3\t\5\t\u02ea\n\t\3\t\3"
          + "\t\3\t\3\t\5\t\u02f0\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u02f9\n\t\f\t"
          + "\16\t\u02fc\13\t\5\t\u02fe\n\t\3\t\3\t\5\t\u0302\n\t\3\t\3\t\3\t\5\t\u0307"
          + "\n\t\3\t\3\t\3\t\5\t\u030c\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0313\n\t\3\t\5"
          + "\t\u0316\n\t\3\t\5\t\u0319\n\t\3\t\3\t\3\t\3\t\3\t\5\t\u0320\n\t\3\t\3"
          + "\t\3\t\5\t\u0325\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u032e\n\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\5\t\u0336\n\t\3\t\3\t\3\t\3\t\5\t\u033c\n\t\3\t\5\t\u033f"
          + "\n\t\3\t\5\t\u0342\n\t\3\t\3\t\3\t\3\t\5\t\u0348\n\t\3\t\3\t\5\t\u034c"
          + "\n\t\3\t\3\t\5\t\u0350\n\t\3\t\3\t\5\t\u0354\n\t\5\t\u0356\n\t\3\t\3\t"
          + "\3\t\3\t\3\t\3\t\5\t\u035e\n\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u0366\n\t\3"
          + "\t\3\t\3\t\3\t\5\t\u036c\n\t\3\t\3\t\3\t\3\t\5\t\u0372\n\t\3\t\5\t\u0375"
          + "\n\t\3\t\3\t\5\t\u0379\n\t\3\t\5\t\u037c\n\t\3\t\3\t\5\t\u0380\n\t\3\t"
          + "\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3"
          + "\t\3\t\3\t\3\t\3\t\3\t\7\t\u039a\n\t\f\t\16\t\u039d\13\t\5\t\u039f\n\t"
          + "\3\t\3\t\5\t\u03a3\n\t\3\t\3\t\3\t\3\t\5\t\u03a9\n\t\3\t\5\t\u03ac\n\t"
          + "\3\t\5\t\u03af\n\t\3\t\3\t\3\t\3\t\5\t\u03b5\n\t\3\t\3\t\3\t\3\t\3\t\3"
          + "\t\5\t\u03bd\n\t\3\t\3\t\3\t\5\t\u03c2\n\t\3\t\3\t\3\t\3\t\5\t\u03c8\n"
          + "\t\3\t\3\t\3\t\3\t\5\t\u03ce\n\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u03d8"
          + "\n\t\f\t\16\t\u03db\13\t\5\t\u03dd\n\t\3\t\3\t\3\t\7\t\u03e2\n\t\f\t\16"
          + "\t\u03e5\13\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\7\t\u03f3"
          + "\n\t\f\t\16\t\u03f6\13\t\3\t\3\t\3\t\3\t\7\t\u03fc\n\t\f\t\16\t\u03ff"
          + "\13\t\5\t\u0401\n\t\3\t\3\t\7\t\u0405\n\t\f\t\16\t\u0408\13\t\3\t\3\t"
          + "\3\t\3\t\7\t\u040e\n\t\f\t\16\t\u0411\13\t\3\t\3\t\7\t\u0415\n\t\f\t\16"
          + "\t\u0418\13\t\5\t\u041a\n\t\3\n\3\n\3\13\3\13\3\13\3\13\3\13\3\13\5\13"
          + "\u0424\n\13\3\13\3\13\5\13\u0428\n\13\3\13\3\13\3\13\3\13\3\13\5\13\u042f"
          + "\n\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\5\13\u04a3\n\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u04ab"
          + "\n\13\3\13\3\13\3\13\3\13\3\13\3\13\5\13\u04b3\n\13\3\13\3\13\3\13\3\13"
          + "\3\13\3\13\3\13\5\13\u04bc\n\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13\3\13"
          + "\5\13\u04c6\n\13\3\f\3\f\5\f\u04ca\n\f\3\f\5\f\u04cd\n\f\3\f\3\f\3\f\3"
          + "\f\5\f\u04d3\n\f\3\f\3\f\3\r\3\r\5\r\u04d9\n\r\3\r\3\r\3\r\3\r\3\16\3"
          + "\16\3\16\3\16\3\16\3\16\5\16\u04e5\n\16\3\16\3\16\3\16\3\16\3\17\3\17"
          + "\3\17\3\17\3\17\3\17\5\17\u04f1\n\17\3\17\3\17\3\17\5\17\u04f6\n\17\3"
          + "\20\3\20\3\20\3\21\3\21\3\21\3\22\5\22\u04ff\n\22\3\22\3\22\3\22\3\23"
          + "\3\23\3\23\5\23\u0507\n\23\3\23\3\23\3\23\3\23\3\23\5\23\u050e\n\23\5"
          + "\23\u0510\n\23\3\23\3\23\3\23\5\23\u0515\n\23\3\23\3\23\5\23\u0519\n\23"
          + "\3\23\3\23\3\23\5\23\u051e\n\23\3\23\3\23\3\23\5\23\u0523\n\23\3\23\3"
          + "\23\3\23\5\23\u0528\n\23\3\23\5\23\u052b\n\23\3\23\3\23\3\23\5\23\u0530"
          + "\n\23\3\23\3\23\5\23\u0534\n\23\3\23\3\23\3\23\5\23\u0539\n\23\5\23\u053b"
          + "\n\23\3\24\3\24\5\24\u053f\n\24\3\25\3\25\3\25\3\25\3\25\7\25\u0546\n"
          + "\25\f\25\16\25\u0549\13\25\3\25\3\25\3\26\3\26\3\26\5\26\u0550\n\26\3"
          + "\27\3\27\3\30\3\30\3\30\3\30\3\30\5\30\u0559\n\30\3\31\3\31\3\31\7\31"
          + "\u055e\n\31\f\31\16\31\u0561\13\31\3\32\3\32\3\32\3\32\7\32\u0567\n\32"
          + "\f\32\16\32\u056a\13\32\3\33\3\33\5\33\u056e\n\33\3\33\5\33\u0571\n\33"
          + "\3\33\3\33\3\33\3\33\3\34\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35"
          + "\3\35\3\35\3\35\7\35\u0584\n\35\f\35\16\35\u0587\13\35\3\36\3\36\3\36"
          + "\3\36\7\36\u058d\n\36\f\36\16\36\u0590\13\36\3\36\3\36\3\37\3\37\5\37"
          + "\u0596\n\37\3\37\5\37\u0599\n\37\3 \3 \3 \7 \u059e\n \f \16 \u05a1\13"
          + " \3 \5 \u05a4\n \3!\3!\3!\3!\5!\u05aa\n!\3\"\3\"\3\"\3\"\7\"\u05b0\n\""
          + "\f\"\16\"\u05b3\13\"\3\"\3\"\3#\3#\3#\3#\7#\u05bb\n#\f#\16#\u05be\13#"
          + "\3#\3#\3$\3$\3$\3$\3$\3$\5$\u05c8\n$\3%\3%\3%\3%\3%\5%\u05cf\n%\3&\3&"
          + "\3&\3&\5&\u05d5\n&\3\'\3\'\3\'\3(\3(\3(\3(\3(\3(\6(\u05e0\n(\r(\16(\u05e1"
          + "\3(\3(\3(\3(\3(\5(\u05e9\n(\3(\3(\3(\3(\3(\5(\u05f0\n(\3(\3(\3(\3(\3("
          + "\3(\3(\3(\3(\3(\5(\u05fc\n(\3(\3(\3(\3(\7(\u0602\n(\f(\16(\u0605\13(\3"
          + "(\7(\u0608\n(\f(\16(\u060b\13(\5(\u060d\n(\3)\3)\3)\3)\3)\3)\3)\3)\3)"
          + "\3)\5)\u0619\n)\3)\3)\3)\3)\7)\u061f\n)\f)\16)\u0622\13)\3)\7)\u0625\n"
          + ")\f)\16)\u0628\13)\3*\3*\3*\3*\3*\7*\u062f\n*\f*\16*\u0632\13*\5*\u0634"
          + "\n*\3*\3*\3*\3*\3*\7*\u063b\n*\f*\16*\u063e\13*\5*\u0640\n*\3*\3*\3*\3"
          + "*\3*\7*\u0647\n*\f*\16*\u064a\13*\5*\u064c\n*\3*\3*\3*\3*\3*\7*\u0653"
          + "\n*\f*\16*\u0656\13*\5*\u0658\n*\3*\5*\u065b\n*\3*\3*\3*\5*\u0660\n*\5"
          + "*\u0662\n*\3+\3+\3+\3,\3,\3,\3,\3,\3,\3,\5,\u066e\n,\3,\3,\3,\3,\3,\5"
          + ",\u0675\n,\3,\3,\3,\3,\3,\5,\u067c\n,\3,\7,\u067f\n,\f,\16,\u0682\13,"
          + "\3-\3-\3-\3-\3-\3-\3-\3-\3-\5-\u068d\n-\3.\3.\5.\u0691\n.\3.\3.\5.\u0695"
          + "\n.\3/\3/\6/\u0699\n/\r/\16/\u069a\3\60\3\60\5\60\u069f\n\60\3\60\3\60"
          + "\3\60\3\60\7\60\u06a5\n\60\f\60\16\60\u06a8\13\60\3\60\5\60\u06ab\n\60"
          + "\3\60\5\60\u06ae\n\60\3\60\5\60\u06b1\n\60\3\60\5\60\u06b4\n\60\3\60\3"
          + "\60\5\60\u06b8\n\60\3\61\3\61\5\61\u06bc\n\61\3\61\5\61\u06bf\n\61\3\61"
          + "\3\61\5\61\u06c3\n\61\3\61\7\61\u06c6\n\61\f\61\16\61\u06c9\13\61\3\61"
          + "\5\61\u06cc\n\61\3\61\5\61\u06cf\n\61\3\61\5\61\u06d2\n\61\3\61\5\61\u06d5"
          + "\n\61\5\61\u06d7\n\61\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62\3\62"
          + "\5\62\u06e3\n\62\3\62\5\62\u06e6\n\62\3\62\3\62\5\62\u06ea\n\62\3\62\3"
          + "\62\3\62\3\62\3\62\3\62\3\62\3\62\5\62\u06f4\n\62\3\62\3\62\5\62\u06f8"
          + "\n\62\5\62\u06fa\n\62\3\62\5\62\u06fd\n\62\3\62\3\62\5\62\u0701\n\62\3"
          + "\63\3\63\7\63\u0705\n\63\f\63\16\63\u0708\13\63\3\63\5\63\u070b\n\63\3"
          + "\63\3\63\3\64\3\64\3\64\3\65\3\65\3\65\3\65\5\65\u0716\n\65\3\65\3\65"
          + "\3\65\3\66\3\66\3\66\3\66\3\66\5\66\u0720\n\66\3\66\3\66\3\66\3\67\3\67"
          + "\3\67\3\67\3\67\3\67\3\67\5\67\u072c\n\67\38\38\38\38\38\38\38\38\38\3"
          + "8\38\78\u0739\n8\f8\168\u073c\138\38\38\58\u0740\n8\39\39\39\79\u0745"
          + "\n9\f9\169\u0748\139\3:\3:\3:\3:\3;\3;\3;\3<\3<\3<\3=\3=\3=\5=\u0757\n"
          + "=\3=\7=\u075a\n=\f=\16=\u075d\13=\3=\3=\3>\3>\3>\3>\3>\3>\7>\u0767\n>"
          + "\f>\16>\u076a\13>\3>\3>\5>\u076e\n>\3?\3?\3?\3?\7?\u0774\n?\f?\16?\u0777"
          + "\13?\3?\7?\u077a\n?\f?\16?\u077d\13?\3?\5?\u0780\n?\3@\3@\3@\3@\3@\7@"
          + "\u0787\n@\f@\16@\u078a\13@\3@\3@\3@\3@\3@\3@\3@\3@\3@\3@\7@\u0796\n@\f"
          + "@\16@\u0799\13@\3@\3@\5@\u079d\n@\3@\3@\3@\3@\3@\3@\3@\3@\7@\u07a7\n@"
          + "\f@\16@\u07aa\13@\3@\3@\5@\u07ae\n@\3A\3A\3A\3A\7A\u07b4\nA\fA\16A\u07b7"
          + "\13A\5A\u07b9\nA\3A\3A\5A\u07bd\nA\3B\3B\3B\3B\3B\3B\3B\3B\3B\3B\7B\u07c9"
          + "\nB\fB\16B\u07cc\13B\3B\3B\3B\3C\3C\3C\3C\3C\7C\u07d6\nC\fC\16C\u07d9"
          + "\13C\3C\3C\5C\u07dd\nC\3D\3D\5D\u07e1\nD\3D\5D\u07e4\nD\3E\3E\3E\5E\u07e9"
          + "\nE\3E\3E\3E\3E\3E\7E\u07f0\nE\fE\16E\u07f3\13E\5E\u07f5\nE\3E\3E\3E\5"
          + "E\u07fa\nE\3E\3E\3E\7E\u07ff\nE\fE\16E\u0802\13E\5E\u0804\nE\3F\3F\3G"
          + "\3G\7G\u080a\nG\fG\16G\u080d\13G\3H\3H\3H\3H\5H\u0813\nH\3H\3H\3H\3H\3"
          + "H\5H\u081a\nH\3I\5I\u081d\nI\3I\3I\3I\5I\u0822\nI\3I\5I\u0825\nI\3I\3"
          + "I\3I\5I\u082a\nI\3I\3I\5I\u082e\nI\3I\5I\u0831\nI\3I\5I\u0834\nI\3J\3"
          + "J\3J\3J\5J\u083a\nJ\3K\3K\3K\5K\u083f\nK\3K\3K\3L\5L\u0844\nL\3L\3L\3"
          + "L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\3L\5L\u0856\nL\5L\u0858\nL\3L\5"
          + "L\u085b\nL\3M\3M\3M\3M\3N\3N\3N\7N\u0864\nN\fN\16N\u0867\13N\3O\3O\3O"
          + "\3O\7O\u086d\nO\fO\16O\u0870\13O\3O\3O\3P\3P\5P\u0876\nP\3Q\3Q\3Q\3Q\7"
          + "Q\u087c\nQ\fQ\16Q\u087f\13Q\3Q\3Q\3R\3R\5R\u0885\nR\3S\3S\5S\u0889\nS"
          + "\3S\3S\3S\3S\3S\3S\5S\u0891\nS\3S\3S\3S\3S\3S\3S\5S\u0899\nS\3S\3S\3S"
          + "\3S\5S\u089f\nS\3T\3T\3T\3T\7T\u08a5\nT\fT\16T\u08a8\13T\3T\3T\3U\3U\3"
          + "U\3U\3U\7U\u08b1\nU\fU\16U\u08b4\13U\5U\u08b6\nU\3U\3U\3U\3V\5V\u08bc"
          + "\nV\3V\3V\5V\u08c0\nV\5V\u08c2\nV\3W\3W\3W\3W\3W\3W\3W\5W\u08cb\nW\3W"
          + "\3W\3W\3W\3W\3W\3W\3W\3W\3W\5W\u08d7\nW\5W\u08d9\nW\3W\3W\3W\3W\3W\5W"
          + "\u08e0\nW\3W\3W\3W\3W\3W\5W\u08e7\nW\3W\3W\3W\3W\5W\u08ed\nW\3W\3W\3W"
          + "\3W\5W\u08f3\nW\5W\u08f5\nW\3X\3X\3X\7X\u08fa\nX\fX\16X\u08fd\13X\3Y\3"
          + "Y\3Y\7Y\u0902\nY\fY\16Y\u0905\13Y\3Z\3Z\3Z\5Z\u090a\nZ\3Z\3Z\3[\3[\3["
          + "\5[\u0911\n[\3[\3[\3\\\3\\\5\\\u0917\n\\\3\\\3\\\5\\\u091b\n\\\5\\\u091d"
          + "\n\\\3]\3]\3]\7]\u0922\n]\f]\16]\u0925\13]\3^\3^\3^\3^\7^\u092b\n^\f^"
          + "\16^\u092e\13^\3^\3^\3_\3_\3_\3_\3_\3_\7_\u0938\n_\f_\16_\u093b\13_\3"
          + "_\3_\5_\u093f\n_\3`\3`\5`\u0943\n`\3a\3a\3b\3b\3b\3b\3b\3b\3b\3b\3b\3"
          + "b\5b\u0951\nb\5b\u0953\nb\3b\3b\3b\3b\3b\3b\7b\u095b\nb\fb\16b\u095e\13"
          + "b\3c\5c\u0961\nc\3c\3c\3c\3c\3c\3c\5c\u0969\nc\3c\3c\3c\3c\3c\7c\u0970"
          + "\nc\fc\16c\u0973\13c\3c\3c\3c\5c\u0978\nc\3c\3c\3c\3c\3c\3c\5c\u0980\n"
          + "c\3c\3c\3c\5c\u0985\nc\3c\3c\3c\3c\3c\3c\3c\3c\7c\u098f\nc\fc\16c\u0992"
          + "\13c\3c\3c\5c\u0996\nc\3c\5c\u0999\nc\3c\3c\3c\3c\5c\u099f\nc\3c\3c\5"
          + "c\u09a3\nc\3c\3c\3c\5c\u09a8\nc\3c\3c\3c\5c\u09ad\nc\3c\3c\3c\5c\u09b2"
          + "\nc\3d\3d\3d\3d\5d\u09b8\nd\3d\3d\3d\3d\3d\3d\3d\3d\3d\3d\3d\3d\3d\3d"
          + "\3d\3d\3d\3d\3d\7d\u09cd\nd\fd\16d\u09d0\13d\3e\3e\3e\3e\6e\u09d6\ne\r"
          + "e\16e\u09d7\3e\3e\5e\u09dc\ne\3e\3e\3e\3e\3e\6e\u09e3\ne\re\16e\u09e4"
          + "\3e\3e\5e\u09e9\ne\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\7e\u09f9"
          + "\ne\fe\16e\u09fc\13e\5e\u09fe\ne\3e\3e\3e\3e\3e\3e\5e\u0a06\ne\3e\3e\3"
          + "e\3e\3e\3e\3e\5e\u0a0f\ne\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3"
          + "e\3e\3e\3e\3e\6e\u0a24\ne\re\16e\u0a25\3e\3e\3e\3e\3e\3e\3e\3e\3e\5e\u0a31"
          + "\ne\3e\3e\3e\7e\u0a36\ne\fe\16e\u0a39\13e\5e\u0a3b\ne\3e\3e\3e\3e\3e\3"
          + "e\3e\5e\u0a44\ne\3e\3e\5e\u0a48\ne\3e\3e\3e\3e\3e\3e\3e\3e\6e\u0a52\n"
          + "e\re\16e\u0a53\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3"
          + "e\3e\3e\3e\3e\5e\u0a6d\ne\3e\3e\3e\3e\3e\5e\u0a74\ne\3e\5e\u0a77\ne\3"
          + "e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\3e\5e\u0a86\ne\3e\3e\5e\u0a8a\ne\3"
          + "e\3e\3e\3e\3e\3e\3e\3e\7e\u0a94\ne\fe\16e\u0a97\13e\3f\3f\3f\3f\3f\3f"
          + "\3f\3f\6f\u0aa1\nf\rf\16f\u0aa2\5f\u0aa5\nf\3g\3g\3h\3h\3i\3i\3j\3j\3"
          + "k\3k\3k\5k\u0ab2\nk\3l\3l\5l\u0ab6\nl\3m\3m\3m\6m\u0abb\nm\rm\16m\u0abc"
          + "\3n\3n\3n\5n\u0ac2\nn\3o\3o\3o\3o\3o\3p\5p\u0aca\np\3p\3p\5p\u0ace\np"
          + "\3q\3q\3q\5q\u0ad3\nq\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\3r\5r"
          + "\u0ae4\nr\3r\3r\5r\u0ae8\nr\3r\3r\3r\3r\3r\7r\u0aef\nr\fr\16r\u0af2\13"
          + "r\3r\5r\u0af5\nr\5r\u0af7\nr\3s\3s\3s\7s\u0afc\ns\fs\16s\u0aff\13s\3t"
          + "\3t\3t\3t\5t\u0b05\nt\3t\5t\u0b08\nt\3t\5t\u0b0b\nt\3u\3u\3u\7u\u0b10"
          + "\nu\fu\16u\u0b13\13u\3v\3v\3v\3v\5v\u0b19\nv\3v\5v\u0b1c\nv\3w\3w\3w\7"
          + "w\u0b21\nw\fw\16w\u0b24\13w\3x\3x\3x\3x\3x\5x\u0b2b\nx\3x\5x\u0b2e\nx"
          + "\3y\3y\3y\3y\3y\3z\3z\3z\3z\7z\u0b39\nz\fz\16z\u0b3c\13z\3{\3{\3{\3{\3"
          + "|\3|\3|\3|\3|\3|\3|\3|\3|\3|\3|\7|\u0b4d\n|\f|\16|\u0b50\13|\3|\3|\3|"
          + "\3|\3|\7|\u0b57\n|\f|\16|\u0b5a\13|\5|\u0b5c\n|\3|\3|\3|\3|\3|\7|\u0b63"
          + "\n|\f|\16|\u0b66\13|\5|\u0b68\n|\5|\u0b6a\n|\3|\5|\u0b6d\n|\3|\5|\u0b70"
          + "\n|\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\3}\5}\u0b82\n}\3~\3~"
          + "\3~\3~\3~\3~\3~\5~\u0b8b\n~\3\177\3\177\3\177\7\177\u0b90\n\177\f\177"
          + "\16\177\u0b93\13\177\3\u0080\3\u0080\3\u0080\3\u0080\5\u0080\u0b99\n\u0080"
          + "\3\u0081\3\u0081\3\u0081\7\u0081\u0b9e\n\u0081\f\u0081\16\u0081\u0ba1"
          + "\13\u0081\3\u0082\3\u0082\3\u0082\3\u0083\3\u0083\6\u0083\u0ba8\n\u0083"
          + "\r\u0083\16\u0083\u0ba9\3\u0083\5\u0083\u0bad\n\u0083\3\u0084\3\u0084"
          + "\3\u0084\5\u0084\u0bb2\n\u0084\3\u0085\3\u0085\3\u0085\3\u0085\3\u0085"
          + "\3\u0085\5\u0085\u0bba\n\u0085\3\u0086\3\u0086\3\u0087\3\u0087\5\u0087"
          + "\u0bc0\n\u0087\3\u0087\3\u0087\3\u0087\5\u0087\u0bc5\n\u0087\3\u0087\3"
          + "\u0087\3\u0087\5\u0087\u0bca\n\u0087\3\u0087\3\u0087\5\u0087\u0bce\n\u0087"
          + "\3\u0087\3\u0087\5\u0087\u0bd2\n\u0087\3\u0087\3\u0087\5\u0087\u0bd6\n"
          + "\u0087\3\u0087\3\u0087\5\u0087\u0bda\n\u0087\3\u0087\3\u0087\5\u0087\u0bde"
          + "\n\u0087\3\u0087\3\u0087\5\u0087\u0be2\n\u0087\3\u0087\3\u0087\5\u0087"
          + "\u0be6\n\u0087\3\u0087\5\u0087\u0be9\n\u0087\3\u0088\3\u0088\3\u0088\3"
          + "\u0088\3\u0088\3\u0088\3\u0088\5\u0088\u0bf2\n\u0088\3\u0089\3\u0089\3"
          + "\u008a\3\u008a\3\u008b\3\u008b\3\u008b\n\u039b\u03d9\u03e3\u03f4\u03fd"
          + "\u0406\u040f\u0416\6V\u00c2\u00c6\u00c8\u008c\2\4\6\b\n\f\16\20\22\24"
          + "\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtv"
          + "xz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094"
          + "\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac"
          + "\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4"
          + "\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc"
          + "\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4"
          + "\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102\u0104\u0106\u0108\u010a\u010c"
          + "\u010e\u0110\u0112\u0114\2-\4\2BB\u00b3\u00b3\4\2\"\"\u00c1\u00c1\4\2"
          + "AA\u0095\u0095\4\2ffrr\3\2-.\4\2\u00e1\u00e1\u0100\u0100\4\2\21\21%%\7"
          + "\2**\66\66XXee\u008e\u008e\3\2FG\4\2XXee\4\2\u0099\u0099\u0119\u0119\4"
          + "\2\16\16\u0088\u0088\4\2\u008a\u008a\u0119\u0119\5\2@@\u0094\u0094\u00cb"
          + "\u00cb\6\2SSyy\u00d3\u00d3\u00f6\u00f6\5\2SS\u00d3\u00d3\u00f6\u00f6\4"
          + "\2\31\31FF\4\2``\u0080\u0080\4\2\20\20KK\4\2\u011d\u011d\u011f\u011f\5"
          + "\2\20\20\25\25\u00d7\u00d7\5\2[[\u00f0\u00f0\u00f8\u00f8\4\2\u010f\u0110"
          + "\u0114\u0114\4\2MM\u0111\u0113\4\2\u010f\u0110\u0117\u0117\4\2;;==\3\2"
          + "\u00df\u00e0\4\2\6\6ff\4\2\6\6bb\5\2\35\35\u0083\u0083\u00eb\u00eb\3\2"
          + "\u0107\u010e\4\2MM\u010f\u0118\6\2\23\23rr\u0098\u0098\u00a0\u00a0\4\2"
          + "[[\u00f0\u00f0\3\2\u010f\u0110\4\2LL\u00a9\u00a9\4\2\u00a1\u00a1\u00d8"
          + "\u00d8\4\2aa\u00b0\u00b0\3\2\u011e\u011f\4\2NN\u00d2\u00d2\62\2\16\17"
          + "\21\22\24\24\26\27\31\32\34\34\36\"%%\'*,,.\64\66\669:?JLNRRTZ]]_adeh"
          + "jmmoqstvxzz}}\177\u0082\u0085\u0095\u0097\u0097\u009a\u009b\u009e\u009f"
          + "\u00a2\u00a2\u00a4\u00a5\u00a7\u00b0\u00b2\u00ba\u00bc\u00c2\u00c4\u00cb"
          + "\u00cd\u00d0\u00d2\u00d6\u00d8\u00e0\u00e2\u00e6\u00ea\u00ea\u00ec\u00f5"
          + "\u00f9\u00fc\u00ff\u0101\u0104\u0104\u0106\u0106\21\2\24\2488SSgguuyy"
          + "~~\u0084\u0084\u0096\u0096\u009c\u009c\u00c3\u00c3\u00cd\u00cd\u00d3\u00d3"
          + "\u00f6\u00f6\u00fe\u00fe\22\2\16\23\25\679RTfhtvxz}\177\u0083\u0085\u0095"
          + "\u0097\u009b\u009d\u00c2\u00c4\u00cc\u00ce\u00d2\u00d4\u00f5\u00f7\u00fd"
          + "\u00ff\u0106\2\u0dd5\2\u0116\3\2\2\2\4\u011f\3\2\2\2\6\u0122\3\2\2\2\b"
          + "\u0125\3\2\2\2\n\u0128\3\2\2\2\f\u012b\3\2\2\2\16\u012e\3\2\2\2\20\u0419"
          + "\3\2\2\2\22\u041b\3\2\2\2\24\u04c5\3\2\2\2\26\u04c7\3\2\2\2\30\u04d8\3"
          + "\2\2\2\32\u04de\3\2\2\2\34\u04ea\3\2\2\2\36\u04f7\3\2\2\2 \u04fa\3\2\2"
          + "\2\"\u04fe\3\2\2\2$\u053a\3\2\2\2&\u053c\3\2\2\2(\u0540\3\2\2\2*\u054c"
          + "\3\2\2\2,\u0551\3\2\2\2.\u0558\3\2\2\2\60\u055a\3\2\2\2\62\u0562\3\2\2"
          + "\2\64\u056b\3\2\2\2\66\u0576\3\2\2\28\u0585\3\2\2\2:\u0588\3\2\2\2<\u0593"
          + "\3\2\2\2>\u05a3\3\2\2\2@\u05a9\3\2\2\2B\u05ab\3\2\2\2D\u05b6\3\2\2\2F"
          + "\u05c7\3\2\2\2H\u05ce\3\2\2\2J\u05d0\3\2\2\2L\u05d6\3\2\2\2N\u060c\3\2"
          + "\2\2P\u060e\3\2\2\2R\u0633\3\2\2\2T\u0663\3\2\2\2V\u0666\3\2\2\2X\u068c"
          + "\3\2\2\2Z\u068e\3\2\2\2\\\u0696\3\2\2\2^\u06b7\3\2\2\2`\u06d6\3\2\2\2"
          + "b\u06e2\3\2\2\2d\u0702\3\2\2\2f\u070e\3\2\2\2h\u0711\3\2\2\2j\u071a\3"
          + "\2\2\2l\u072b\3\2\2\2n\u073f\3\2\2\2p\u0741\3\2\2\2r\u0749\3\2\2\2t\u074d"
          + "\3\2\2\2v\u0750\3\2\2\2x\u0753\3\2\2\2z\u076d\3\2\2\2|\u076f\3\2\2\2~"
          + "\u07ad\3\2\2\2\u0080\u07bc\3\2\2\2\u0082\u07be\3\2\2\2\u0084\u07dc\3\2"
          + "\2\2\u0086\u07de\3\2\2\2\u0088\u07e5\3\2\2\2\u008a\u0805\3\2\2\2\u008c"
          + "\u0807\3\2\2\2\u008e\u0819\3\2\2\2\u0090\u0833\3\2\2\2\u0092\u0839\3\2"
          + "\2\2\u0094\u083b\3\2\2\2\u0096\u085a\3\2\2\2\u0098\u085c\3\2\2\2\u009a"
          + "\u0860\3\2\2\2\u009c\u0868\3\2\2\2\u009e\u0873\3\2\2\2\u00a0\u0877\3\2"
          + "\2\2\u00a2\u0882\3\2\2\2\u00a4\u089e\3\2\2\2\u00a6\u08a0\3\2\2\2\u00a8"
          + "\u08ab\3\2\2\2\u00aa\u08c1\3\2\2\2\u00ac\u08f4\3\2\2\2\u00ae\u08f6\3\2"
          + "\2\2\u00b0\u08fe\3\2\2\2\u00b2\u0909\3\2\2\2\u00b4\u0910\3\2\2\2\u00b6"
          + "\u0914\3\2\2\2\u00b8\u091e\3\2\2\2\u00ba\u0926\3\2\2\2\u00bc\u093e\3\2"
          + "\2\2\u00be\u0942\3\2\2\2\u00c0\u0944\3\2\2\2\u00c2\u0952\3\2\2\2\u00c4"
          + "\u09b1\3\2\2\2\u00c6\u09b7\3\2\2\2\u00c8\u0a89\3\2\2\2\u00ca\u0aa4\3\2"
          + "\2\2\u00cc\u0aa6\3\2\2\2\u00ce\u0aa8\3\2\2\2\u00d0\u0aaa\3\2\2\2\u00d2"
          + "\u0aac\3\2\2\2\u00d4\u0aae\3\2\2\2\u00d6\u0ab3\3\2\2\2\u00d8\u0aba\3\2"
          + "\2\2\u00da\u0abe\3\2\2\2\u00dc\u0ac3\3\2\2\2\u00de\u0acd\3\2\2\2\u00e0"
          + "\u0ad2\3\2\2\2\u00e2\u0af6\3\2\2\2\u00e4\u0af8\3\2\2\2\u00e6\u0b00\3\2"
          + "\2\2\u00e8\u0b0c\3\2\2\2\u00ea\u0b14\3\2\2\2\u00ec\u0b1d\3\2\2\2\u00ee"
          + "\u0b25\3\2\2\2\u00f0\u0b2f\3\2\2\2\u00f2\u0b34\3\2\2\2\u00f4\u0b3d\3\2"
          + "\2\2\u00f6\u0b6f\3\2\2\2\u00f8\u0b81\3\2\2\2\u00fa\u0b8a\3\2\2\2\u00fc"
          + "\u0b8c\3\2\2\2\u00fe\u0b98\3\2\2\2\u0100\u0b9a\3\2\2\2\u0102\u0ba2\3\2"
          + "\2\2\u0104\u0bac\3\2\2\2\u0106\u0bb1\3\2\2\2\u0108\u0bb9\3\2\2\2\u010a"
          + "\u0bbb\3\2\2\2\u010c\u0be8\3\2\2\2\u010e\u0bf1\3\2\2\2\u0110\u0bf3\3\2"
          + "\2\2\u0112\u0bf5\3\2\2\2\u0114\u0bf7\3\2\2\2\u0116\u011a\5\20\t\2\u0117"
          + "\u0119\7\3\2\2\u0118\u0117\3\2\2\2\u0119\u011c\3\2\2\2\u011a\u0118\3\2"
          + "\2\2\u011a\u011b\3\2\2\2\u011b\u011d\3\2\2\2\u011c\u011a\3\2\2\2\u011d"
          + "\u011e\7\2\2\3\u011e\3\3\2\2\2\u011f\u0120\5\u00b6\\\2\u0120\u0121\7\2"
          + "\2\3\u0121\5\3\2\2\2\u0122\u0123\5\u00b2Z\2\u0123\u0124\7\2\2\3\u0124"
          + "\7\3\2\2\2\u0125\u0126\5\u00b0Y\2\u0126\u0127\7\2\2\3\u0127\t\3\2\2\2"
          + "\u0128\u0129\5\u00b4[\2\u0129\u012a\7\2\2\3\u012a\13\3\2\2\2\u012b\u012c"
          + "\5\u00e2r\2\u012c\u012d\7\2\2\3\u012d\r\3\2\2\2\u012e\u012f\5\u00e8u\2"
          + "\u012f\u0130\7\2\2\3\u0130\17\3\2\2\2\u0131\u041a\5\"\22\2\u0132\u0134"
          + "\5\62\32\2\u0133\u0132\3\2\2\2\u0133\u0134\3\2\2\2\u0134\u0135\3\2\2\2"
          + "\u0135\u041a\5N(\2\u0136\u0138\7\u00fc\2\2\u0137\u0139\7\u0094\2\2\u0138"
          + "\u0137\3\2\2\2\u0138\u0139\3\2\2\2\u0139\u013a\3\2\2\2\u013a\u041a\5\u00b0"
          + "Y\2\u013b\u013c\7\67\2\2\u013c\u0140\5,\27\2\u013d\u013e\7o\2\2\u013e"
          + "\u013f\7\u0098\2\2\u013f\u0141\7U\2\2\u0140\u013d\3\2\2\2\u0140\u0141"
          + "\3\2\2\2\u0141\u0142\3\2\2\2\u0142\u014a\5\u00b0Y\2\u0143\u0149\5 \21"
          + "\2\u0144\u0149\5\36\20\2\u0145\u0146\7\u0105\2\2\u0146\u0147\t\2\2\2\u0147"
          + "\u0149\5:\36\2\u0148\u0143\3\2\2\2\u0148\u0144\3\2\2\2\u0148\u0145\3\2"
          + "\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014a\u014b\3\2\2\2\u014b"
          + "\u041a\3\2\2\2\u014c\u014a\3\2\2\2\u014d\u014e\7\21\2\2\u014e\u014f\5"
          + ",\27\2\u014f\u0150\5\u00b0Y\2\u0150\u0151\7\u00d2\2\2\u0151\u0152\t\2"
          + "\2\2\u0152\u0153\5:\36\2\u0153\u041a\3\2\2\2\u0154\u0155\7\21\2\2\u0155"
          + "\u0156\5,\27\2\u0156\u0157\5\u00b0Y\2\u0157\u0158\7\u00d2\2\2\u0158\u0159"
          + "\5\36\20\2\u0159\u041a\3\2\2\2\u015a\u015b\7N\2\2\u015b\u015e\5,\27\2"
          + "\u015c\u015d\7o\2\2\u015d\u015f\7U\2\2\u015e\u015c\3\2\2\2\u015e\u015f"
          + "\3\2\2\2\u015f\u0160\3\2\2\2\u0160\u0162\5\u00b0Y\2\u0161\u0163\t\3\2"
          + "\2\u0162\u0161\3\2\2\2\u0162\u0163\3\2\2\2\u0163\u041a\3\2\2\2\u0164\u0165"
          + "\7\u00d5\2\2\u0165\u0168\t\4\2\2\u0166\u0167\t\5\2\2\u0167\u0169\5\u00b0"
          + "Y\2\u0168\u0166\3\2\2\2\u0168\u0169\3\2\2\2\u0169\u016e\3\2\2\2\u016a"
          + "\u016c\7\u0085\2\2\u016b\u016a\3\2\2\2\u016b\u016c\3\2\2\2\u016c\u016d"
          + "\3\2\2\2\u016d\u016f\7\u0119\2\2\u016e\u016b\3\2\2\2\u016e\u016f\3\2\2"
          + "\2\u016f\u041a\3\2\2\2\u0170\u0175\5\26\f\2\u0171\u0172\7\4\2\2\u0172"
          + "\u0173\5\u00e8u\2\u0173\u0174\7\5\2\2\u0174\u0176\3\2\2\2\u0175\u0171"
          + "\3\2\2\2\u0175\u0176\3\2\2\2\u0176\u0177\3\2\2\2\u0177\u0178\5\66\34\2"
          + "\u0178\u017d\58\35\2\u0179\u017b\7\30\2\2\u017a\u0179\3\2\2\2\u017a\u017b"
          + "\3\2\2\2\u017b\u017c\3\2\2\2\u017c\u017e\5\"\22\2\u017d\u017a\3\2\2\2"
          + "\u017d\u017e\3\2\2\2\u017e\u041a\3\2\2\2\u017f\u0184\5\26\f\2\u0180\u0181"
          + "\7\4\2\2\u0181\u0182\5\u00e8u\2\u0182\u0183\7\5\2\2\u0183\u0185\3\2\2"
          + "\2\u0184\u0180\3\2\2\2\u0184\u0185\3\2\2\2\u0185\u019b\3\2\2\2\u0186\u019a"
          + "\5 \21\2\u0187\u0188\7\u00aa\2\2\u0188\u0189\7 \2\2\u0189\u018a\7\4\2"
          + "\2\u018a\u018b\5\u00e8u\2\u018b\u018c\7\5\2\2\u018c\u0191\3\2\2\2\u018d"
          + "\u018e\7\u00aa\2\2\u018e\u018f\7 \2\2\u018f\u0191\5\u0098M\2\u0190\u0187"
          + "\3\2\2\2\u0190\u018d\3\2\2\2\u0191\u019a\3\2\2\2\u0192\u019a\5\32\16\2"
          + "\u0193\u019a\5\34\17\2\u0194\u019a\5\u00acW\2\u0195\u019a\5F$\2\u0196"
          + "\u019a\5\36\20\2\u0197\u0198\7\u00e4\2\2\u0198\u019a\5:\36\2\u0199\u0186"
          + "\3\2\2\2\u0199\u0190\3\2\2\2\u0199\u0192\3\2\2\2\u0199\u0193\3\2\2\2\u0199"
          + "\u0194\3\2\2\2\u0199\u0195\3\2\2\2\u0199\u0196\3\2\2\2\u0199\u0197\3\2"
          + "\2\2\u019a\u019d\3\2\2\2\u019b\u0199\3\2\2\2\u019b\u019c\3\2\2\2\u019c"
          + "\u01a2\3\2\2\2\u019d\u019b\3\2\2\2\u019e\u01a0\7\30\2\2\u019f\u019e\3"
          + "\2\2\2\u019f\u01a0\3\2\2\2\u01a0\u01a1\3\2\2\2\u01a1\u01a3\5\"\22\2\u01a2"
          + "\u019f\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3\u041a\3\2\2\2\u01a4\u01a5\7\67"
          + "\2\2\u01a5\u01a9\7\u00e1\2\2\u01a6\u01a7\7o\2\2\u01a7\u01a8\7\u0098\2"
          + "\2\u01a8\u01aa\7U\2\2\u01a9\u01a6\3\2\2\2\u01a9\u01aa\3\2\2\2\u01aa\u01ab"
          + "\3\2\2\2\u01ab\u01ac\5\u00b2Z\2\u01ac\u01ad\7\u0085\2\2\u01ad\u01b6\5"
          + "\u00b2Z\2\u01ae\u01b5\5\66\34\2\u01af\u01b5\5\u00acW\2\u01b0\u01b5\5F"
          + "$\2\u01b1\u01b5\5\36\20\2\u01b2\u01b3\7\u00e4\2\2\u01b3\u01b5\5:\36\2"
          + "\u01b4\u01ae\3\2\2\2\u01b4\u01af\3\2\2\2\u01b4\u01b0\3\2\2\2\u01b4\u01b1"
          + "\3\2\2\2\u01b4\u01b2\3\2\2\2\u01b5\u01b8\3\2\2\2\u01b6\u01b4\3\2\2\2\u01b6"
          + "\u01b7\3\2\2\2\u01b7\u041a\3\2\2\2\u01b8\u01b6\3\2\2\2\u01b9\u01be\5\30"
          + "\r\2\u01ba\u01bb\7\4\2\2\u01bb\u01bc\5\u00e8u\2\u01bc\u01bd\7\5\2\2\u01bd"
          + "\u01bf\3\2\2\2\u01be\u01ba\3\2\2\2\u01be\u01bf\3\2\2\2\u01bf\u01c0\3\2"
          + "\2\2\u01c0\u01c1\5\66\34\2\u01c1\u01c6\58\35\2\u01c2\u01c4\7\30\2\2\u01c3"
          + "\u01c2\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c7\5\""
          + "\22\2\u01c6\u01c3\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u041a\3\2\2\2\u01c8"
          + "\u01c9\7\22\2\2\u01c9\u01ca\7\u00e1\2\2\u01ca\u01cc\5\u00b0Y\2\u01cb\u01cd"
          + "\5(\25\2\u01cc\u01cb\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\u01ce\3\2\2\2\u01ce"
          + "\u01cf\7\63\2\2\u01cf\u01d7\7\u00db\2\2\u01d0\u01d8\5\u0106\u0084\2\u01d1"
          + "\u01d2\7b\2\2\u01d2\u01d3\7.\2\2\u01d3\u01d8\5\u009aN\2\u01d4\u01d5\7"
          + "b\2\2\u01d5\u01d6\7\20\2\2\u01d6\u01d8\7.\2\2\u01d7\u01d0\3\2\2\2\u01d7"
          + "\u01d1\3\2\2\2\u01d7\u01d4\3\2\2\2\u01d7\u01d8\3\2\2\2\u01d8\u041a\3\2"
          + "\2\2\u01d9\u01da\7\21\2\2\u01da\u01db\7\u00e1\2\2\u01db\u01dc\5\u00b0"
          + "Y\2\u01dc\u01dd\7\16\2\2\u01dd\u01de\t\6\2\2\u01de\u01df\5\u00e4s\2\u01df"
          + "\u041a\3\2\2\2\u01e0\u01e1\7\21\2\2\u01e1\u01e2\7\u00e1\2\2\u01e2\u01e3"
          + "\5\u00b0Y\2\u01e3\u01e4\7\16\2\2\u01e4\u01e5\t\6\2\2\u01e5\u01e6\7\4\2"
          + "\2\u01e6\u01e7\5\u00e4s\2\u01e7\u01e8\7\5\2\2\u01e8\u041a\3\2\2\2\u01e9"
          + "\u01ea\7\21\2\2\u01ea\u01eb\7\u00e1\2\2\u01eb\u01ec\5\u00b0Y\2\u01ec\u01ed"
          + "\7\u00bd\2\2\u01ed\u01ee\7-\2\2\u01ee\u01ef\5\u00b0Y\2\u01ef\u01f0\7\u00e9"
          + "\2\2\u01f0\u01f1\5\u0102\u0082\2\u01f1\u041a\3\2\2\2\u01f2\u01f3\7\21"
          + "\2\2\u01f3\u01f4\7\u00e1\2\2\u01f4\u01f5\5\u00b0Y\2\u01f5\u01f6\7N\2\2"
          + "\u01f6\u01f7\t\6\2\2\u01f7\u01f8\7\4\2\2\u01f8\u01f9\5\u00aeX\2\u01f9"
          + "\u01fa\7\5\2\2\u01fa\u041a\3\2\2\2\u01fb\u01fc\7\21\2\2\u01fc\u01fd\7"
          + "\u00e1\2\2\u01fd\u01fe\5\u00b0Y\2\u01fe\u01ff\7N\2\2\u01ff\u0200\t\6\2"
          + "\2\u0200\u0201\5\u00aeX\2\u0201\u041a\3\2\2\2\u0202\u0203\7\21\2\2\u0203"
          + "\u0204\t\7\2\2\u0204\u0205\5\u00b0Y\2\u0205\u0206\7\u00bd\2\2\u0206\u0207"
          + "\7\u00e9\2\2\u0207\u0208\5\u00b0Y\2\u0208\u041a\3\2\2\2\u0209\u020a\7"
          + "\21\2\2\u020a\u020b\t\7\2\2\u020b\u020c\5\u00b0Y\2\u020c\u020d\7\u00d2"
          + "\2\2\u020d\u020e\7\u00e4\2\2\u020e\u020f\5:\36\2\u020f\u041a\3\2\2\2\u0210"
          + "\u0211\7\21\2\2\u0211\u0212\t\7\2\2\u0212\u0213\5\u00b0Y\2\u0213\u0214"
          + "\7\u00fa\2\2\u0214\u0217\7\u00e4\2\2\u0215\u0216\7o\2\2\u0216\u0218\7"
          + "U\2\2\u0217\u0215\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0219\3\2\2\2\u0219"
          + "\u021a\5:\36\2\u021a\u041a\3\2\2\2\u021b\u021c\7\21\2\2\u021c\u021d\7"
          + "\u00e1\2\2\u021d\u021e\5\u00b0Y\2\u021e\u0220\t\b\2\2\u021f\u0221\7-\2"
          + "\2\u0220\u021f\3\2\2\2\u0220\u0221\3\2\2\2\u0221\u0222\3\2\2\2\u0222\u0224"
          + "\5\u00b0Y\2\u0223\u0225\5\u010e\u0088\2\u0224\u0223\3\2\2\2\u0224\u0225"
          + "\3\2\2\2\u0225\u041a\3\2\2\2\u0226\u0227\7\21\2\2\u0227\u0228\7\u00e1"
          + "\2\2\u0228\u022a\5\u00b0Y\2\u0229\u022b\5(\25\2\u022a\u0229\3\2\2\2\u022a"
          + "\u022b\3\2\2\2\u022b\u022c\3\2\2\2\u022c\u022e\7%\2\2\u022d\u022f\7-\2"
          + "\2\u022e\u022d\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0231"
          + "\5\u00b0Y\2\u0231\u0233\5\u00eav\2\u0232\u0234\5\u00e0q\2\u0233\u0232"
          + "\3\2\2\2\u0233\u0234\3\2\2\2\u0234\u041a\3\2\2\2\u0235\u0236\7\21\2\2"
          + "\u0236\u0237\7\u00e1\2\2\u0237\u0239\5\u00b0Y\2\u0238\u023a\5(\25\2\u0239"
          + "\u0238\3\2\2\2\u0239\u023a\3\2\2\2\u023a\u023b\3\2\2\2\u023b\u023c\7\u00bf"
          + "\2\2\u023c\u023d\7.\2\2\u023d\u023e\7\4\2\2\u023e\u023f\5\u00e4s\2\u023f"
          + "\u0240\7\5\2\2\u0240\u041a\3\2\2\2\u0241\u0242\7\21\2\2\u0242\u0243\7"
          + "\u00e1\2\2\u0243\u0245\5\u00b0Y\2\u0244\u0246\5(\25\2\u0245\u0244\3\2"
          + "\2\2\u0245\u0246\3\2\2\2\u0246\u0247\3\2\2\2\u0247\u0248\7\u00d2\2\2\u0248"
          + "\u0249\7\u00cf\2\2\u0249\u024d\7\u0119\2\2\u024a\u024b\7\u0105\2\2\u024b"
          + "\u024c\7\u00d0\2\2\u024c\u024e\5:\36\2\u024d\u024a\3\2\2\2\u024d\u024e"
          + "\3\2\2\2\u024e\u041a\3\2\2\2\u024f\u0250\7\21\2\2\u0250\u0251\7\u00e1"
          + "\2\2\u0251\u0253\5\u00b0Y\2\u0252\u0254\5(\25\2\u0253\u0252\3\2\2\2\u0253"
          + "\u0254\3\2\2\2\u0254\u0255\3\2\2\2\u0255\u0256\7\u00d2\2\2\u0256\u0257"
          + "\7\u00d0\2\2\u0257\u0258\5:\36\2\u0258\u041a\3\2\2\2\u0259\u025a\7\21"
          + "\2\2\u025a\u025b\t\7\2\2\u025b\u025c\5\u00b0Y\2\u025c\u0260\7\16\2\2\u025d"
          + "\u025e\7o\2\2\u025e\u025f\7\u0098\2\2\u025f\u0261\7U\2\2\u0260\u025d\3"
          + "\2\2\2\u0260\u0261\3\2\2\2\u0261\u0263\3\2\2\2\u0262\u0264\5&\24\2\u0263"
          + "\u0262\3\2\2\2\u0264\u0265\3\2\2\2\u0265\u0263\3\2\2\2\u0265\u0266\3\2"
          + "\2\2\u0266\u041a\3\2\2\2\u0267\u0268\7\21\2\2\u0268\u0269\7\u00e1\2\2"
          + "\u0269\u026a\5\u00b0Y\2\u026a\u026b\5(\25\2\u026b\u026c\7\u00bd\2\2\u026c"
          + "\u026d\7\u00e9\2\2\u026d\u026e\5(\25\2\u026e\u041a\3\2\2\2\u026f\u0270"
          + "\7\21\2\2\u0270\u0271\t\7\2\2\u0271\u0272\5\u00b0Y\2\u0272\u0275\7N\2"
          + "\2\u0273\u0274\7o\2\2\u0274\u0276\7U\2\2\u0275\u0273\3\2\2\2\u0275\u0276"
          + "\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u027c\5(\25\2\u0278\u0279\7\6\2\2\u0279"
          + "\u027b\5(\25\2\u027a\u0278\3\2\2\2\u027b\u027e\3\2\2\2\u027c\u027a\3\2"
          + "\2\2\u027c\u027d\3\2\2\2\u027d\u0280\3\2\2\2\u027e\u027c\3\2\2\2\u027f"
          + "\u0281\7\u00b4\2\2\u0280\u027f\3\2\2\2\u0280\u0281\3\2\2\2\u0281\u041a"
          + "\3\2\2\2\u0282\u0283\7\21\2\2\u0283\u0284\7\u00e1\2\2\u0284\u0286\5\u00b0"
          + "Y\2\u0285\u0287\5(\25\2\u0286\u0285\3\2\2\2\u0286\u0287\3\2\2\2\u0287"
          + "\u0288\3\2\2\2\u0288\u0289\7\u00d2\2\2\u0289\u028a\5\36\20\2\u028a\u041a"
          + "\3\2\2\2\u028b\u028c\7\21\2\2\u028c\u028d\7\u00e1\2\2\u028d\u028e\5\u00b0"
          + "Y\2\u028e\u028f\7\u00b9\2\2\u028f\u0290\7\u00ab\2\2\u0290\u041a\3\2\2"
          + "\2\u0291\u0292\7N\2\2\u0292\u0295\7\u00e1\2\2\u0293\u0294\7o\2\2\u0294"
          + "\u0296\7U\2\2\u0295\u0293\3\2\2\2\u0295\u0296\3\2\2\2\u0296\u0297\3\2"
          + "\2\2\u0297\u0299\5\u00b0Y\2\u0298\u029a\7\u00b4\2\2\u0299\u0298\3\2\2"
          + "\2\u0299\u029a\3\2\2\2\u029a\u041a\3\2\2\2\u029b\u029c\7N\2\2\u029c\u029f"
          + "\7\u0100\2\2\u029d\u029e\7o\2\2\u029e\u02a0\7U\2\2\u029f\u029d\3\2\2\2"
          + "\u029f\u02a0\3\2\2\2\u02a0\u02a1\3\2\2\2\u02a1\u041a\5\u00b0Y\2\u02a2"
          + "\u02a5\7\67\2\2\u02a3\u02a4\7\u00a0\2\2\u02a4\u02a6\7\u00bf\2\2\u02a5"
          + "\u02a3\3\2\2\2\u02a5\u02a6\3\2\2\2\u02a6\u02ab\3\2\2\2\u02a7\u02a9\7j"
          + "\2\2\u02a8\u02a7\3\2\2\2\u02a8\u02a9\3\2\2\2\u02a9\u02aa\3\2\2\2\u02aa"
          + "\u02ac\7\u00e5\2\2\u02ab\u02a8\3\2\2\2\u02ab\u02ac\3\2\2\2\u02ac\u02ad"
          + "\3\2\2\2\u02ad\u02b1\7\u0100\2\2\u02ae\u02af\7o\2\2\u02af\u02b0\7\u0098"
          + "\2\2\u02b0\u02b2\7U\2\2\u02b1\u02ae\3\2\2\2\u02b1\u02b2\3\2\2\2\u02b2"
          + "\u02b3\3\2\2\2\u02b3\u02b5\5\u00b0Y\2\u02b4\u02b6\5\u00a0Q\2\u02b5\u02b4"
          + "\3\2\2\2\u02b5\u02b6\3\2\2\2\u02b6\u02bf\3\2\2\2\u02b7\u02be\5 \21\2\u02b8"
          + "\u02b9\7\u00aa\2\2\u02b9\u02ba\7\u009c\2\2\u02ba\u02be\5\u0098M\2\u02bb"
          + "\u02bc\7\u00e4\2\2\u02bc\u02be\5:\36\2\u02bd\u02b7\3\2\2\2\u02bd\u02b8"
          + "\3\2\2\2\u02bd\u02bb\3\2\2\2\u02be\u02c1\3\2\2\2\u02bf\u02bd\3\2\2\2\u02bf"
          + "\u02c0\3\2\2\2\u02c0\u02c2\3\2\2\2\u02c1\u02bf\3\2\2\2\u02c2\u02c3\7\30"
          + "\2\2\u02c3\u02c4\5\"\22\2\u02c4\u041a\3\2\2\2\u02c5\u02c8\7\67\2\2\u02c6"
          + "\u02c7\7\u00a0\2\2\u02c7\u02c9\7\u00bf\2\2\u02c8\u02c6\3\2\2\2\u02c8\u02c9"
          + "\3\2\2\2\u02c9\u02cb\3\2\2\2\u02ca\u02cc\7j\2\2\u02cb\u02ca\3\2\2\2\u02cb"
          + "\u02cc\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cd\u02ce\7\u00e5\2\2\u02ce\u02cf"
          + "\7\u0100\2\2\u02cf\u02d4\5\u00b2Z\2\u02d0\u02d1\7\4\2\2\u02d1\u02d2\5"
          + "\u00e8u\2\u02d2\u02d3\7\5\2\2\u02d3\u02d5\3\2\2\2\u02d4\u02d0\3\2\2\2"
          + "\u02d4\u02d5\3\2\2\2\u02d5\u02d6\3\2\2\2\u02d6\u02d9\5\66\34\2\u02d7\u02d8"
          + "\7\u009f\2\2\u02d8\u02da\5:\36\2\u02d9\u02d7\3\2\2\2\u02d9\u02da\3\2\2"
          + "\2\u02da\u041a\3\2\2\2\u02db\u02dc\7\21\2\2\u02dc\u02dd\7\u0100\2\2\u02dd"
          + "\u02df\5\u00b0Y\2\u02de\u02e0\7\30\2\2\u02df\u02de\3\2\2\2\u02df\u02e0"
          + "\3\2\2\2\u02e0\u02e1\3\2\2\2\u02e1\u02e2\5\"\22\2\u02e2\u041a\3\2\2\2"
          + "\u02e3\u02e6\7\67\2\2\u02e4\u02e5\7\u00a0\2\2\u02e5\u02e7\7\u00bf\2\2"
          + "\u02e6\u02e4\3\2\2\2\u02e6\u02e7\3\2\2\2\u02e7\u02e9\3\2\2\2\u02e8\u02ea"
          + "\7\u00e5\2\2\u02e9\u02e8\3\2\2\2\u02e9\u02ea\3\2\2\2\u02ea\u02eb\3\2\2"
          + "\2\u02eb\u02ef\7h\2\2\u02ec\u02ed\7o\2\2\u02ed\u02ee\7\u0098\2\2\u02ee"
          + "\u02f0\7U\2\2\u02ef\u02ec\3\2\2\2\u02ef\u02f0\3\2\2\2\u02f0\u02f1\3\2"
          + "\2\2\u02f1\u02f2\5\u00b0Y\2\u02f2\u02f3\7\30\2\2\u02f3\u02fd\7\u0119\2"
          + "\2\u02f4\u02f5\7\u00fe\2\2\u02f5\u02fa\5L\'\2\u02f6\u02f7\7\6\2\2\u02f7"
          + "\u02f9\5L\'\2\u02f8\u02f6\3\2\2\2\u02f9\u02fc\3\2\2\2\u02fa\u02f8\3\2"
          + "\2\2\u02fa\u02fb\3\2\2\2\u02fb\u02fe\3\2\2\2\u02fc\u02fa\3\2\2\2\u02fd"
          + "\u02f4\3\2\2\2\u02fd\u02fe\3\2\2\2\u02fe\u041a\3\2\2\2\u02ff\u0301\7N"
          + "\2\2\u0300\u0302\7\u00e5\2\2\u0301\u0300\3\2\2\2\u0301\u0302\3\2\2\2\u0302"
          + "\u0303\3\2\2\2\u0303\u0306\7h\2\2\u0304\u0305\7o\2\2\u0305\u0307\7U\2"
          + "\2\u0306\u0304\3\2\2\2\u0306\u0307\3\2\2\2\u0307\u0308\3\2\2\2\u0308\u041a"
          + "\5\u00b0Y\2\u0309\u030b\7V\2\2\u030a\u030c\t\t\2\2\u030b\u030a\3\2\2\2"
          + "\u030b\u030c\3\2\2\2\u030c\u030d\3\2\2\2\u030d\u041a\5\20\t\2\u030e\u030f"
          + "\7\u00d5\2\2\u030f\u0312\7\u00e2\2\2\u0310\u0311\t\5\2\2\u0311\u0313\5"
          + "\u00b0Y\2\u0312\u0310\3\2\2\2\u0312\u0313\3\2\2\2\u0313\u0318\3\2\2\2"
          + "\u0314\u0316\7\u0085\2\2\u0315\u0314\3\2\2\2\u0315\u0316\3\2\2\2\u0316"
          + "\u0317\3\2\2\2\u0317\u0319\7\u0119\2\2\u0318\u0315\3\2\2\2\u0318\u0319"
          + "\3\2\2\2\u0319\u041a\3\2\2\2\u031a\u031b\7\u00d5\2\2\u031b\u031c\7\u00e1"
          + "\2\2\u031c\u031f\7X\2\2\u031d\u031e\t\5\2\2\u031e\u0320\5\u00b0Y\2\u031f"
          + "\u031d\3\2\2\2\u031f\u0320\3\2\2\2\u0320\u0321\3\2\2\2\u0321\u0322\7\u0085"
          + "\2\2\u0322\u0324\7\u0119\2\2\u0323\u0325\5(\25\2\u0324\u0323\3\2\2\2\u0324"
          + "\u0325\3\2\2\2\u0325\u041a\3\2\2\2\u0326\u0327\7\u00d5\2\2\u0327\u0328"
          + "\7\u00e4\2\2\u0328\u032d\5\u00b0Y\2\u0329\u032a\7\4\2\2\u032a\u032b\5"
          + "> \2\u032b\u032c\7\5\2\2\u032c\u032e\3\2\2\2\u032d\u0329\3\2\2\2\u032d"
          + "\u032e\3\2\2\2\u032e\u041a\3\2\2\2\u032f\u0330\7\u00d5\2\2\u0330\u0331"
          + "\7.\2\2\u0331\u0332\t\5\2\2\u0332\u0335\5\u00b0Y\2\u0333\u0334\t\5\2\2"
          + "\u0334\u0336\5\u00b0Y\2\u0335\u0333\3\2\2\2\u0335\u0336\3\2\2\2\u0336"
          + "\u041a\3\2\2\2\u0337\u0338\7\u00d5\2\2\u0338\u033b\7\u0101\2\2\u0339\u033a"
          + "\t\5\2\2\u033a\u033c\5\u00b0Y\2\u033b\u0339\3\2\2\2\u033b\u033c\3\2\2"
          + "\2\u033c\u0341\3\2\2\2\u033d\u033f\7\u0085\2\2\u033e\u033d\3\2\2\2\u033e"
          + "\u033f\3\2\2\2\u033f\u0340\3\2\2\2\u0340\u0342\7\u0119\2\2\u0341\u033e"
          + "\3\2\2\2\u0341\u0342\3\2\2\2\u0342\u041a\3\2\2\2\u0343\u0344\7\u00d5\2"
          + "\2\u0344\u0345\7\u00ab\2\2\u0345\u0347\5\u00b0Y\2\u0346\u0348\5(\25\2"
          + "\u0347\u0346\3\2\2\2\u0347\u0348\3\2\2\2\u0348\u041a\3\2\2\2\u0349\u034b"
          + "\7\u00d5\2\2\u034a\u034c\5\u0106\u0084\2\u034b\u034a\3\2\2\2\u034b\u034c"
          + "\3\2\2\2\u034c\u034d\3\2\2\2\u034d\u0355\7i\2\2\u034e\u0350\7\u0085\2"
          + "\2\u034f\u034e\3\2\2\2\u034f\u0350\3\2\2\2\u0350\u0353\3\2\2\2\u0351\u0354"
          + "\5\u00b0Y\2\u0352\u0354\7\u0119\2\2\u0353\u0351\3\2\2\2\u0353\u0352\3"
          + "\2\2\2\u0354\u0356\3\2\2\2\u0355\u034f\3\2\2\2\u0355\u0356\3\2\2\2\u0356"
          + "\u041a\3\2\2\2\u0357\u0358\7\u00d5\2\2\u0358\u0359\7\67\2\2\u0359\u035a"
          + "\7\u00e1\2\2\u035a\u035d\5\u00b0Y\2\u035b\u035c\7\30\2\2\u035c\u035e\7"
          + "\u00cf\2\2\u035d\u035b\3\2\2\2\u035d\u035e\3\2\2\2\u035e\u041a\3\2\2\2"
          + "\u035f\u0360\7\u00d5\2\2\u0360\u0361\7:\2\2\u0361\u041a\7\u0094\2\2\u0362"
          + "\u0363\t\n\2\2\u0363\u0365\7h\2\2\u0364\u0366\7X\2\2\u0365\u0364\3\2\2"
          + "\2\u0365\u0366\3\2\2\2\u0366\u0367\3\2\2\2\u0367\u041a\5.\30\2\u0368\u0369"
          + "\t\n\2\2\u0369\u036b\5,\27\2\u036a\u036c\7X\2\2\u036b\u036a\3\2\2\2\u036b"
          + "\u036c\3\2\2\2\u036c\u036d\3\2\2\2\u036d\u036e\5\u00b0Y\2\u036e\u041a"
          + "\3\2\2\2\u036f\u0371\t\n\2\2\u0370\u0372\7\u00e1\2\2\u0371\u0370\3\2\2"
          + "\2\u0371\u0372\3\2\2\2\u0372\u0374\3\2\2\2\u0373\u0375\t\13\2\2\u0374"
          + "\u0373\3\2\2\2\u0374\u0375\3\2\2\2\u0375\u0376\3\2\2\2\u0376\u0378\5\u00b0"
          + "Y\2\u0377\u0379\5(\25\2\u0378\u0377\3\2\2\2\u0378\u0379\3\2\2\2\u0379"
          + "\u037b\3\2\2\2\u037a\u037c\5\60\31\2\u037b\u037a\3\2\2\2\u037b\u037c\3"
          + "\2\2\2\u037c\u041a\3\2\2\2\u037d\u037f\t\n\2\2\u037e\u0380\7\u00b5\2\2"
          + "\u037f\u037e\3\2\2\2\u037f\u0380\3\2\2\2\u0380\u0381\3\2\2\2\u0381\u041a"
          + "\5\"\22\2\u0382\u0383\7/\2\2\u0383\u0384\7\u009c\2\2\u0384\u0385\5,\27"
          + "\2\u0385\u0386\5\u00b0Y\2\u0386\u0387\7|\2\2\u0387\u0388\t\f\2\2\u0388"
          + "\u041a\3\2\2\2\u0389\u038a\7/\2\2\u038a\u038b\7\u009c\2\2\u038b\u038c"
          + "\7\u00e1\2\2\u038c\u038d\5\u00b0Y\2\u038d\u038e\7|\2\2\u038e\u038f\t\f"
          + "\2\2\u038f\u041a\3\2\2\2\u0390\u0391\7\u00bc\2\2\u0391\u0392\7\u00e1\2"
          + "\2\u0392\u041a\5\u00b0Y\2\u0393\u0394\7\u00bc\2\2\u0394\u0395\7h\2\2\u0395"
          + "\u041a\5\u00b0Y\2\u0396\u039e\7\u00bc\2\2\u0397\u039f\7\u0119\2\2\u0398"
          + "\u039a\13\2\2\2\u0399\u0398\3\2\2\2\u039a\u039d\3\2\2\2\u039b\u039c\3"
          + "\2\2\2\u039b\u0399\3\2\2\2\u039c\u039f\3\2\2\2\u039d\u039b\3\2\2\2\u039e"
          + "\u0397\3\2\2\2\u039e\u039b\3\2\2\2\u039f\u041a\3\2\2\2\u03a0\u03a2\7!"
          + "\2\2\u03a1\u03a3\7\u0082\2\2\u03a2\u03a1\3\2\2\2\u03a2\u03a3\3\2\2\2\u03a3"
          + "\u03a4\3\2\2\2\u03a4\u03a5\7\u00e1\2\2\u03a5\u03a8\5\u00b0Y\2\u03a6\u03a7"
          + "\7\u009f\2\2\u03a7\u03a9\5:\36\2\u03a8\u03a6\3\2\2\2\u03a8\u03a9\3\2\2"
          + "\2\u03a9\u03ae\3\2\2\2\u03aa\u03ac\7\30\2\2\u03ab\u03aa\3\2\2\2\u03ab"
          + "\u03ac\3\2\2\2\u03ac\u03ad\3\2\2\2\u03ad\u03af\5\"\22\2\u03ae\u03ab\3"
          + "\2\2\2\u03ae\u03af\3\2\2\2\u03af\u041a\3\2\2\2\u03b0\u03b1\7\u00f5\2\2"
          + "\u03b1\u03b4\7\u00e1\2\2\u03b2\u03b3\7o\2\2\u03b3\u03b5\7U\2\2\u03b4\u03b2"
          + "\3\2\2\2\u03b4\u03b5\3\2\2\2\u03b5\u03b6\3\2\2\2\u03b6\u041a\5\u00b0Y"
          + "\2\u03b7\u03b8\7\'\2\2\u03b8\u041a\7!\2\2\u03b9\u03ba\7\u0089\2\2\u03ba"
          + "\u03bc\7?\2\2\u03bb\u03bd\7\u008a\2\2\u03bc\u03bb\3\2\2\2\u03bc\u03bd"
          + "\3\2\2\2\u03bd\u03be\3\2\2\2\u03be\u03bf\7v\2\2\u03bf\u03c1\7\u0119\2"
          + "\2\u03c0\u03c2\7\u00a8\2\2\u03c1\u03c0\3\2\2\2\u03c1\u03c2\3\2\2\2\u03c2"
          + "\u03c3\3\2\2\2\u03c3\u03c4\7{\2\2\u03c4\u03c5\7\u00e1\2\2\u03c5\u03c7"
          + "\5\u00b0Y\2\u03c6\u03c8\5(\25\2\u03c7\u03c6\3\2\2\2\u03c7\u03c8\3\2\2"
          + "\2\u03c8\u041a\3\2\2\2\u03c9\u03ca\7\u00f1\2\2\u03ca\u03cb\7\u00e1\2\2"
          + "\u03cb\u03cd\5\u00b0Y\2\u03cc\u03ce\5(\25\2\u03cd\u03cc\3\2\2\2\u03cd"
          + "\u03ce\3\2\2\2\u03ce\u041a\3\2\2\2\u03cf\u03d0\7\u0093\2\2\u03d0\u03d1"
          + "\7\u00be\2\2\u03d1\u03d2\7\u00e1\2\2\u03d2\u041a\5\u00b0Y\2\u03d3\u03d4"
          + "\t\r\2\2\u03d4\u03dc\5\u0106\u0084\2\u03d5\u03dd\7\u0119\2\2\u03d6\u03d8"
          + "\13\2\2\2\u03d7\u03d6\3\2\2\2\u03d8\u03db\3\2\2\2\u03d9\u03da\3\2\2\2"
          + "\u03d9\u03d7\3\2\2\2\u03da\u03dd\3\2\2\2\u03db\u03d9\3\2\2\2\u03dc\u03d5"
          + "\3\2\2\2\u03dc\u03d9\3\2\2\2\u03dd\u041a\3\2\2\2\u03de\u03df\7\u00d2\2"
          + "\2\u03df\u03e3\7\u00c5\2\2\u03e0\u03e2\13\2\2\2\u03e1\u03e0\3\2\2\2\u03e2"
          + "\u03e5\3\2\2\2\u03e3\u03e4\3\2\2\2\u03e3\u03e1\3\2\2\2\u03e4\u041a\3\2"
          + "\2\2\u03e5\u03e3\3\2\2\2\u03e6\u03e7\7\u00d2\2\2\u03e7\u03e8\7\u00e8\2"
          + "\2\u03e8\u03e9\7\u0106\2\2\u03e9\u041a\5\u00d4k\2\u03ea\u03eb\7\u00d2"
          + "\2\2\u03eb\u03ec\7\u00e8\2\2\u03ec\u03ed\7\u0106\2\2\u03ed\u041a\t\16"
          + "\2\2\u03ee\u03ef\7\u00d2\2\2\u03ef\u03f0\7\u00e8\2\2\u03f0\u03f4\7\u0106"
          + "\2\2\u03f1\u03f3\13\2\2\2\u03f2\u03f1\3\2\2\2\u03f3\u03f6\3\2\2\2\u03f4"
          + "\u03f5\3\2\2\2\u03f4\u03f2\3\2\2\2\u03f5\u041a\3\2\2\2\u03f6\u03f4\3\2"
          + "\2\2\u03f7\u03f8\7\u00d2\2\2\u03f8\u0400\5\22\n\2\u03f9\u03fd\7\u0107"
          + "\2\2\u03fa\u03fc\13\2\2\2\u03fb\u03fa\3\2\2\2\u03fc\u03ff\3\2\2\2\u03fd"
          + "\u03fe\3\2\2\2\u03fd\u03fb\3\2\2\2\u03fe\u0401\3\2\2\2\u03ff\u03fd\3\2"
          + "\2\2\u0400\u03f9\3\2\2\2\u0400\u0401\3\2\2\2\u0401\u041a\3\2\2\2\u0402"
          + "\u0406\7\u00d2\2\2\u0403\u0405\13\2\2\2\u0404\u0403\3\2\2\2\u0405\u0408"
          + "\3\2\2\2\u0406\u0407\3\2\2\2\u0406\u0404\3\2\2\2\u0407\u041a\3\2\2\2\u0408"
          + "\u0406\3\2\2\2\u0409\u040a\7\u00c0\2\2\u040a\u041a\5\22\n\2\u040b\u040f"
          + "\7\u00c0\2\2\u040c\u040e\13\2\2\2\u040d\u040c\3\2\2\2\u040e\u0411\3\2"
          + "\2\2\u040f\u0410\3\2\2\2\u040f\u040d\3\2\2\2\u0410\u041a\3\2\2\2\u0411"
          + "\u040f\3\2\2\2\u0412\u0416\5\24\13\2\u0413\u0415\13\2\2\2\u0414\u0413"
          + "\3\2\2\2\u0415\u0418\3\2\2\2\u0416\u0417\3\2\2\2\u0416\u0414\3\2\2\2\u0417"
          + "\u041a\3\2\2\2\u0418\u0416\3\2\2\2\u0419\u0131\3\2\2\2\u0419\u0133\3\2"
          + "\2\2\u0419\u0136\3\2\2\2\u0419\u013b\3\2\2\2\u0419\u014d\3\2\2\2\u0419"
          + "\u0154\3\2\2\2\u0419\u015a\3\2\2\2\u0419\u0164\3\2\2\2\u0419\u0170\3\2"
          + "\2\2\u0419\u017f\3\2\2\2\u0419\u01a4\3\2\2\2\u0419\u01b9\3\2\2\2\u0419"
          + "\u01c8\3\2\2\2\u0419\u01d9\3\2\2\2\u0419\u01e0\3\2\2\2\u0419\u01e9\3\2"
          + "\2\2\u0419\u01f2\3\2\2\2\u0419\u01fb\3\2\2\2\u0419\u0202\3\2\2\2\u0419"
          + "\u0209\3\2\2\2\u0419\u0210\3\2\2\2\u0419\u021b\3\2\2\2\u0419\u0226\3\2"
          + "\2\2\u0419\u0235\3\2\2\2\u0419\u0241\3\2\2\2\u0419\u024f\3\2\2\2\u0419"
          + "\u0259\3\2\2\2\u0419\u0267\3\2\2\2\u0419\u026f\3\2\2\2\u0419\u0282\3\2"
          + "\2\2\u0419\u028b\3\2\2\2\u0419\u0291\3\2\2\2\u0419\u029b\3\2\2\2\u0419"
          + "\u02a2\3\2\2\2\u0419\u02c5\3\2\2\2\u0419\u02db\3\2\2\2\u0419\u02e3\3\2"
          + "\2\2\u0419\u02ff\3\2\2\2\u0419\u0309\3\2\2\2\u0419\u030e\3\2\2\2\u0419"
          + "\u031a\3\2\2\2\u0419\u0326\3\2\2\2\u0419\u032f\3\2\2\2\u0419\u0337\3\2"
          + "\2\2\u0419\u0343\3\2\2\2\u0419\u0349\3\2\2\2\u0419\u0357\3\2\2\2\u0419"
          + "\u035f\3\2\2\2\u0419\u0362\3\2\2\2\u0419\u0368\3\2\2\2\u0419\u036f\3\2"
          + "\2\2\u0419\u037d\3\2\2\2\u0419\u0382\3\2\2\2\u0419\u0389\3\2\2\2\u0419"
          + "\u0390\3\2\2\2\u0419\u0393\3\2\2\2\u0419\u0396\3\2\2\2\u0419\u03a0\3\2"
          + "\2\2\u0419\u03b0\3\2\2\2\u0419\u03b7\3\2\2\2\u0419\u03b9\3\2\2\2\u0419"
          + "\u03c9\3\2\2\2\u0419\u03cf\3\2\2\2\u0419\u03d3\3\2\2\2\u0419\u03de\3\2"
          + "\2\2\u0419\u03e6\3\2\2\2\u0419\u03ea\3\2\2\2\u0419\u03ee\3\2\2\2\u0419"
          + "\u03f7\3\2\2\2\u0419\u0402\3\2\2\2\u0419\u0409\3\2\2\2\u0419\u040b\3\2"
          + "\2\2\u0419\u0412\3\2\2\2\u041a\21\3\2\2\2\u041b\u041c\5\u010a\u0086\2"
          + "\u041c\23\3\2\2\2\u041d\u041e\7\67\2\2\u041e\u04c6\7\u00c5\2\2\u041f\u0420"
          + "\7N\2\2\u0420\u04c6\7\u00c5\2\2\u0421\u0423\7k\2\2\u0422\u0424\7\u00c5"
          + "\2\2\u0423\u0422\3\2\2\2\u0423\u0424\3\2\2\2\u0424\u04c6\3\2\2\2\u0425"
          + "\u0427\7\u00c2\2\2\u0426\u0428\7\u00c5\2\2\u0427\u0426\3\2\2\2\u0427\u0428"
          + "\3\2\2\2\u0428\u04c6\3\2\2\2\u0429\u042a\7\u00d5\2\2\u042a\u04c6\7k\2"
          + "\2\u042b\u042c\7\u00d5\2\2\u042c\u042e\7\u00c5\2\2\u042d\u042f\7k\2\2"
          + "\u042e\u042d\3\2\2\2\u042e\u042f\3\2\2\2\u042f\u04c6\3\2\2\2\u0430\u0431"
          + "\7\u00d5\2\2\u0431\u04c6\7\u00b2\2\2\u0432\u0433\7\u00d5\2\2\u0433\u04c6"
          + "\7\u00c6\2\2\u0434\u0435\7\u00d5\2\2\u0435\u0436\7:\2\2\u0436\u04c6\7"
          + "\u00c6\2\2\u0437\u0438\7W\2\2\u0438\u04c6\7\u00e1\2\2\u0439\u043a\7q\2"
          + "\2\u043a\u04c6\7\u00e1\2\2\u043b\u043c\7\u00d5\2\2\u043c\u04c6\7\62\2"
          + "\2\u043d\u043e\7\u00d5\2\2\u043e\u043f\7\67\2\2\u043f\u04c6\7\u00e1\2"
          + "\2\u0440\u0441\7\u00d5\2\2\u0441\u04c6\7\u00ed\2\2\u0442\u0443\7\u00d5"
          + "\2\2\u0443\u04c6\7t\2\2\u0444\u0445\7\u00d5\2\2\u0445\u04c6\7\u008d\2"
          + "\2\u0446\u0447\7\67\2\2\u0447\u04c6\7s\2\2\u0448\u0449\7N\2\2\u0449\u04c6"
          + "\7s\2\2\u044a\u044b\7\21\2\2\u044b\u04c6\7s\2\2\u044c\u044d\7\u008c\2"
          + "\2\u044d\u04c6\7\u00e1\2\2\u044e\u044f\7\u008c\2\2\u044f\u04c6\7@\2\2"
          + "\u0450\u0451\7\u00f9\2\2\u0451\u04c6\7\u00e1\2\2\u0452\u0453\7\u00f9\2"
          + "\2\u0453\u04c6\7@\2\2\u0454\u0455\7\67\2\2\u0455\u0456\7\u00e5\2\2\u0456"
          + "\u04c6\7\u008f\2\2\u0457\u0458\7N\2\2\u0458\u0459\7\u00e5\2\2\u0459\u04c6"
          + "\7\u008f\2\2\u045a\u045b\7\21\2\2\u045b\u045c\7\u00e1\2\2\u045c\u045d"
          + "\5\u00b2Z\2\u045d\u045e\7\u0098\2\2\u045e\u045f\7)\2\2\u045f\u04c6\3\2"
          + "\2\2\u0460\u0461\7\21\2\2\u0461\u0462\7\u00e1\2\2\u0462\u0463\5\u00b2"
          + "Z\2\u0463\u0464\7)\2\2\u0464\u0465\7 \2\2\u0465\u04c6\3\2\2\2\u0466\u0467"
          + "\7\21\2\2\u0467\u0468\7\u00e1\2\2\u0468\u0469\5\u00b2Z\2\u0469\u046a\7"
          + "\u0098\2\2\u046a\u046b\7\u00d9\2\2\u046b\u04c6\3\2\2\2\u046c\u046d\7\21"
          + "\2\2\u046d\u046e\7\u00e1\2\2\u046e\u046f\5\u00b2Z\2\u046f\u0470\7\u00d6"
          + "\2\2\u0470\u0471\7 \2\2\u0471\u04c6\3\2\2\2\u0472\u0473\7\21\2\2\u0473"
          + "\u0474\7\u00e1\2\2\u0474\u0475\5\u00b2Z\2\u0475\u0476\7\u0098\2\2\u0476"
          + "\u0477\7\u00d6\2\2\u0477\u04c6\3\2\2\2\u0478\u0479\7\21\2\2\u0479\u047a"
          + "\7\u00e1\2\2\u047a\u047b\5\u00b2Z\2\u047b\u047c\7\u0098\2\2\u047c\u047d"
          + "\7\u00dc\2\2\u047d\u047e\7\30\2\2\u047e\u047f\7I\2\2\u047f\u04c6\3\2\2"
          + "\2\u0480\u0481\7\21\2\2\u0481\u0482\7\u00e1\2\2\u0482\u0483\5\u00b2Z\2"
          + "\u0483\u0484\7\u00d2\2\2\u0484\u0485\7\u00d6\2\2\u0485\u0486\7\u008b\2"
          + "\2\u0486\u04c6\3\2\2\2\u0487\u0488\7\21\2\2\u0488\u0489\7\u00e1\2\2\u0489"
          + "\u048a\5\u00b2Z\2\u048a\u048b\7T\2\2\u048b\u048c\7\u00a9\2\2\u048c\u04c6"
          + "\3\2\2\2\u048d\u048e\7\21\2\2\u048e\u048f\7\u00e1\2\2\u048f\u0490\5\u00b2"
          + "Z\2\u0490\u0491\7\26\2\2\u0491\u0492\7\u00a9\2\2\u0492\u04c6\3\2\2\2\u0493"
          + "\u0494\7\21\2\2\u0494\u0495\7\u00e1\2\2\u0495\u0496\5\u00b2Z\2\u0496\u0497"
          + "\7\u00f3\2\2\u0497\u0498\7\u00a9\2\2\u0498\u04c6\3\2\2\2\u0499\u049a\7"
          + "\21\2\2\u049a\u049b\7\u00e1\2\2\u049b\u049c\5\u00b2Z\2\u049c\u049d\7\u00ea"
          + "\2\2\u049d\u04c6\3\2\2\2\u049e\u049f\7\21\2\2\u049f\u04a0\7\u00e1\2\2"
          + "\u04a0\u04a2\5\u00b2Z\2\u04a1\u04a3\5(\25\2\u04a2\u04a1\3\2\2\2\u04a2"
          + "\u04a3\3\2\2\2\u04a3\u04a4\3\2\2\2\u04a4\u04a5\7\61\2\2\u04a5\u04c6\3"
          + "\2\2\2\u04a6\u04a7\7\21\2\2\u04a7\u04a8\7\u00e1\2\2\u04a8\u04aa\5\u00b2"
          + "Z\2\u04a9\u04ab\5(\25\2\u04aa\u04a9\3\2\2\2\u04aa\u04ab\3\2\2\2\u04ab"
          + "\u04ac\3\2\2\2\u04ac\u04ad\7\64\2\2\u04ad\u04c6\3\2\2\2\u04ae\u04af\7"
          + "\21\2\2\u04af\u04b0\7\u00e1\2\2\u04b0\u04b2\5\u00b2Z\2\u04b1\u04b3\5("
          + "\25\2\u04b2\u04b1\3\2\2\2\u04b2\u04b3\3\2\2\2\u04b3\u04b4\3\2\2\2\u04b4"
          + "\u04b5\7\u00d2\2\2\u04b5\u04b6\7_\2\2\u04b6\u04c6\3\2\2\2\u04b7\u04b8"
          + "\7\21\2\2\u04b8\u04b9\7\u00e1\2\2\u04b9\u04bb\5\u00b2Z\2\u04ba\u04bc\5"
          + "(\25\2\u04bb\u04ba\3\2\2\2\u04bb\u04bc\3\2\2\2\u04bc\u04bd\3\2\2\2\u04bd"
          + "\u04be\7\u00bf\2\2\u04be\u04bf\7.\2\2\u04bf\u04c6\3\2\2\2\u04c0\u04c1"
          + "\7\u00da\2\2\u04c1\u04c6\7\u00ec\2\2\u04c2\u04c6\7\60\2\2\u04c3\u04c6"
          + "\7\u00c7\2\2\u04c4\u04c6\7H\2\2\u04c5\u041d\3\2\2\2\u04c5\u041f\3\2\2"
          + "\2\u04c5\u0421\3\2\2\2\u04c5\u0425\3\2\2\2\u04c5\u0429\3\2\2\2\u04c5\u042b"
          + "\3\2\2\2\u04c5\u0430\3\2\2\2\u04c5\u0432\3\2\2\2\u04c5\u0434\3\2\2\2\u04c5"
          + "\u0437\3\2\2\2\u04c5\u0439\3\2\2\2\u04c5\u043b\3\2\2\2\u04c5\u043d\3\2"
          + "\2\2\u04c5\u0440\3\2\2\2\u04c5\u0442\3\2\2\2\u04c5\u0444\3\2\2\2\u04c5"
          + "\u0446\3\2\2\2\u04c5\u0448\3\2\2\2\u04c5\u044a\3\2\2\2\u04c5\u044c\3\2"
          + "\2\2\u04c5\u044e\3\2\2\2\u04c5\u0450\3\2\2\2\u04c5\u0452\3\2\2\2\u04c5"
          + "\u0454\3\2\2\2\u04c5\u0457\3\2\2\2\u04c5\u045a\3\2\2\2\u04c5\u0460\3\2"
          + "\2\2\u04c5\u0466\3\2\2\2\u04c5\u046c\3\2\2\2\u04c5\u0472\3\2\2\2\u04c5"
          + "\u0478\3\2\2\2\u04c5\u0480\3\2\2\2\u04c5\u0487\3\2\2\2\u04c5\u048d\3\2"
          + "\2\2\u04c5\u0493\3\2\2\2\u04c5\u0499\3\2\2\2\u04c5\u049e\3\2\2\2\u04c5"
          + "\u04a6\3\2\2\2\u04c5\u04ae\3\2\2\2\u04c5\u04b7\3\2\2\2\u04c5\u04c0\3\2"
          + "\2\2\u04c5\u04c2\3\2\2\2\u04c5\u04c3\3\2\2\2\u04c5\u04c4\3\2\2\2\u04c6"
          + "\25\3\2\2\2\u04c7\u04c9\7\67\2\2\u04c8\u04ca\7\u00e5\2\2\u04c9\u04c8\3"
          + "\2\2\2\u04c9\u04ca\3\2\2\2\u04ca\u04cc\3\2\2\2\u04cb\u04cd\7Y\2\2\u04cc"
          + "\u04cb\3\2\2\2\u04cc\u04cd\3\2\2\2\u04cd\u04ce\3\2\2\2\u04ce\u04d2\7\u00e1"
          + "\2\2\u04cf\u04d0\7o\2\2\u04d0\u04d1\7\u0098\2\2\u04d1\u04d3\7U\2\2\u04d2"
          + "\u04cf\3\2\2\2\u04d2\u04d3\3\2\2\2\u04d3\u04d4\3\2\2\2\u04d4\u04d5\5\u00b0"
          + "Y\2\u04d5\27\3\2\2\2\u04d6\u04d7\7\67\2\2\u04d7\u04d9\7\u00a0\2\2\u04d8"
          + "\u04d6\3\2\2\2\u04d8\u04d9\3\2\2\2\u04d9\u04da\3\2\2\2\u04da\u04db\7\u00bf"
          + "\2\2\u04db\u04dc\7\u00e1\2\2\u04dc\u04dd\5\u00b0Y\2\u04dd\31\3\2\2\2\u04de"
          + "\u04df\7)\2\2\u04df\u04e0\7 \2\2\u04e0\u04e4\5\u0098M\2\u04e1\u04e2\7"
          + "\u00d9\2\2\u04e2\u04e3\7 \2\2\u04e3\u04e5\5\u009cO\2\u04e4\u04e1\3\2\2"
          + "\2\u04e4\u04e5\3\2\2\2\u04e5\u04e6\3\2\2\2\u04e6\u04e7\7{\2\2\u04e7\u04e8"
          + "\7\u011d\2\2\u04e8\u04e9\7\37\2\2\u04e9\33\3\2\2\2\u04ea\u04eb\7\u00d6"
          + "\2\2\u04eb\u04ec\7 \2\2\u04ec\u04ed\5\u0098M\2\u04ed\u04f0\7\u009c\2\2"
          + "\u04ee\u04f1\5B\"\2\u04ef\u04f1\5D#\2\u04f0\u04ee\3\2\2\2\u04f0\u04ef"
          + "\3\2\2\2\u04f1\u04f5\3\2\2\2\u04f2\u04f3\7\u00dc\2\2\u04f3\u04f4\7\30"
          + "\2\2\u04f4\u04f6\7I\2\2\u04f5\u04f2\3\2\2\2\u04f5\u04f6\3\2\2\2\u04f6"
          + "\35\3\2\2\2\u04f7\u04f8\7\u008b\2\2\u04f8\u04f9\7\u0119\2\2\u04f9\37\3"
          + "\2\2\2\u04fa\u04fb\7/\2\2\u04fb\u04fc\7\u0119\2\2\u04fc!\3\2\2\2\u04fd"
          + "\u04ff\5\62\32\2\u04fe\u04fd\3\2\2\2\u04fe\u04ff\3\2\2\2\u04ff\u0500\3"
          + "\2\2\2\u0500\u0501\5V,\2\u0501\u0502\5R*\2\u0502#\3\2\2\2\u0503\u0504"
          + "\7x\2\2\u0504\u0506\7\u00a8\2\2\u0505\u0507\7\u00e1\2\2\u0506\u0505\3"
          + "\2\2\2\u0506\u0507\3\2\2\2\u0507\u0508\3\2\2\2\u0508\u050f\5\u00b0Y\2"
          + "\u0509\u050d\5(\25\2\u050a\u050b\7o\2\2\u050b\u050c\7\u0098\2\2\u050c"
          + "\u050e\7U\2\2\u050d\u050a\3\2\2\2\u050d\u050e\3\2\2\2\u050e\u0510\3\2"
          + "\2\2\u050f\u0509\3\2\2\2\u050f\u0510\3\2\2\2\u0510\u053b\3\2\2\2\u0511"
          + "\u0512\7x\2\2\u0512\u0514\7{\2\2\u0513\u0515\7\u00e1\2\2\u0514\u0513\3"
          + "\2\2\2\u0514\u0515\3\2\2\2\u0515\u0516\3\2\2\2\u0516\u0518\5\u00b0Y\2"
          + "\u0517\u0519\5(\25\2\u0518\u0517\3\2\2\2\u0518\u0519\3\2\2\2\u0519\u051d"
          + "\3\2\2\2\u051a\u051b\7o\2\2\u051b\u051c\7\u0098\2\2\u051c\u051e\7U\2\2"
          + "\u051d\u051a\3\2\2\2\u051d\u051e\3\2\2\2\u051e\u053b\3\2\2\2\u051f\u0520"
          + "\7x\2\2\u0520\u0522\7\u00a8\2\2\u0521\u0523\7\u008a\2\2\u0522\u0521\3"
          + "\2\2\2\u0522\u0523\3\2\2\2\u0523\u0524\3\2\2\2\u0524\u0525\7J\2\2\u0525"
          + "\u0527\7\u0119\2\2\u0526\u0528\5\u00acW\2\u0527\u0526\3\2\2\2\u0527\u0528"
          + "\3\2\2\2\u0528\u052a\3\2\2\2\u0529\u052b\5F$\2\u052a\u0529\3\2\2\2\u052a"
          + "\u052b\3\2\2\2\u052b\u053b\3\2\2\2\u052c\u052d\7x\2\2\u052d\u052f\7\u00a8"
          + "\2\2\u052e\u0530\7\u008a\2\2\u052f\u052e\3\2\2\2\u052f\u0530\3\2\2\2\u0530"
          + "\u0531\3\2\2\2\u0531\u0533\7J\2\2\u0532\u0534\7\u0119\2\2\u0533\u0532"
          + "\3\2\2\2\u0533\u0534\3\2\2\2\u0534\u0535\3\2\2\2\u0535\u0538\5\66\34\2"
          + "\u0536\u0537\7\u009f\2\2\u0537\u0539\5:\36\2\u0538\u0536\3\2\2\2\u0538"
          + "\u0539\3\2\2\2\u0539\u053b\3\2\2\2\u053a\u0503\3\2\2\2\u053a\u0511\3\2"
          + "\2\2\u053a\u051f\3\2\2\2\u053a\u052c\3\2\2\2\u053b%\3\2\2\2\u053c\u053e"
          + "\5(\25\2\u053d\u053f\5\36\20\2\u053e\u053d\3\2\2\2\u053e\u053f\3\2\2\2"
          + "\u053f\'\3\2\2\2\u0540\u0541\7\u00a9\2\2\u0541\u0542\7\4\2\2\u0542\u0547"
          + "\5*\26\2\u0543\u0544\7\6\2\2\u0544\u0546\5*\26\2\u0545\u0543\3\2\2\2\u0546"
          + "\u0549\3\2\2\2\u0547\u0545\3\2\2\2\u0547\u0548\3\2\2\2\u0548\u054a\3\2"
          + "\2\2\u0549\u0547\3\2\2\2\u054a\u054b\7\5\2\2\u054b)\3\2\2\2\u054c\u054f"
          + "\5\u0106\u0084\2\u054d\u054e\7\u0107\2\2\u054e\u0550\5\u00caf\2\u054f"
          + "\u054d\3\2\2\2\u054f\u0550\3\2\2\2\u0550+\3\2\2\2\u0551\u0552\t\17\2\2"
          + "\u0552-\3\2\2\2\u0553\u0559\5\u0100\u0081\2\u0554\u0559\7\u0119\2\2\u0555"
          + "\u0559\5\u00ccg\2\u0556\u0559\5\u00ceh\2\u0557\u0559\5\u00d0i\2\u0558"
          + "\u0553\3\2\2\2\u0558\u0554\3\2\2\2\u0558\u0555\3\2\2\2\u0558\u0556\3\2"
          + "\2\2\u0558\u0557\3\2\2\2\u0559/\3\2\2\2\u055a\u055f\5\u0106\u0084\2\u055b"
          + "\u055c\7\7\2\2\u055c\u055e\5\u0106\u0084\2\u055d\u055b\3\2\2\2\u055e\u0561"
          + "\3\2\2\2\u055f\u055d\3\2\2\2\u055f\u0560\3\2\2\2\u0560\61\3\2\2\2\u0561"
          + "\u055f\3\2\2\2\u0562\u0563\7\u0105\2\2\u0563\u0568\5\64\33\2\u0564\u0565"
          + "\7\6\2\2\u0565\u0567\5\64\33\2\u0566\u0564\3\2\2\2\u0567\u056a\3\2\2\2"
          + "\u0568\u0566\3\2\2\2\u0568\u0569\3\2\2\2\u0569\63\3\2\2\2\u056a\u0568"
          + "\3\2\2\2\u056b\u056d\5\u0102\u0082\2\u056c\u056e\5\u0098M\2\u056d\u056c"
          + "\3\2\2\2\u056d\u056e\3\2\2\2\u056e\u0570\3\2\2\2\u056f\u0571\7\30\2\2"
          + "\u0570\u056f\3\2\2\2\u0570\u0571\3\2\2\2\u0571\u0572\3\2\2\2\u0572\u0573"
          + "\7\4\2\2\u0573\u0574\5\"\22\2\u0574\u0575\7\5\2\2\u0575\65\3\2\2\2\u0576"
          + "\u0577\7\u00fe\2\2\u0577\u0578\5\u00b0Y\2\u0578\67\3\2\2\2\u0579\u057a"
          + "\7\u009f\2\2\u057a\u0584\5:\36\2\u057b\u057c\7\u00aa\2\2\u057c\u057d\7"
          + " \2\2\u057d\u0584\5\u00ba^\2\u057e\u0584\5\32\16\2\u057f\u0584\5\36\20"
          + "\2\u0580\u0584\5 \21\2\u0581\u0582\7\u00e4\2\2\u0582\u0584\5:\36\2\u0583"
          + "\u0579\3\2\2\2\u0583\u057b\3\2\2\2\u0583\u057e\3\2\2\2\u0583\u057f\3\2"
          + "\2\2\u0583\u0580\3\2\2\2\u0583\u0581\3\2\2\2\u0584\u0587\3\2\2\2\u0585"
          + "\u0583\3\2\2\2\u0585\u0586\3\2\2\2\u05869\3\2\2\2\u0587\u0585\3\2\2\2"
          + "\u0588\u0589\7\4\2\2\u0589\u058e\5<\37\2\u058a\u058b\7\6\2\2\u058b\u058d"
          + "\5<\37\2\u058c\u058a\3\2\2\2\u058d\u0590\3\2\2\2\u058e\u058c\3\2\2\2\u058e"
          + "\u058f\3\2\2\2\u058f\u0591\3\2\2\2\u0590\u058e\3\2\2\2\u0591\u0592\7\5"
          + "\2\2\u0592;\3\2\2\2\u0593\u0598\5> \2\u0594\u0596\7\u0107\2\2\u0595\u0594"
          + "\3\2\2\2\u0595\u0596\3\2\2\2\u0596\u0597\3\2\2\2\u0597\u0599\5@!\2\u0598"
          + "\u0595\3\2\2\2\u0598\u0599\3\2\2\2\u0599=\3\2\2\2\u059a\u059f\5\u0106"
          + "\u0084\2\u059b\u059c\7\7\2\2\u059c\u059e\5\u0106\u0084\2\u059d\u059b\3"
          + "\2\2\2\u059e\u05a1\3\2\2\2\u059f\u059d\3\2\2\2\u059f\u05a0\3\2\2\2\u05a0"
          + "\u05a4\3\2\2\2\u05a1\u059f\3\2\2\2\u05a2\u05a4\7\u0119\2\2\u05a3\u059a"
          + "\3\2\2\2\u05a3\u05a2\3\2\2\2\u05a4?\3\2\2\2\u05a5\u05aa\7\u011d\2\2\u05a6"
          + "\u05aa\7\u011f\2\2\u05a7\u05aa\5\u00d2j\2\u05a8\u05aa\7\u0119\2\2\u05a9"
          + "\u05a5\3\2\2\2\u05a9\u05a6\3\2\2\2\u05a9\u05a7\3\2\2\2\u05a9\u05a8\3\2"
          + "\2\2\u05aaA\3\2\2\2\u05ab\u05ac\7\4\2\2\u05ac\u05b1\5\u00caf\2\u05ad\u05ae"
          + "\7\6\2\2\u05ae\u05b0\5\u00caf\2\u05af\u05ad\3\2\2\2\u05b0\u05b3\3\2\2"
          + "\2\u05b1\u05af\3\2\2\2\u05b1\u05b2\3\2\2\2\u05b2\u05b4\3\2\2\2\u05b3\u05b1"
          + "\3\2\2\2\u05b4\u05b5\7\5\2\2\u05b5C\3\2\2\2\u05b6\u05b7\7\4\2\2\u05b7"
          + "\u05bc\5B\"\2\u05b8\u05b9\7\6\2\2\u05b9\u05bb\5B\"\2\u05ba\u05b8\3\2\2"
          + "\2\u05bb\u05be\3\2\2\2\u05bc\u05ba\3\2\2\2\u05bc\u05bd\3\2\2\2\u05bd\u05bf"
          + "\3\2\2\2\u05be\u05bc\3\2\2\2\u05bf\u05c0\7\5\2\2\u05c0E\3\2\2\2\u05c1"
          + "\u05c2\7\u00dc\2\2\u05c2\u05c3\7\30\2\2\u05c3\u05c8\5H%\2\u05c4\u05c5"
          + "\7\u00dc\2\2\u05c5\u05c6\7 \2\2\u05c6\u05c8\5J&\2\u05c7\u05c1\3\2\2\2"
          + "\u05c7\u05c4\3\2\2\2\u05c8G\3\2\2\2\u05c9\u05ca\7w\2\2\u05ca\u05cb\7\u0119"
          + "\2\2\u05cb\u05cc\7\u00a4\2\2\u05cc\u05cf\7\u0119\2\2\u05cd\u05cf\5\u0106"
          + "\u0084\2\u05ce\u05c9\3\2\2\2\u05ce\u05cd\3\2\2\2\u05cfI\3\2\2\2\u05d0"
          + "\u05d4\7\u0119\2\2\u05d1\u05d2\7\u0105\2\2\u05d2\u05d3\7\u00d0\2\2\u05d3"
          + "\u05d5\5:\36\2\u05d4\u05d1\3\2\2\2\u05d4\u05d5\3\2\2\2\u05d5K\3\2\2\2"
          + "\u05d6\u05d7\5\u0106\u0084\2\u05d7\u05d8\7\u0119\2\2\u05d8M\3\2\2\2\u05d9"
          + "\u05da\5$\23\2\u05da\u05db\5V,\2\u05db\u05dc\5R*\2\u05dc\u060d\3\2\2\2"
          + "\u05dd\u05df\5|?\2\u05de\u05e0\5T+\2\u05df\u05de\3\2\2\2\u05e0\u05e1\3"
          + "\2\2\2\u05e1\u05df\3\2\2\2\u05e1\u05e2\3\2\2\2\u05e2\u060d\3\2\2\2\u05e3"
          + "\u05e4\7D\2\2\u05e4\u05e5\7f\2\2\u05e5\u05e6\5\u00b0Y\2\u05e6\u05e8\5"
          + "\u00aaV\2\u05e7\u05e9\5t;\2\u05e8\u05e7\3\2\2\2\u05e8\u05e9\3\2\2\2\u05e9"
          + "\u060d\3\2\2\2\u05ea\u05eb\7\u00fb\2\2\u05eb\u05ec\5\u00b0Y\2\u05ec\u05ed"
          + "\5\u00aaV\2\u05ed\u05ef\5f\64\2\u05ee\u05f0\5t;\2\u05ef\u05ee\3\2\2\2"
          + "\u05ef\u05f0\3\2\2\2\u05f0\u060d\3\2\2\2\u05f1\u05f2\7\u0092\2\2\u05f2"
          + "\u05f3\7{\2\2\u05f3\u05f4\5\u00b0Y\2\u05f4\u05f5\5\u00aaV\2\u05f5\u05fb"
          + "\7\u00fe\2\2\u05f6\u05fc\5\u00b0Y\2\u05f7\u05f8\7\4\2\2\u05f8\u05f9\5"
          + "\"\22\2\u05f9\u05fa\7\5\2\2\u05fa\u05fc\3\2\2\2\u05fb\u05f6\3\2\2\2\u05fb"
          + "\u05f7\3\2\2\2\u05fc\u05fd\3\2\2\2\u05fd\u05fe\5\u00aaV\2\u05fe\u05ff"
          + "\7\u009c\2\2\u05ff\u0603\5\u00c2b\2\u0600\u0602\5h\65\2\u0601\u0600\3"
          + "\2\2\2\u0602\u0605\3\2\2\2\u0603\u0601\3\2\2\2\u0603\u0604\3\2\2\2\u0604"
          + "\u0609\3\2\2\2\u0605\u0603\3\2\2\2\u0606\u0608\5j\66\2\u0607\u0606\3\2"
          + "\2\2\u0608\u060b\3\2\2\2\u0609\u0607\3\2\2\2\u0609\u060a\3\2\2\2\u060a"
          + "\u060d\3\2\2\2\u060b\u0609\3\2\2\2\u060c\u05d9\3\2\2\2\u060c\u05dd\3\2"
          + "\2\2\u060c\u05e3\3\2\2\2\u060c\u05ea\3\2\2\2\u060c\u05f1\3\2\2\2\u060d"
          + "O\3\2\2\2\u060e\u060f\7\u0092\2\2\u060f\u0610\7{\2\2\u0610\u0611\5\u00b0"
          + "Y\2\u0611\u0612\5\u00aaV\2\u0612\u0618\7\u00fe\2\2\u0613\u0619\5\u00b0"
          + "Y\2\u0614\u0615\7\4\2\2\u0615\u0616\5\"\22\2\u0616\u0617\7\5\2\2\u0617"
          + "\u0619\3\2\2\2\u0618\u0613\3\2\2\2\u0618\u0614\3\2\2\2\u0619\u061a\3\2"
          + "\2\2\u061a\u061b\5\u00aaV\2\u061b\u061c\7\u009c\2\2\u061c\u0620\5\u00c2"
          + "b\2\u061d\u061f\5h\65\2\u061e\u061d\3\2\2\2\u061f\u0622\3\2\2\2\u0620"
          + "\u061e\3\2\2\2\u0620\u0621\3\2\2\2\u0621\u0626\3\2\2\2\u0622\u0620\3\2"
          + "\2\2\u0623\u0625\5j\66\2\u0624\u0623\3\2\2\2\u0625\u0628\3\2\2\2\u0626"
          + "\u0624\3\2\2\2\u0626\u0627\3\2\2\2\u0627Q\3\2\2\2\u0628\u0626\3\2\2\2"
          + "\u0629\u062a\7\u00a1\2\2\u062a\u062b\7 \2\2\u062b\u0630\5Z.\2\u062c\u062d"
          + "\7\6\2\2\u062d\u062f\5Z.\2\u062e\u062c\3\2\2\2\u062f\u0632\3\2\2\2\u0630"
          + "\u062e\3\2\2\2\u0630\u0631\3\2\2\2\u0631\u0634\3\2\2\2\u0632\u0630\3\2"
          + "\2\2\u0633\u0629\3\2\2\2\u0633\u0634\3\2\2\2\u0634\u063f\3\2\2\2\u0635"
          + "\u0636\7(\2\2\u0636\u0637\7 \2\2\u0637\u063c\5\u00c0a\2\u0638\u0639\7"
          + "\6\2\2\u0639\u063b\5\u00c0a\2\u063a\u0638\3\2\2\2\u063b\u063e\3\2\2\2"
          + "\u063c\u063a\3\2\2\2\u063c\u063d\3\2\2\2\u063d\u0640\3\2\2\2\u063e\u063c"
          + "\3\2\2\2\u063f\u0635\3\2\2\2\u063f\u0640\3\2\2\2\u0640\u064b\3\2\2\2\u0641"
          + "\u0642\7L\2\2\u0642\u0643\7 \2\2\u0643\u0648\5\u00c0a\2\u0644\u0645\7"
          + "\6\2\2\u0645\u0647\5\u00c0a\2\u0646\u0644\3\2\2\2\u0647\u064a\3\2\2\2"
          + "\u0648\u0646\3\2\2\2\u0648\u0649\3\2\2\2\u0649\u064c\3\2\2\2\u064a\u0648"
          + "\3\2\2\2\u064b\u0641\3\2\2\2\u064b\u064c\3\2\2\2\u064c\u0657\3\2\2\2\u064d"
          + "\u064e\7\u00d8\2\2\u064e\u064f\7 \2\2\u064f\u0654\5Z.\2\u0650\u0651\7"
          + "\6\2\2\u0651\u0653\5Z.\2\u0652\u0650\3\2\2\2\u0653\u0656\3\2\2\2\u0654"
          + "\u0652\3\2\2\2\u0654\u0655\3\2\2\2\u0655\u0658\3\2\2\2\u0656\u0654\3\2"
          + "\2\2\u0657\u064d\3\2\2\2\u0657\u0658\3\2\2\2\u0658\u065a\3\2\2\2\u0659"
          + "\u065b\5\u00f2z\2\u065a\u0659\3\2\2\2\u065a\u065b\3\2\2\2\u065b\u0661"
          + "\3\2\2\2\u065c\u065f\7\u0086\2\2\u065d\u0660\7\20\2\2\u065e\u0660\5\u00c0"
          + "a\2\u065f\u065d\3\2\2\2\u065f\u065e\3\2\2\2\u0660\u0662\3\2\2\2\u0661"
          + "\u065c\3\2\2\2\u0661\u0662\3\2\2\2\u0662S\3\2\2\2\u0663\u0664\5$\23\2"
          + "\u0664\u0665\5^\60\2\u0665U\3\2\2\2\u0666\u0667\b,\1\2\u0667\u0668\5X"
          + "-\2\u0668\u0680\3\2\2\2\u0669\u066a\f\5\2\2\u066a\u066b\6,\3\2\u066b\u066d"
          + "\t\20\2\2\u066c\u066e\5\u008aF\2\u066d\u066c\3\2\2\2\u066d\u066e\3\2\2"
          + "\2\u066e\u066f\3\2\2\2\u066f\u067f\5V,\6\u0670\u0671\f\4\2\2\u0671\u0672"
          + "\6,\5\2\u0672\u0674\7y\2\2\u0673\u0675\5\u008aF\2\u0674\u0673\3\2\2\2"
          + "\u0674\u0675\3\2\2\2\u0675\u0676\3\2\2\2\u0676\u067f\5V,\5\u0677\u0678"
          + "\f\3\2\2\u0678\u0679\6,\7\2\u0679\u067b\t\21\2\2\u067a\u067c\5\u008aF"
          + "\2\u067b\u067a\3\2\2\2\u067b\u067c\3\2\2\2\u067c\u067d\3\2\2\2\u067d\u067f"
          + "\5V,\4\u067e\u0669\3\2\2\2\u067e\u0670\3\2\2\2\u067e\u0677\3\2\2\2\u067f"
          + "\u0682\3\2\2\2\u0680\u067e\3\2\2\2\u0680\u0681\3\2\2\2\u0681W\3\2\2\2"
          + "\u0682\u0680\3\2\2\2\u0683\u068d\5`\61\2\u0684\u068d\5\\/\2\u0685\u0686"
          + "\7\u00e1\2\2\u0686\u068d\5\u00b0Y\2\u0687\u068d\5\u00a6T\2\u0688\u0689"
          + "\7\4\2\2\u0689\u068a\5\"\22\2\u068a\u068b\7\5\2\2\u068b\u068d\3\2\2\2"
          + "\u068c\u0683\3\2\2\2\u068c\u0684\3\2\2\2\u068c\u0685\3\2\2\2\u068c\u0687"
          + "\3\2\2\2\u068c\u0688\3\2\2\2\u068dY\3\2\2\2\u068e\u0690\5\u00c0a\2\u068f"
          + "\u0691\t\22\2\2\u0690\u068f\3\2\2\2\u0690\u0691\3\2\2\2\u0691\u0694\3"
          + "\2\2\2\u0692\u0693\7\u009a\2\2\u0693\u0695\t\23\2\2\u0694\u0692\3\2\2"
          + "\2\u0694\u0695\3\2\2\2\u0695[\3\2\2\2\u0696\u0698\5|?\2\u0697\u0699\5"
          + "^\60\2\u0698\u0697\3\2\2\2\u0699\u069a\3\2\2\2\u069a\u0698\3\2\2\2\u069a"
          + "\u069b\3\2\2\2\u069b]\3\2\2\2\u069c\u069e\5b\62\2\u069d\u069f\5t;\2\u069e"
          + "\u069d\3\2\2\2\u069e\u069f\3\2\2\2\u069f\u06a0\3\2\2\2\u06a0\u06a1\5R"
          + "*\2\u06a1\u06b8\3\2\2\2\u06a2\u06a6\5d\63\2\u06a3\u06a5\5\u0088E\2\u06a4"
          + "\u06a3\3\2\2\2\u06a5\u06a8\3\2\2\2\u06a6\u06a4\3\2\2\2\u06a6\u06a7\3\2"
          + "\2\2\u06a7\u06aa\3\2\2\2\u06a8\u06a6\3\2\2\2\u06a9\u06ab\5t;\2\u06aa\u06a9"
          + "\3\2\2\2\u06aa\u06ab\3\2\2\2\u06ab\u06ad\3\2\2\2\u06ac\u06ae\5~@\2\u06ad"
          + "\u06ac\3\2\2\2\u06ad\u06ae\3\2\2\2\u06ae\u06b0\3\2\2\2\u06af\u06b1\5v"
          + "<\2\u06b0\u06af\3\2\2\2\u06b0\u06b1\3\2\2\2\u06b1\u06b3\3\2\2\2\u06b2"
          + "\u06b4\5\u00f2z\2\u06b3\u06b2\3\2\2\2\u06b3\u06b4\3\2\2\2\u06b4\u06b5"
          + "\3\2\2\2\u06b5\u06b6\5R*\2\u06b6\u06b8\3\2\2\2\u06b7\u069c\3\2\2\2\u06b7"
          + "\u06a2\3\2\2\2\u06b8_\3\2\2\2\u06b9\u06bb\5b\62\2\u06ba\u06bc\5|?\2\u06bb"
          + "\u06ba\3\2\2\2\u06bb\u06bc\3\2\2\2\u06bc\u06be\3\2\2\2\u06bd\u06bf\5t"
          + ";\2\u06be\u06bd\3\2\2\2\u06be\u06bf\3\2\2\2\u06bf\u06d7\3\2\2\2\u06c0"
          + "\u06c2\5d\63\2\u06c1\u06c3\5|?\2\u06c2\u06c1\3\2\2\2\u06c2\u06c3\3\2\2"
          + "\2\u06c3\u06c7\3\2\2\2\u06c4\u06c6\5\u0088E\2\u06c5\u06c4\3\2\2\2\u06c6"
          + "\u06c9\3\2\2\2\u06c7\u06c5\3\2\2\2\u06c7\u06c8\3\2\2\2\u06c8\u06cb\3\2"
          + "\2\2\u06c9\u06c7\3\2\2\2\u06ca\u06cc\5t;\2\u06cb\u06ca\3\2\2\2\u06cb\u06cc"
          + "\3\2\2\2\u06cc\u06ce\3\2\2\2\u06cd\u06cf\5~@\2\u06ce\u06cd\3\2\2\2\u06ce"
          + "\u06cf\3\2\2\2\u06cf\u06d1\3\2\2\2\u06d0\u06d2\5v<\2\u06d1\u06d0\3\2\2"
          + "\2\u06d1\u06d2\3\2\2\2\u06d2\u06d4\3\2\2\2\u06d3\u06d5\5\u00f2z\2\u06d4"
          + "\u06d3\3\2\2\2\u06d4\u06d5\3\2\2\2\u06d5\u06d7\3\2\2\2\u06d6\u06b9\3\2"
          + "\2\2\u06d6\u06c0\3\2\2\2\u06d7a\3\2\2\2\u06d8\u06d9\7\u00cc\2\2\u06d9"
          + "\u06da\7\u00ee\2\2\u06da\u06db\7\4\2\2\u06db\u06dc\5\u00b8]\2\u06dc\u06dd"
          + "\7\5\2\2\u06dd\u06e3\3\2\2\2\u06de\u06df\7\u0090\2\2\u06df\u06e3\5\u00b8"
          + "]\2\u06e0\u06e1\7\u00ba\2\2\u06e1\u06e3\5\u00b8]\2\u06e2\u06d8\3\2\2\2"
          + "\u06e2\u06de\3\2\2\2\u06e2\u06e0\3\2\2\2\u06e3\u06e5\3\2\2\2\u06e4\u06e6"
          + "\5\u00acW\2\u06e5\u06e4\3\2\2\2\u06e5\u06e6\3\2\2\2\u06e6\u06e9\3\2\2"
          + "\2\u06e7\u06e8\7\u00b8\2\2\u06e8\u06ea\7\u0119\2\2\u06e9\u06e7\3\2\2\2"
          + "\u06e9\u06ea\3\2\2\2\u06ea\u06eb\3\2\2\2\u06eb\u06ec\7\u00fe\2\2\u06ec"
          + "\u06f9\7\u0119\2\2\u06ed\u06f7\7\30\2\2\u06ee\u06f8\5\u009aN\2\u06ef\u06f8"
          + "\5\u00e8u\2\u06f0\u06f3\7\4\2\2\u06f1\u06f4\5\u009aN\2\u06f2\u06f4\5\u00e8"
          + "u\2\u06f3\u06f1\3\2\2\2\u06f3\u06f2\3\2\2\2\u06f4\u06f5\3\2\2\2\u06f5"
          + "\u06f6\7\5\2\2\u06f6\u06f8\3\2\2\2\u06f7\u06ee\3\2\2\2\u06f7\u06ef\3\2"
          + "\2\2\u06f7\u06f0\3\2\2\2\u06f8\u06fa\3\2\2\2\u06f9\u06ed\3\2\2\2\u06f9"
          + "\u06fa\3\2\2\2\u06fa\u06fc\3\2\2\2\u06fb\u06fd\5\u00acW\2\u06fc\u06fb"
          + "\3\2\2\2\u06fc\u06fd\3\2\2\2\u06fd\u0700\3\2\2\2\u06fe\u06ff\7\u00b7\2"
          + "\2\u06ff\u0701\7\u0119\2\2\u0700\u06fe\3\2\2\2\u0700\u0701\3\2\2\2\u0701"
          + "c\3\2\2\2\u0702\u0706\7\u00cc\2\2\u0703\u0705\5x=\2\u0704\u0703\3\2\2"
          + "\2\u0705\u0708\3\2\2\2\u0706\u0704\3\2\2\2\u0706\u0707\3\2\2\2\u0707\u070a"
          + "\3\2\2\2\u0708\u0706\3\2\2\2\u0709\u070b\5\u008aF\2\u070a\u0709\3\2\2"
          + "\2\u070a\u070b\3\2\2\2\u070b\u070c\3\2\2\2\u070c\u070d\5\u00b8]\2\u070d"
          + "e\3\2\2\2\u070e\u070f\7\u00d2\2\2\u070f\u0710\5p9\2\u0710g\3\2\2\2\u0711"
          + "\u0712\7\u0102\2\2\u0712\u0715\7\u0091\2\2\u0713\u0714\7\23\2\2\u0714"
          + "\u0716\5\u00c2b\2\u0715\u0713\3\2\2\2\u0715\u0716\3\2\2\2\u0716\u0717"
          + "\3\2\2\2\u0717\u0718\7\u00e7\2\2\u0718\u0719\5l\67\2\u0719i\3\2\2\2\u071a"
          + "\u071b\7\u0102\2\2\u071b\u071c\7\u0098\2\2\u071c\u071f\7\u0091\2\2\u071d"
          + "\u071e\7\23\2\2\u071e\u0720\5\u00c2b\2\u071f\u071d\3\2\2\2\u071f\u0720"
          + "\3\2\2\2\u0720\u0721\3\2\2\2\u0721\u0722\7\u00e7\2\2\u0722\u0723\5n8\2"
          + "\u0723k\3\2\2\2\u0724\u072c\7D\2\2\u0725\u0726\7\u00fb\2\2\u0726\u0727"
          + "\7\u00d2\2\2\u0727\u072c\7\u0111\2\2\u0728\u0729\7\u00fb\2\2\u0729\u072a"
          + "\7\u00d2\2\2\u072a\u072c\5p9\2\u072b\u0724\3\2\2\2\u072b\u0725\3\2\2\2"
          + "\u072b\u0728\3\2\2\2\u072cm\3\2\2\2\u072d\u072e\7x\2\2\u072e\u0740\7\u0111"
          + "\2\2\u072f\u0730\7x\2\2\u0730\u0731\7\4\2\2\u0731\u0732\5\u00aeX\2\u0732"
          + "\u0733\7\5\2\2\u0733\u0734\7\u00ff\2\2\u0734\u0735\7\4\2\2\u0735\u073a"
          + "\5\u00c0a\2\u0736\u0737\7\6\2\2\u0737\u0739\5\u00c0a\2\u0738\u0736\3\2"
          + "\2\2\u0739\u073c\3\2\2\2\u073a\u0738\3\2\2\2\u073a\u073b\3\2\2\2\u073b"
          + "\u073d\3\2\2\2\u073c\u073a\3\2\2\2\u073d\u073e\7\5\2\2\u073e\u0740\3\2"
          + "\2\2\u073f\u072d\3\2\2\2\u073f\u072f\3\2\2\2\u0740o\3\2\2\2\u0741\u0746"
          + "\5r:\2\u0742\u0743\7\6\2\2\u0743\u0745\5r:\2\u0744\u0742\3\2\2\2\u0745"
          + "\u0748\3\2\2\2\u0746\u0744\3\2\2\2\u0746\u0747\3\2\2\2\u0747q\3\2\2\2"
          + "\u0748\u0746\3\2\2\2\u0749\u074a\5\u00b0Y\2\u074a\u074b\7\u0107\2\2\u074b"
          + "\u074c\5\u00c0a\2\u074cs\3\2\2\2\u074d\u074e\7\u0103\2\2\u074e\u074f\5"
          + "\u00c2b\2\u074fu\3\2\2\2\u0750\u0751\7n\2\2\u0751\u0752\5\u00c2b\2\u0752"
          + "w\3\2\2\2\u0753\u0754\7\b\2\2\u0754\u075b\5z>\2\u0755\u0757\7\6\2\2\u0756"
          + "\u0755\3\2\2\2\u0756\u0757\3\2\2\2\u0757\u0758\3\2\2\2\u0758\u075a\5z"
          + ">\2\u0759\u0756\3\2\2\2\u075a\u075d\3\2\2\2\u075b\u0759\3\2\2\2\u075b"
          + "\u075c\3\2\2\2\u075c\u075e\3\2\2\2\u075d\u075b\3\2\2\2\u075e\u075f\7\t"
          + "\2\2\u075fy\3\2\2\2\u0760\u076e\5\u0106\u0084\2\u0761\u0762\5\u0106\u0084"
          + "\2\u0762\u0763\7\4\2\2\u0763\u0768\5\u00c8e\2\u0764\u0765\7\6\2\2\u0765"
          + "\u0767\5\u00c8e\2\u0766\u0764\3\2\2\2\u0767\u076a\3\2\2\2\u0768\u0766"
          + "\3\2\2\2\u0768\u0769\3\2\2\2\u0769\u076b\3\2\2\2\u076a\u0768\3\2\2\2\u076b"
          + "\u076c\7\5\2\2\u076c\u076e\3\2\2\2\u076d\u0760\3\2\2\2\u076d\u0761\3\2"
          + "\2\2\u076e{\3\2\2\2\u076f\u0770\7f\2\2\u0770\u0775\5\u008cG\2\u0771\u0772"
          + "\7\6\2\2\u0772\u0774\5\u008cG\2\u0773\u0771\3\2\2\2\u0774\u0777\3\2\2"
          + "\2\u0775\u0773\3\2\2\2\u0775\u0776\3\2\2\2\u0776\u077b\3\2\2\2\u0777\u0775"
          + "\3\2\2\2\u0778\u077a\5\u0088E\2\u0779\u0778\3\2\2\2\u077a\u077d\3\2\2"
          + "\2\u077b\u0779\3\2\2\2\u077b\u077c\3\2\2\2\u077c\u077f\3\2\2\2\u077d\u077b"
          + "\3\2\2\2\u077e\u0780\5\u0082B\2\u077f\u077e\3\2\2\2\u077f\u0780\3\2\2"
          + "\2\u0780}\3\2\2\2\u0781\u0782\7l\2\2\u0782\u0783\7 \2\2\u0783\u0788\5"
          + "\u00c0a\2\u0784\u0785\7\6\2\2\u0785\u0787\5\u00c0a\2\u0786\u0784\3\2\2"
          + "\2\u0787\u078a\3\2\2\2\u0788\u0786\3\2\2\2\u0788\u0789\3\2\2\2\u0789\u079c"
          + "\3\2\2\2\u078a\u0788\3\2\2\2\u078b\u078c\7\u0105\2\2\u078c\u079d\7\u00c8"
          + "\2\2\u078d\u078e\7\u0105\2\2\u078e\u079d\79\2\2\u078f\u0790\7m\2\2\u0790"
          + "\u0791\7\u00d4\2\2\u0791\u0792\7\4\2\2\u0792\u0797\5\u0080A\2\u0793\u0794"
          + "\7\6\2\2\u0794\u0796\5\u0080A\2\u0795\u0793\3\2\2\2\u0796\u0799\3\2\2"
          + "\2\u0797\u0795\3\2\2\2\u0797\u0798\3\2\2\2\u0798\u079a\3\2\2\2\u0799\u0797"
          + "\3\2\2\2\u079a\u079b\7\5\2\2\u079b\u079d\3\2\2\2\u079c\u078b\3\2\2\2\u079c"
          + "\u078d\3\2\2\2\u079c\u078f\3\2\2\2\u079c\u079d\3\2\2\2\u079d\u07ae\3\2"
          + "\2\2\u079e\u079f\7l\2\2\u079f\u07a0\7 \2\2\u07a0\u07a1\7m\2\2\u07a1\u07a2"
          + "\7\u00d4\2\2\u07a2\u07a3\7\4\2\2\u07a3\u07a8\5\u0080A\2\u07a4\u07a5\7"
          + "\6\2\2\u07a5\u07a7\5\u0080A\2\u07a6\u07a4\3\2\2\2\u07a7\u07aa\3\2\2\2"
          + "\u07a8\u07a6\3\2\2\2\u07a8\u07a9\3\2\2\2\u07a9\u07ab\3\2\2\2\u07aa\u07a8"
          + "\3\2\2\2\u07ab\u07ac\7\5\2\2\u07ac\u07ae\3\2\2\2\u07ad\u0781\3\2\2\2\u07ad"
          + "\u079e\3\2\2\2\u07ae\177\3\2\2\2\u07af\u07b8\7\4\2\2\u07b0\u07b5\5\u00c0"
          + "a\2\u07b1\u07b2\7\6\2\2\u07b2\u07b4\5\u00c0a\2\u07b3\u07b1\3\2\2\2\u07b4"
          + "\u07b7\3\2\2\2\u07b5\u07b3\3\2\2\2\u07b5\u07b6\3\2\2\2\u07b6\u07b9\3\2"
          + "\2\2\u07b7\u07b5\3\2\2\2\u07b8\u07b0\3\2\2\2\u07b8\u07b9\3\2\2\2\u07b9"
          + "\u07ba\3\2\2\2\u07ba\u07bd\7\5\2\2\u07bb\u07bd\5\u00c0a\2\u07bc\u07af"
          + "\3\2\2\2\u07bc\u07bb\3\2\2\2\u07bd\u0081\3\2\2\2\u07be\u07bf\7\u00ad\2"
          + "\2\u07bf\u07c0\7\4\2\2\u07c0\u07c1\5\u00b8]\2\u07c1\u07c2\7b\2\2\u07c2"
          + "\u07c3\5\u0084C\2\u07c3\u07c4\7r\2\2\u07c4\u07c5\7\4\2\2\u07c5\u07ca\5"
          + "\u0086D\2\u07c6\u07c7\7\6\2\2\u07c7\u07c9\5\u0086D\2\u07c8\u07c6\3\2\2"
          + "\2\u07c9\u07cc\3\2\2\2\u07ca\u07c8\3\2\2\2\u07ca\u07cb\3\2\2\2\u07cb\u07cd"
          + "\3\2\2\2\u07cc\u07ca\3\2\2\2\u07cd\u07ce\7\5\2\2\u07ce\u07cf\7\5\2\2\u07cf"
          + "\u0083\3\2\2\2\u07d0\u07dd\5\u0106\u0084\2\u07d1\u07d2\7\4\2\2\u07d2\u07d7"
          + "\5\u0106\u0084\2\u07d3\u07d4\7\6\2\2\u07d4\u07d6\5\u0106\u0084\2\u07d5"
          + "\u07d3\3\2\2\2\u07d6\u07d9\3\2\2\2\u07d7\u07d5\3\2\2\2\u07d7\u07d8\3\2"
          + "\2\2\u07d8\u07da\3\2\2\2\u07d9\u07d7\3\2\2\2\u07da\u07db\7\5\2\2\u07db"
          + "\u07dd\3\2\2\2\u07dc\u07d0\3\2\2\2\u07dc\u07d1\3\2\2\2\u07dd\u0085\3\2"
          + "\2\2\u07de\u07e3\5\u00c0a\2\u07df\u07e1\7\30\2\2\u07e0\u07df\3\2\2\2\u07e0"
          + "\u07e1\3\2\2\2\u07e1\u07e2\3\2\2\2\u07e2\u07e4\5\u0106\u0084\2\u07e3\u07e0"
          + "\3\2\2\2\u07e3\u07e4\3\2\2\2\u07e4\u0087\3\2\2\2\u07e5\u07e6\7\u0081\2"
          + "\2\u07e6\u07e8\7\u0100\2\2\u07e7\u07e9\7\u00a3\2\2\u07e8\u07e7\3\2\2\2"
          + "\u07e8\u07e9\3\2\2\2\u07e9\u07ea\3\2\2\2\u07ea\u07eb\5\u0100\u0081\2\u07eb"
          + "\u07f4\7\4\2\2\u07ec\u07f1\5\u00c0a\2\u07ed\u07ee\7\6\2\2\u07ee\u07f0"
          + "\5\u00c0a\2\u07ef\u07ed\3\2\2\2\u07f0\u07f3\3\2\2\2\u07f1\u07ef\3\2\2"
          + "\2\u07f1\u07f2\3\2\2\2\u07f2\u07f5\3\2\2\2\u07f3\u07f1\3\2\2\2\u07f4\u07ec"
          + "\3\2\2\2\u07f4\u07f5\3\2\2\2\u07f5\u07f6\3\2\2\2\u07f6\u07f7\7\5\2\2\u07f7"
          + "\u0803\5\u0106\u0084\2\u07f8\u07fa\7\30\2\2\u07f9\u07f8\3\2\2\2\u07f9"
          + "\u07fa\3\2\2\2\u07fa\u07fb\3\2\2\2\u07fb\u0800\5\u0106\u0084\2\u07fc\u07fd"
          + "\7\6\2\2\u07fd\u07ff\5\u0106\u0084\2\u07fe\u07fc\3\2\2\2\u07ff\u0802\3"
          + "\2\2\2\u0800\u07fe\3\2\2\2\u0800\u0801\3\2\2\2\u0801\u0804\3\2\2\2\u0802"
          + "\u0800\3\2\2\2\u0803\u07f9\3\2\2\2\u0803\u0804\3\2\2\2\u0804\u0089\3\2"
          + "\2\2\u0805\u0806\t\24\2\2\u0806\u008b\3\2\2\2\u0807\u080b\5\u00a4S\2\u0808"
          + "\u080a\5\u008eH\2\u0809\u0808\3\2\2\2\u080a\u080d\3\2\2\2\u080b\u0809"
          + "\3\2\2\2\u080b\u080c\3\2\2\2\u080c\u008d\3\2\2\2\u080d\u080b\3\2\2\2\u080e"
          + "\u080f\5\u0090I\2\u080f\u0810\7~\2\2\u0810\u0812\5\u00a4S\2\u0811\u0813"
          + "\5\u0092J\2\u0812\u0811\3\2\2\2\u0812\u0813\3\2\2\2\u0813\u081a\3\2\2"
          + "\2\u0814\u0815\7\u0096\2\2\u0815\u0816\5\u0090I\2\u0816\u0817\7~\2\2\u0817"
          + "\u0818\5\u00a4S\2\u0818\u081a\3\2\2\2\u0819\u080e\3\2\2\2\u0819\u0814"
          + "\3\2\2\2\u081a\u008f\3\2\2\2\u081b\u081d\7u\2\2\u081c\u081b\3\2\2\2\u081c"
          + "\u081d\3\2\2\2\u081d\u0834\3\2\2\2\u081e\u0834\78\2\2\u081f\u0821\7\u0084"
          + "\2\2\u0820\u0822\7\u00a3\2\2\u0821\u0820\3\2\2\2\u0821\u0822\3\2\2\2\u0822"
          + "\u0834\3\2\2\2\u0823\u0825\7\u0084\2\2\u0824\u0823\3\2\2\2\u0824\u0825"
          + "\3\2\2\2\u0825\u0826\3\2\2\2\u0826\u0834\7\u00cd\2\2\u0827\u0829\7\u00c3"
          + "\2\2\u0828\u082a\7\u00a3\2\2\u0829\u0828\3\2\2\2\u0829\u082a\3\2\2\2\u082a"
          + "\u0834\3\2\2\2\u082b\u082d\7g\2\2\u082c\u082e\7\u00a3\2\2\u082d\u082c"
          + "\3\2\2\2\u082d\u082e\3\2\2\2\u082e\u0834\3\2\2\2\u082f\u0831\7\u0084\2"
          + "\2\u0830\u082f\3\2\2\2\u0830\u0831\3\2\2\2\u0831\u0832\3\2\2\2\u0832\u0834"
          + "\7\24\2\2\u0833\u081c\3\2\2\2\u0833\u081e\3\2\2\2\u0833\u081f\3\2\2\2"
          + "\u0833\u0824\3\2\2\2\u0833\u0827\3\2\2\2\u0833\u082b\3\2\2\2\u0833\u0830"
          + "\3\2\2\2\u0834\u0091\3\2\2\2\u0835\u0836\7\u009c\2\2\u0836\u083a\5\u00c2"
          + "b\2\u0837\u0838\7\u00fe\2\2\u0838\u083a\5\u0098M\2\u0839\u0835\3\2\2\2"
          + "\u0839\u0837\3\2\2\2\u083a\u0093\3\2\2\2\u083b\u083c\7\u00e3\2\2\u083c"
          + "\u083e\7\4\2\2\u083d\u083f\5\u0096L\2\u083e\u083d\3\2\2\2\u083e\u083f"
          + "\3\2\2\2\u083f\u0840\3\2\2\2\u0840\u0841\7\5\2\2\u0841\u0095\3\2\2\2\u0842"
          + "\u0844\7\u0110\2\2\u0843\u0842\3\2\2\2\u0843\u0844\3\2\2\2\u0844\u0845"
          + "\3\2\2\2\u0845\u0846\t\25\2\2\u0846\u085b\7\u00ac\2\2\u0847\u0848\5\u00c0"
          + "a\2\u0848\u0849\7\u00ca\2\2\u0849\u085b\3\2\2\2\u084a\u084b\7\36\2\2\u084b"
          + "\u084c\7\u011d\2\2\u084c\u084d\7\u00a2\2\2\u084d\u084e\7\u009b\2\2\u084e"
          + "\u0857\7\u011d\2\2\u084f\u0855\7\u009c\2\2\u0850\u0856\5\u0106\u0084\2"
          + "\u0851\u0852\5\u0100\u0081\2\u0852\u0853\7\4\2\2\u0853\u0854\7\5\2\2\u0854"
          + "\u0856\3\2\2\2\u0855\u0850\3\2\2\2\u0855\u0851\3\2\2\2\u0856\u0858\3\2"
          + "\2\2\u0857\u084f\3\2\2\2\u0857\u0858\3\2\2\2\u0858\u085b\3\2\2\2\u0859"
          + "\u085b\5\u00c0a\2\u085a\u0843\3\2\2\2\u085a\u0847\3\2\2\2\u085a\u084a"
          + "\3\2\2\2\u085a\u0859\3\2\2\2\u085b\u0097\3\2\2\2\u085c\u085d\7\4\2\2\u085d"
          + "\u085e\5\u009aN\2\u085e\u085f\7\5\2\2\u085f\u0099\3\2\2\2\u0860\u0865"
          + "\5\u0102\u0082\2\u0861\u0862\7\6\2\2\u0862\u0864\5\u0102\u0082\2\u0863"
          + "\u0861\3\2\2\2\u0864\u0867\3\2\2\2\u0865\u0863\3\2\2\2\u0865\u0866\3\2"
          + "\2\2\u0866\u009b\3\2\2\2\u0867\u0865\3\2\2\2\u0868\u0869\7\4\2\2\u0869"
          + "\u086e\5\u009eP\2\u086a\u086b\7\6\2\2\u086b\u086d\5\u009eP\2\u086c\u086a"
          + "\3\2\2\2\u086d\u0870\3\2\2\2\u086e\u086c\3\2\2\2\u086e\u086f\3\2\2\2\u086f"
          + "\u0871\3\2\2";
  private static final String _serializedATNSegment1 =
      "\2\u0870\u086e\3\2\2\2\u0871\u0872\7\5\2\2\u0872\u009d\3\2\2\2\u0873\u0875"
          + "\5\u0102\u0082\2\u0874\u0876\t\22\2\2\u0875\u0874\3\2\2\2\u0875\u0876"
          + "\3\2\2\2\u0876\u009f\3\2\2\2\u0877\u0878\7\4\2\2\u0878\u087d\5\u00a2R"
          + "\2\u0879\u087a\7\6\2\2\u087a\u087c\5\u00a2R\2\u087b\u0879\3\2\2\2\u087c"
          + "\u087f\3\2\2\2\u087d\u087b\3\2\2\2\u087d\u087e\3\2\2\2\u087e\u0880\3\2"
          + "\2\2\u087f\u087d\3\2\2\2\u0880\u0881\7\5\2\2\u0881\u00a1\3\2\2\2\u0882"
          + "\u0884\5\u0106\u0084\2\u0883\u0885\5 \21\2\u0884\u0883\3\2\2\2\u0884\u0885"
          + "\3\2\2\2\u0885\u00a3\3\2\2\2\u0886\u0888\5\u00b0Y\2\u0887\u0889\5\u0094"
          + "K\2\u0888\u0887\3\2\2\2\u0888\u0889\3\2\2\2\u0889\u088a\3\2\2\2\u088a"
          + "\u088b\5\u00aaV\2\u088b\u089f\3\2\2\2\u088c\u088d\7\4\2\2\u088d\u088e"
          + "\5\"\22\2\u088e\u0890\7\5\2\2\u088f\u0891\5\u0094K\2\u0890\u088f\3\2\2"
          + "\2\u0890\u0891\3\2\2\2\u0891\u0892\3\2\2\2\u0892\u0893\5\u00aaV\2\u0893"
          + "\u089f\3\2\2\2\u0894\u0895\7\4\2\2\u0895\u0896\5\u008cG\2\u0896\u0898"
          + "\7\5\2\2\u0897\u0899\5\u0094K\2\u0898\u0897\3\2\2\2\u0898\u0899\3\2\2"
          + "\2\u0899\u089a\3\2\2\2\u089a\u089b\5\u00aaV\2\u089b\u089f\3\2\2\2\u089c"
          + "\u089f\5\u00a6T\2\u089d\u089f\5\u00a8U\2\u089e\u0886\3\2\2\2\u089e\u088c"
          + "\3\2\2\2\u089e\u0894\3\2\2\2\u089e\u089c\3\2\2\2\u089e\u089d\3\2\2\2\u089f"
          + "\u00a5\3\2\2\2\u08a0\u08a1\7\u00ff\2\2\u08a1\u08a6\5\u00c0a\2\u08a2\u08a3"
          + "\7\6\2\2\u08a3\u08a5\5\u00c0a\2\u08a4\u08a2\3\2\2\2\u08a5\u08a8\3\2\2"
          + "\2\u08a6\u08a4\3\2\2\2\u08a6\u08a7\3\2\2\2\u08a7\u08a9\3\2\2\2\u08a8\u08a6"
          + "\3\2\2\2\u08a9\u08aa\5\u00aaV\2\u08aa\u00a7\3\2\2\2\u08ab\u08ac\5\u0102"
          + "\u0082\2\u08ac\u08b5\7\4\2\2\u08ad\u08b2\5\u00c0a\2\u08ae\u08af\7\6\2"
          + "\2\u08af\u08b1\5\u00c0a\2\u08b0\u08ae\3\2\2\2\u08b1\u08b4\3\2\2\2\u08b2"
          + "\u08b0\3\2\2\2\u08b2\u08b3\3\2\2\2\u08b3\u08b6\3\2\2\2\u08b4\u08b2\3\2"
          + "\2\2\u08b5\u08ad\3\2\2\2\u08b5\u08b6\3\2\2\2\u08b6\u08b7\3\2\2\2\u08b7"
          + "\u08b8\7\5\2\2\u08b8\u08b9\5\u00aaV\2\u08b9\u00a9\3\2\2\2\u08ba\u08bc"
          + "\7\30\2\2\u08bb\u08ba\3\2\2\2\u08bb\u08bc\3\2\2\2\u08bc\u08bd\3\2\2\2"
          + "\u08bd\u08bf\5\u0108\u0085\2\u08be\u08c0\5\u0098M\2\u08bf\u08be\3\2\2"
          + "\2\u08bf\u08c0\3\2\2\2\u08c0\u08c2\3\2\2\2\u08c1\u08bb\3\2\2\2\u08c1\u08c2"
          + "\3\2\2\2\u08c2\u00ab\3\2\2\2\u08c3\u08c4\7\u00c9\2\2\u08c4\u08c5\7d\2"
          + "\2\u08c5\u08c6\7\u00cf\2\2\u08c6\u08ca\7\u0119\2\2\u08c7\u08c8\7\u0105"
          + "\2\2\u08c8\u08c9\7\u00d0\2\2\u08c9\u08cb\5:\36\2\u08ca\u08c7\3\2\2\2\u08ca"
          + "\u08cb\3\2\2\2\u08cb\u08f5\3\2\2\2\u08cc\u08cd\7\u00c9\2\2\u08cd\u08ce"
          + "\7d\2\2\u08ce\u08d8\7E\2\2\u08cf\u08d0\7]\2\2\u08d0\u08d1\7\u00e6\2\2"
          + "\u08d1\u08d2\7 \2\2\u08d2\u08d6\7\u0119\2\2\u08d3\u08d4\7R\2\2\u08d4\u08d5"
          + "\7 \2\2\u08d5\u08d7\7\u0119\2\2\u08d6\u08d3\3\2\2\2\u08d6\u08d7\3\2\2"
          + "\2\u08d7\u08d9\3\2\2\2\u08d8\u08cf\3\2\2\2\u08d8\u08d9\3\2\2\2\u08d9\u08df"
          + "\3\2\2\2\u08da\u08db\7,\2\2\u08db\u08dc\7}\2\2\u08dc\u08dd\7\u00e6\2\2"
          + "\u08dd\u08de\7 \2\2\u08de\u08e0\7\u0119\2\2\u08df\u08da\3\2\2\2\u08df"
          + "\u08e0\3\2\2\2\u08e0\u08e6\3\2\2\2\u08e1\u08e2\7\u0090\2\2\u08e2\u08e3"
          + "\7\177\2\2\u08e3\u08e4\7\u00e6\2\2\u08e4\u08e5\7 \2\2\u08e5\u08e7\7\u0119"
          + "\2\2\u08e6\u08e1\3\2\2\2\u08e6\u08e7\3\2\2\2\u08e7\u08ec\3\2\2\2\u08e8"
          + "\u08e9\7\u0087\2\2\u08e9\u08ea\7\u00e6\2\2\u08ea\u08eb\7 \2\2\u08eb\u08ed"
          + "\7\u0119\2\2\u08ec\u08e8\3\2\2\2\u08ec\u08ed\3\2\2\2\u08ed\u08f2\3\2\2"
          + "\2\u08ee\u08ef\7\u0099\2\2\u08ef\u08f0\7C\2\2\u08f0\u08f1\7\30\2\2\u08f1"
          + "\u08f3\7\u0119\2\2\u08f2\u08ee\3\2\2\2\u08f2\u08f3\3\2\2\2\u08f3\u08f5"
          + "\3\2\2\2\u08f4\u08c3\3\2\2\2\u08f4\u08cc\3\2\2\2\u08f5\u00ad\3\2\2\2\u08f6"
          + "\u08fb\5\u00b0Y\2\u08f7\u08f8\7\6\2\2\u08f8\u08fa\5\u00b0Y\2\u08f9\u08f7"
          + "\3\2\2\2\u08fa\u08fd\3\2\2\2\u08fb\u08f9\3\2\2\2\u08fb\u08fc\3\2\2\2\u08fc"
          + "\u00af\3\2\2\2\u08fd\u08fb\3\2\2\2\u08fe\u0903\5\u0102\u0082\2\u08ff\u0900"
          + "\7\7\2\2\u0900\u0902\5\u0102\u0082\2\u0901\u08ff\3\2\2\2\u0902\u0905\3"
          + "\2\2\2\u0903\u0901\3\2\2\2\u0903\u0904\3\2\2\2\u0904\u00b1\3\2\2\2\u0905"
          + "\u0903\3\2\2\2\u0906\u0907\5\u0102\u0082\2\u0907\u0908\7\7\2\2\u0908\u090a"
          + "\3\2\2\2\u0909\u0906\3\2\2\2\u0909\u090a\3\2\2\2\u090a\u090b\3\2\2\2\u090b"
          + "\u090c\5\u0102\u0082\2\u090c\u00b3\3\2\2\2\u090d\u090e\5\u0102\u0082\2"
          + "\u090e\u090f\7\7\2\2\u090f\u0911\3\2\2\2\u0910\u090d\3\2\2\2\u0910\u0911"
          + "\3\2\2\2\u0911\u0912\3\2\2\2\u0912\u0913\5\u0102\u0082\2\u0913\u00b5\3"
          + "\2\2\2\u0914\u091c\5\u00c0a\2\u0915\u0917\7\30\2\2\u0916\u0915\3\2\2\2"
          + "\u0916\u0917\3\2\2\2\u0917\u091a\3\2\2\2\u0918\u091b\5\u0102\u0082\2\u0919"
          + "\u091b\5\u0098M\2\u091a\u0918\3\2\2\2\u091a\u0919\3\2\2\2\u091b\u091d"
          + "\3\2\2\2\u091c\u0916\3\2\2\2\u091c\u091d\3\2\2\2\u091d\u00b7\3\2\2\2\u091e"
          + "\u0923\5\u00b6\\\2\u091f\u0920\7\6\2\2\u0920\u0922\5\u00b6\\\2\u0921\u091f"
          + "\3\2\2\2\u0922\u0925\3\2\2\2\u0923\u0921\3\2\2\2\u0923\u0924\3\2\2\2\u0924"
          + "\u00b9\3\2\2\2\u0925\u0923\3\2\2\2\u0926\u0927\7\4\2\2\u0927\u092c\5\u00bc"
          + "_\2\u0928\u0929\7\6\2\2\u0929\u092b\5\u00bc_\2\u092a\u0928\3\2\2\2\u092b"
          + "\u092e\3\2\2\2\u092c\u092a\3\2\2\2\u092c\u092d\3\2\2\2\u092d\u092f\3\2"
          + "\2\2\u092e\u092c\3\2\2\2\u092f\u0930\7\5\2\2\u0930\u00bb\3\2\2\2\u0931"
          + "\u093f\5\u0100\u0081\2\u0932\u0933\5\u0106\u0084\2\u0933\u0934\7\4\2\2"
          + "\u0934\u0939\5\u00be`\2\u0935\u0936\7\6\2\2\u0936\u0938\5\u00be`\2\u0937"
          + "\u0935\3\2\2\2\u0938\u093b\3\2\2\2\u0939\u0937\3\2\2\2\u0939\u093a\3\2"
          + "\2\2\u093a\u093c\3\2\2\2\u093b\u0939\3\2\2\2\u093c\u093d\7\5\2\2\u093d"
          + "\u093f\3\2\2\2\u093e\u0931\3\2\2\2\u093e\u0932\3\2\2\2\u093f\u00bd\3\2"
          + "\2\2\u0940\u0943\5\u0100\u0081\2\u0941\u0943\5\u00caf\2\u0942\u0940\3"
          + "\2\2\2\u0942\u0941\3\2\2\2\u0943\u00bf\3\2\2\2\u0944\u0945\5\u00c2b\2"
          + "\u0945\u00c1\3\2\2\2\u0946\u0947\bb\1\2\u0947\u0948\7\u0098\2\2\u0948"
          + "\u0953\5\u00c2b\7\u0949\u094a\7U\2\2\u094a\u094b\7\4\2\2\u094b\u094c\5"
          + "\"\22\2\u094c\u094d\7\5\2\2\u094d\u0953\3\2\2\2\u094e\u0950\5\u00c6d\2"
          + "\u094f\u0951\5\u00c4c\2\u0950\u094f\3\2\2\2\u0950\u0951\3\2\2\2\u0951"
          + "\u0953\3\2\2\2\u0952\u0946\3\2\2\2\u0952\u0949\3\2\2\2\u0952\u094e\3\2"
          + "\2\2\u0953\u095c\3\2\2\2\u0954\u0955\f\4\2\2\u0955\u0956\7\23\2\2\u0956"
          + "\u095b\5\u00c2b\5\u0957\u0958\f\3\2\2\u0958\u0959\7\u00a0\2\2\u0959\u095b"
          + "\5\u00c2b\4\u095a\u0954\3\2\2\2\u095a\u0957\3\2\2\2\u095b\u095e\3\2\2"
          + "\2\u095c\u095a\3\2\2\2\u095c\u095d\3\2\2\2\u095d\u00c3\3\2\2\2\u095e\u095c"
          + "\3\2\2\2\u095f\u0961\7\u0098\2\2\u0960\u095f\3\2\2\2\u0960\u0961\3\2\2"
          + "\2\u0961\u0962\3\2\2\2\u0962\u0963\7\34\2\2\u0963\u0964\5\u00c6d\2\u0964"
          + "\u0965\7\23\2\2\u0965\u0966\5\u00c6d\2\u0966\u09b2\3\2\2\2\u0967\u0969"
          + "\7\u0098\2\2\u0968\u0967\3\2\2\2\u0968\u0969\3\2\2\2\u0969\u096a\3\2\2"
          + "\2\u096a\u096b\7r\2\2\u096b\u096c\7\4\2\2\u096c\u0971\5\u00c0a\2\u096d"
          + "\u096e\7\6\2\2\u096e\u0970\5\u00c0a\2\u096f\u096d\3\2\2\2\u0970\u0973"
          + "\3\2\2\2\u0971\u096f\3\2\2\2\u0971\u0972\3\2\2\2\u0972\u0974\3\2\2\2\u0973"
          + "\u0971\3\2\2\2\u0974\u0975\7\5\2\2\u0975\u09b2\3\2\2\2\u0976\u0978\7\u0098"
          + "\2\2\u0977\u0976\3\2\2\2\u0977\u0978\3\2\2\2\u0978\u0979\3\2\2\2\u0979"
          + "\u097a\7r\2\2\u097a\u097b\7\4\2\2\u097b\u097c\5\"\22\2\u097c\u097d\7\5"
          + "\2\2\u097d\u09b2\3\2\2\2\u097e\u0980\7\u0098\2\2\u097f\u097e\3\2\2\2\u097f"
          + "\u0980\3\2\2\2\u0980\u0981\3\2\2\2\u0981\u0982\7\u00c4\2\2\u0982\u09b2"
          + "\5\u00c6d\2\u0983\u0985\7\u0098\2\2\u0984\u0983\3\2\2\2\u0984\u0985\3"
          + "\2\2\2\u0985\u0986\3\2\2\2\u0986\u0987\7\u0085\2\2\u0987\u0995\t\26\2"
          + "\2\u0988\u0989\7\4\2\2\u0989\u0996\7\5\2\2\u098a\u098b\7\4\2\2\u098b\u0990"
          + "\5\u00c0a\2\u098c\u098d\7\6\2\2\u098d\u098f\5\u00c0a\2\u098e\u098c\3\2"
          + "\2\2\u098f\u0992\3\2\2\2\u0990\u098e\3\2\2\2\u0990\u0991\3\2\2\2\u0991"
          + "\u0993\3\2\2\2\u0992\u0990\3\2\2\2\u0993\u0994\7\5\2\2\u0994\u0996\3\2"
          + "\2\2\u0995\u0988\3\2\2\2\u0995\u098a\3\2\2\2\u0996\u09b2\3\2\2\2\u0997"
          + "\u0999\7\u0098\2\2\u0998\u0997\3\2\2\2\u0998\u0999\3\2\2\2\u0999\u099a"
          + "\3\2\2\2\u099a\u099b\7\u0085\2\2\u099b\u099e\5\u00c6d\2\u099c\u099d\7"
          + "Q\2\2\u099d\u099f\7\u0119\2\2\u099e\u099c\3\2\2\2\u099e\u099f\3\2\2\2"
          + "\u099f\u09b2\3\2\2\2\u09a0\u09a2\7|\2\2\u09a1\u09a3\7\u0098\2\2\u09a2"
          + "\u09a1\3\2\2\2\u09a2\u09a3\3\2\2\2\u09a3\u09a4\3\2\2\2\u09a4\u09b2\7\u0099"
          + "\2\2\u09a5\u09a7\7|\2\2\u09a6\u09a8\7\u0098\2\2\u09a7\u09a6\3\2\2\2\u09a7"
          + "\u09a8\3\2\2\2\u09a8\u09a9\3\2\2\2\u09a9\u09b2\t\27\2\2\u09aa\u09ac\7"
          + "|\2\2\u09ab\u09ad\7\u0098\2\2\u09ac\u09ab\3\2\2\2\u09ac\u09ad\3\2\2\2"
          + "\u09ad\u09ae\3\2\2\2\u09ae\u09af\7K\2\2\u09af\u09b0\7f\2\2\u09b0\u09b2"
          + "\5\u00c6d\2\u09b1\u0960\3\2\2\2\u09b1\u0968\3\2\2\2\u09b1\u0977\3\2\2"
          + "\2\u09b1\u097f\3\2\2\2\u09b1\u0984\3\2\2\2\u09b1\u0998\3\2\2\2\u09b1\u09a0"
          + "\3\2\2\2\u09b1\u09a5\3\2\2\2\u09b1\u09aa\3\2\2\2\u09b2\u00c5\3\2\2\2\u09b3"
          + "\u09b4\bd\1\2\u09b4\u09b8\5\u00c8e\2\u09b5\u09b6\t\30\2\2\u09b6\u09b8"
          + "\5\u00c6d\t\u09b7\u09b3\3\2\2\2\u09b7\u09b5\3\2\2\2\u09b8\u09ce\3\2\2"
          + "\2\u09b9\u09ba\f\b\2\2\u09ba\u09bb\t\31\2\2\u09bb\u09cd\5\u00c6d\t\u09bc"
          + "\u09bd\f\7\2\2\u09bd\u09be\t\32\2\2\u09be\u09cd\5\u00c6d\b\u09bf\u09c0"
          + "\f\6\2\2\u09c0\u09c1\7\u0115\2\2\u09c1\u09cd\5\u00c6d\7\u09c2\u09c3\f"
          + "\5\2\2\u09c3\u09c4\7\u0118\2\2\u09c4\u09cd\5\u00c6d\6\u09c5\u09c6\f\4"
          + "\2\2\u09c6\u09c7\7\u0116\2\2\u09c7\u09cd\5\u00c6d\5\u09c8\u09c9\f\3\2"
          + "\2\u09c9\u09ca\5\u00ccg\2\u09ca\u09cb\5\u00c6d\4\u09cb\u09cd\3\2\2\2\u09cc"
          + "\u09b9\3\2\2\2\u09cc\u09bc\3\2\2\2\u09cc\u09bf\3\2\2\2\u09cc\u09c2\3\2"
          + "\2\2\u09cc\u09c5\3\2\2\2\u09cc\u09c8\3\2\2\2\u09cd\u09d0\3\2\2\2\u09ce"
          + "\u09cc\3\2\2\2\u09ce\u09cf\3\2\2\2\u09cf\u00c7\3\2\2\2\u09d0\u09ce\3\2"
          + "\2\2\u09d1\u09d2\be\1\2\u09d2\u0a8a\t\33\2\2\u09d3\u09d5\7#\2\2\u09d4"
          + "\u09d6\5\u00f0y\2\u09d5\u09d4\3\2\2\2\u09d6\u09d7\3\2\2\2\u09d7\u09d5"
          + "\3\2\2\2\u09d7\u09d8\3\2\2\2\u09d8\u09db\3\2\2\2\u09d9\u09da\7O\2\2\u09da"
          + "\u09dc\5\u00c0a\2\u09db\u09d9\3\2\2\2\u09db\u09dc\3\2\2\2\u09dc\u09dd"
          + "\3\2\2\2\u09dd\u09de\7P\2\2\u09de\u0a8a\3\2\2\2\u09df\u09e0\7#\2\2\u09e0"
          + "\u09e2\5\u00c0a\2\u09e1\u09e3\5\u00f0y\2\u09e2\u09e1\3\2\2\2\u09e3\u09e4"
          + "\3\2\2\2\u09e4\u09e2\3\2\2\2\u09e4\u09e5\3\2\2\2\u09e5\u09e8\3\2\2\2\u09e6"
          + "\u09e7\7O\2\2\u09e7\u09e9\5\u00c0a\2\u09e8\u09e6\3\2\2\2\u09e8\u09e9\3"
          + "\2\2\2\u09e9\u09ea\3\2\2\2\u09ea\u09eb\7P\2\2\u09eb\u0a8a\3\2\2\2\u09ec"
          + "\u09ed\7$\2\2\u09ed\u09ee\7\4\2\2\u09ee\u09ef\5\u00c0a\2\u09ef\u09f0\7"
          + "\30\2\2\u09f0\u09f1\5\u00e2r\2\u09f1\u09f2\7\5\2\2\u09f2\u0a8a\3\2\2\2"
          + "\u09f3\u09f4\7\u00de\2\2\u09f4\u09fd\7\4\2\2\u09f5\u09fa\5\u00b6\\\2\u09f6"
          + "\u09f7\7\6\2\2\u09f7\u09f9\5\u00b6\\\2\u09f8\u09f6\3\2\2\2\u09f9\u09fc"
          + "\3\2\2\2\u09fa\u09f8\3\2\2\2\u09fa\u09fb\3\2\2\2\u09fb\u09fe\3\2\2\2\u09fc"
          + "\u09fa\3\2\2\2\u09fd\u09f5\3\2\2\2\u09fd\u09fe\3\2\2\2\u09fe\u09ff\3\2"
          + "\2\2\u09ff\u0a8a\7\5\2\2\u0a00\u0a01\7`\2\2\u0a01\u0a02\7\4\2\2\u0a02"
          + "\u0a05\5\u00c0a\2\u0a03\u0a04\7p\2\2\u0a04\u0a06\7\u009a\2\2\u0a05\u0a03"
          + "\3\2\2\2\u0a05\u0a06\3\2\2\2\u0a06\u0a07\3\2\2\2\u0a07\u0a08\7\5\2\2\u0a08"
          + "\u0a8a\3\2\2\2\u0a09\u0a0a\7\u0080\2\2\u0a0a\u0a0b\7\4\2\2\u0a0b\u0a0e"
          + "\5\u00c0a\2\u0a0c\u0a0d\7p\2\2\u0a0d\u0a0f\7\u009a\2\2\u0a0e\u0a0c\3\2"
          + "\2\2\u0a0e\u0a0f\3\2\2\2\u0a0f\u0a10\3\2\2\2\u0a10\u0a11\7\5\2\2\u0a11"
          + "\u0a8a\3\2\2\2\u0a12\u0a13\7\u00af\2\2\u0a13\u0a14\7\4\2\2\u0a14\u0a15"
          + "\5\u00c6d\2\u0a15\u0a16\7r\2\2\u0a16\u0a17\5\u00c6d\2\u0a17\u0a18\7\5"
          + "\2\2\u0a18\u0a8a\3\2\2\2\u0a19\u0a8a\5\u00caf\2\u0a1a\u0a8a\7\u0111\2"
          + "\2\u0a1b\u0a1c\5\u0100\u0081\2\u0a1c\u0a1d\7\7\2\2\u0a1d\u0a1e\7\u0111"
          + "\2\2\u0a1e\u0a8a\3\2\2\2\u0a1f\u0a20\7\4\2\2\u0a20\u0a23\5\u00b6\\\2\u0a21"
          + "\u0a22\7\6\2\2\u0a22\u0a24\5\u00b6\\\2\u0a23\u0a21\3\2\2\2\u0a24\u0a25"
          + "\3\2\2\2\u0a25\u0a23\3\2\2\2\u0a25\u0a26\3\2\2\2\u0a26\u0a27\3\2\2\2\u0a27"
          + "\u0a28\7\5\2\2\u0a28\u0a8a\3\2\2\2\u0a29\u0a2a\7\4\2\2\u0a2a\u0a2b\5\""
          + "\22\2\u0a2b\u0a2c\7\5\2\2\u0a2c\u0a8a\3\2\2\2\u0a2d\u0a2e\5\u00fe\u0080"
          + "\2\u0a2e\u0a3a\7\4\2\2\u0a2f\u0a31\5\u008aF\2\u0a30\u0a2f\3\2\2\2\u0a30"
          + "\u0a31\3\2\2\2\u0a31\u0a32\3\2\2\2\u0a32\u0a37\5\u00c0a\2\u0a33\u0a34"
          + "\7\6\2\2\u0a34\u0a36\5\u00c0a\2\u0a35\u0a33\3\2\2\2\u0a36\u0a39\3\2\2"
          + "\2\u0a37\u0a35\3\2\2\2\u0a37\u0a38\3\2\2\2\u0a38\u0a3b\3\2\2\2\u0a39\u0a37"
          + "\3\2\2\2\u0a3a\u0a30\3\2\2\2\u0a3a\u0a3b\3\2\2\2\u0a3b\u0a3c\3\2\2\2\u0a3c"
          + "\u0a43\7\5\2\2\u0a3d\u0a3e\7^\2\2\u0a3e\u0a3f\7\4\2\2\u0a3f\u0a40\7\u0103"
          + "\2\2\u0a40\u0a41\5\u00c2b\2\u0a41\u0a42\7\5\2\2\u0a42\u0a44\3\2\2\2\u0a43"
          + "\u0a3d\3\2\2\2\u0a43\u0a44\3\2\2\2\u0a44\u0a47\3\2\2\2\u0a45\u0a46\7\u00a5"
          + "\2\2\u0a46\u0a48\5\u00f6|\2\u0a47\u0a45\3\2\2\2\u0a47\u0a48\3\2\2\2\u0a48"
          + "\u0a8a\3\2\2\2\u0a49\u0a4a\5\u0106\u0084\2\u0a4a\u0a4b\7\n\2\2\u0a4b\u0a4c"
          + "\5\u00c0a\2\u0a4c\u0a8a\3\2\2\2\u0a4d\u0a4e\7\4\2\2\u0a4e\u0a51\5\u0106"
          + "\u0084\2\u0a4f\u0a50\7\6\2\2\u0a50\u0a52\5\u0106\u0084\2\u0a51\u0a4f\3"
          + "\2\2\2\u0a52\u0a53\3\2\2\2\u0a53\u0a51\3\2\2\2\u0a53\u0a54\3\2\2\2\u0a54"
          + "\u0a55\3\2\2\2\u0a55\u0a56\7\5\2\2\u0a56\u0a57\7\n\2\2\u0a57\u0a58\5\u00c0"
          + "a\2\u0a58\u0a8a\3\2\2\2\u0a59\u0a8a\5\u0106\u0084\2\u0a5a\u0a5b\7\4\2"
          + "\2\u0a5b\u0a5c\5\u00c0a\2\u0a5c\u0a5d\7\5\2\2\u0a5d\u0a8a\3\2\2\2\u0a5e"
          + "\u0a5f\7Z\2\2\u0a5f\u0a60\7\4\2\2\u0a60\u0a61\5\u0106\u0084\2\u0a61\u0a62"
          + "\7f\2\2\u0a62\u0a63\5\u00c6d\2\u0a63\u0a64\7\5\2\2\u0a64\u0a8a\3\2\2\2"
          + "\u0a65\u0a66\t\34\2\2\u0a66\u0a67\7\4\2\2\u0a67\u0a68\5\u00c6d\2\u0a68"
          + "\u0a69\t\35\2\2\u0a69\u0a6c\5\u00c6d\2\u0a6a\u0a6b\t\36\2\2\u0a6b\u0a6d"
          + "\5\u00c6d\2\u0a6c\u0a6a\3\2\2\2\u0a6c\u0a6d\3\2\2\2\u0a6d\u0a6e\3\2\2"
          + "\2\u0a6e\u0a6f\7\5\2\2\u0a6f\u0a8a\3\2\2\2\u0a70\u0a71\7\u00ef\2\2\u0a71"
          + "\u0a73\7\4\2\2\u0a72\u0a74\t\37\2\2\u0a73\u0a72\3\2\2\2\u0a73\u0a74\3"
          + "\2\2\2\u0a74\u0a76\3\2\2\2\u0a75\u0a77\5\u00c6d\2\u0a76\u0a75\3\2\2\2"
          + "\u0a76\u0a77\3\2\2\2\u0a77\u0a78\3\2\2\2\u0a78\u0a79\7f\2\2\u0a79\u0a7a"
          + "\5\u00c6d\2\u0a7a\u0a7b\7\5\2\2\u0a7b\u0a8a\3\2\2\2\u0a7c\u0a7d\7\u00a7"
          + "\2\2\u0a7d\u0a7e\7\4\2\2\u0a7e\u0a7f\5\u00c6d\2\u0a7f\u0a80\7\u00ae\2"
          + "\2\u0a80\u0a81\5\u00c6d\2\u0a81\u0a82\7f\2\2\u0a82\u0a85\5\u00c6d\2\u0a83"
          + "\u0a84\7b\2\2\u0a84\u0a86\5\u00c6d\2\u0a85\u0a83\3\2\2\2\u0a85\u0a86\3"
          + "\2\2\2\u0a86\u0a87\3\2\2\2\u0a87\u0a88\7\5\2\2\u0a88\u0a8a\3\2\2\2\u0a89"
          + "\u09d1\3\2\2\2\u0a89\u09d3\3\2\2\2\u0a89\u09df\3\2\2\2\u0a89\u09ec\3\2"
          + "\2\2\u0a89\u09f3\3\2\2\2\u0a89\u0a00\3\2\2\2\u0a89\u0a09\3\2\2\2\u0a89"
          + "\u0a12\3\2\2\2\u0a89\u0a19\3\2\2\2\u0a89\u0a1a\3\2\2\2\u0a89\u0a1b\3\2"
          + "\2\2\u0a89\u0a1f\3\2\2\2\u0a89\u0a29\3\2\2\2\u0a89\u0a2d\3\2\2\2\u0a89"
          + "\u0a49\3\2\2\2\u0a89\u0a4d\3\2\2\2\u0a89\u0a59\3\2\2\2\u0a89\u0a5a\3\2"
          + "\2\2\u0a89\u0a5e\3\2\2\2\u0a89\u0a65\3\2\2\2\u0a89\u0a70\3\2\2\2\u0a89"
          + "\u0a7c\3\2\2\2\u0a8a\u0a95\3\2\2\2\u0a8b\u0a8c\f\n\2\2\u0a8c\u0a8d\7\13"
          + "\2\2\u0a8d\u0a8e\5\u00c6d\2\u0a8e\u0a8f\7\f\2\2\u0a8f\u0a94\3\2\2\2\u0a90"
          + "\u0a91\f\b\2\2\u0a91\u0a92\7\7\2\2\u0a92\u0a94\5\u0106\u0084\2\u0a93\u0a8b"
          + "\3\2\2\2\u0a93\u0a90\3\2\2\2\u0a94\u0a97\3\2\2\2\u0a95\u0a93\3\2\2\2\u0a95"
          + "\u0a96\3\2\2\2\u0a96\u00c9\3\2\2\2\u0a97\u0a95\3\2\2\2\u0a98\u0aa5\7\u0099"
          + "\2\2\u0a99\u0aa5\5\u00d4k\2\u0a9a\u0a9b\5\u0106\u0084\2\u0a9b\u0a9c\7"
          + "\u0119\2\2\u0a9c\u0aa5\3\2\2\2\u0a9d\u0aa5\5\u010c\u0087\2\u0a9e\u0aa5"
          + "\5\u00d2j\2\u0a9f\u0aa1\7\u0119\2\2\u0aa0\u0a9f\3\2\2\2\u0aa1\u0aa2\3"
          + "\2\2\2\u0aa2\u0aa0\3\2\2\2\u0aa2\u0aa3\3\2\2\2\u0aa3\u0aa5\3\2\2\2\u0aa4"
          + "\u0a98\3\2\2\2\u0aa4\u0a99\3\2\2\2\u0aa4\u0a9a\3\2\2\2\u0aa4\u0a9d\3\2"
          + "\2\2\u0aa4\u0a9e\3\2\2\2\u0aa4\u0aa0\3\2\2\2\u0aa5\u00cb\3\2\2\2\u0aa6"
          + "\u0aa7\t \2\2\u0aa7\u00cd\3\2\2\2\u0aa8\u0aa9\t!\2\2\u0aa9\u00cf\3\2\2"
          + "\2\u0aaa\u0aab\t\"\2\2\u0aab\u00d1\3\2\2\2\u0aac\u0aad\t#\2\2\u0aad\u00d3"
          + "\3\2\2\2\u0aae\u0ab1\7z\2\2\u0aaf\u0ab2\5\u00d6l\2\u0ab0\u0ab2\5\u00da"
          + "n\2\u0ab1\u0aaf\3\2\2\2\u0ab1\u0ab0\3\2\2\2\u0ab1\u0ab2\3\2\2\2\u0ab2"
          + "\u00d5\3\2\2\2\u0ab3\u0ab5\5\u00d8m\2\u0ab4\u0ab6\5\u00dco\2\u0ab5\u0ab4"
          + "\3\2\2\2\u0ab5\u0ab6\3\2\2\2\u0ab6\u00d7\3\2\2\2\u0ab7\u0ab8\5\u00dep"
          + "\2\u0ab8\u0ab9\5\u0106\u0084\2\u0ab9\u0abb\3\2\2\2\u0aba\u0ab7\3\2\2\2"
          + "\u0abb\u0abc\3\2\2\2\u0abc\u0aba\3\2\2\2\u0abc\u0abd\3\2\2\2\u0abd\u00d9"
          + "\3\2\2\2\u0abe\u0ac1\5\u00dco\2\u0abf\u0ac2\5\u00d8m\2\u0ac0\u0ac2\5\u00dc"
          + "o\2\u0ac1\u0abf\3\2\2\2\u0ac1\u0ac0\3\2\2\2\u0ac1\u0ac2\3\2\2\2\u0ac2"
          + "\u00db\3\2\2\2\u0ac3\u0ac4\5\u00dep\2\u0ac4\u0ac5\5\u0106\u0084\2\u0ac5"
          + "\u0ac6\7\u00e9\2\2\u0ac6\u0ac7\5\u0106\u0084\2\u0ac7\u00dd\3\2\2\2\u0ac8"
          + "\u0aca\t$\2\2\u0ac9\u0ac8\3\2\2\2\u0ac9\u0aca\3\2\2\2\u0aca\u0acb\3\2"
          + "\2\2\u0acb\u0ace\t\25\2\2\u0acc\u0ace\7\u0119\2\2\u0acd\u0ac9\3\2\2\2"
          + "\u0acd\u0acc\3\2\2\2\u0ace\u00df\3\2\2\2\u0acf\u0ad3\7`\2\2\u0ad0\u0ad1"
          + "\7\17\2\2\u0ad1\u0ad3\5\u0102\u0082\2\u0ad2\u0acf\3\2\2\2\u0ad2\u0ad0"
          + "\3\2\2\2\u0ad3\u00e1\3\2\2\2\u0ad4\u0ad5\7\27\2\2\u0ad5\u0ad6\7\u010b"
          + "\2\2\u0ad6\u0ad7\5\u00e2r\2\u0ad7\u0ad8\7\u010d\2\2\u0ad8\u0af7\3\2\2"
          + "\2\u0ad9\u0ada\7\u0090\2\2\u0ada\u0adb\7\u010b\2\2\u0adb\u0adc\5\u00e2"
          + "r\2\u0adc\u0add\7\6\2\2\u0add\u0ade\5\u00e2r\2\u0ade\u0adf\7\u010d\2\2"
          + "\u0adf\u0af7\3\2\2\2\u0ae0\u0ae7\7\u00de\2\2\u0ae1\u0ae3\7\u010b\2\2\u0ae2"
          + "\u0ae4\5\u00ecw\2\u0ae3\u0ae2\3\2\2\2\u0ae3\u0ae4\3\2\2\2\u0ae4\u0ae5"
          + "\3\2\2\2\u0ae5\u0ae8\7\u010d\2\2\u0ae6\u0ae8\7\u0109\2\2\u0ae7\u0ae1\3"
          + "\2\2\2\u0ae7\u0ae6\3\2\2\2\u0ae8\u0af7\3\2\2\2\u0ae9\u0af4\5\u0106\u0084"
          + "\2\u0aea\u0aeb\7\4\2\2\u0aeb\u0af0\7\u011d\2\2\u0aec\u0aed\7\6\2\2\u0aed"
          + "\u0aef\7\u011d\2\2\u0aee\u0aec\3\2\2\2\u0aef\u0af2\3\2\2\2\u0af0\u0aee"
          + "\3\2\2\2\u0af0\u0af1\3\2\2\2\u0af1\u0af3\3\2\2\2\u0af2\u0af0\3\2\2\2\u0af3"
          + "\u0af5\7\5\2\2\u0af4\u0aea\3\2\2\2\u0af4\u0af5\3\2\2\2\u0af5\u0af7\3\2"
          + "\2\2\u0af6\u0ad4\3\2\2\2\u0af6\u0ad9\3\2\2\2\u0af6\u0ae0\3\2\2\2\u0af6"
          + "\u0ae9\3\2\2\2\u0af7\u00e3\3\2\2\2\u0af8\u0afd\5\u00e6t\2\u0af9\u0afa"
          + "\7\6\2\2\u0afa\u0afc\5\u00e6t\2\u0afb\u0af9\3\2\2\2\u0afc\u0aff\3\2\2"
          + "\2\u0afd\u0afb\3\2\2\2\u0afd\u0afe\3\2\2\2\u0afe\u00e5\3\2\2\2\u0aff\u0afd"
          + "\3\2\2\2\u0b00\u0b01\5\u00b0Y\2\u0b01\u0b04\5\u00e2r\2\u0b02\u0b03\7\u0098"
          + "\2\2\u0b03\u0b05\7\u0099\2\2\u0b04\u0b02\3\2\2\2\u0b04\u0b05\3\2\2\2\u0b05"
          + "\u0b07\3\2\2\2\u0b06\u0b08\5 \21\2\u0b07\u0b06\3\2\2\2\u0b07\u0b08\3\2"
          + "\2\2\u0b08\u0b0a\3\2\2\2\u0b09\u0b0b\5\u00e0q\2\u0b0a\u0b09\3\2\2\2\u0b0a"
          + "\u0b0b\3\2\2\2\u0b0b\u00e7\3\2\2\2\u0b0c\u0b11\5\u00eav\2\u0b0d\u0b0e"
          + "\7\6\2\2\u0b0e\u0b10\5\u00eav\2\u0b0f\u0b0d\3\2\2\2\u0b10\u0b13\3\2\2"
          + "\2\u0b11\u0b0f\3\2\2\2\u0b11\u0b12\3\2\2\2\u0b12\u00e9\3\2\2\2\u0b13\u0b11"
          + "\3\2\2\2\u0b14\u0b15\5\u0102\u0082\2\u0b15\u0b18\5\u00e2r\2\u0b16\u0b17"
          + "\7\u0098\2\2\u0b17\u0b19\7\u0099\2\2\u0b18\u0b16\3\2\2\2\u0b18\u0b19\3"
          + "\2\2\2\u0b19\u0b1b\3\2\2\2\u0b1a\u0b1c\5 \21\2\u0b1b\u0b1a\3\2\2\2\u0b1b"
          + "\u0b1c\3\2\2\2\u0b1c\u00eb\3\2\2\2\u0b1d\u0b22\5\u00eex\2\u0b1e\u0b1f"
          + "\7\6\2\2\u0b1f\u0b21\5\u00eex\2\u0b20\u0b1e\3\2\2\2\u0b21\u0b24\3\2\2"
          + "\2\u0b22\u0b20\3\2\2\2\u0b22\u0b23\3\2\2\2\u0b23\u00ed\3\2\2\2\u0b24\u0b22"
          + "\3\2\2\2\u0b25\u0b26\5\u0106\u0084\2\u0b26\u0b27\7\r\2\2\u0b27\u0b2a\5"
          + "\u00e2r\2\u0b28\u0b29\7\u0098\2\2\u0b29\u0b2b\7\u0099\2\2\u0b2a\u0b28"
          + "\3\2\2\2\u0b2a\u0b2b\3\2\2\2\u0b2b\u0b2d\3\2\2\2\u0b2c\u0b2e\5 \21\2\u0b2d"
          + "\u0b2c\3\2\2\2\u0b2d\u0b2e\3\2\2\2\u0b2e\u00ef\3\2\2\2\u0b2f\u0b30\7\u0102"
          + "\2\2\u0b30\u0b31\5\u00c0a\2\u0b31\u0b32\7\u00e7\2\2\u0b32\u0b33\5\u00c0"
          + "a\2\u0b33\u00f1\3\2\2\2\u0b34\u0b35\7\u0104\2\2\u0b35\u0b3a\5\u00f4{\2"
          + "\u0b36\u0b37\7\6\2\2\u0b37\u0b39\5\u00f4{\2\u0b38\u0b36\3\2\2\2\u0b39"
          + "\u0b3c\3\2\2\2\u0b3a\u0b38\3\2\2\2\u0b3a\u0b3b\3\2\2\2\u0b3b\u00f3\3\2"
          + "\2\2\u0b3c\u0b3a\3\2\2\2\u0b3d\u0b3e\5\u0102\u0082\2\u0b3e\u0b3f\7\30"
          + "\2\2\u0b3f\u0b40\5\u00f6|\2\u0b40\u00f5\3\2\2\2\u0b41\u0b70\5\u0102\u0082"
          + "\2\u0b42\u0b43\7\4\2\2\u0b43\u0b44\5\u0102\u0082\2\u0b44\u0b45\7\5\2\2"
          + "\u0b45\u0b70\3\2\2\2\u0b46\u0b69\7\4\2\2\u0b47\u0b48\7(\2\2\u0b48\u0b49"
          + "\7 \2\2\u0b49\u0b4e\5\u00c0a\2\u0b4a\u0b4b\7\6\2\2\u0b4b\u0b4d\5\u00c0"
          + "a\2\u0b4c\u0b4a\3\2\2\2\u0b4d\u0b50\3\2\2\2\u0b4e\u0b4c\3\2\2\2\u0b4e"
          + "\u0b4f\3\2\2\2\u0b4f\u0b6a\3\2\2\2\u0b50\u0b4e\3\2\2\2\u0b51\u0b52\t%"
          + "\2\2\u0b52\u0b53\7 \2\2\u0b53\u0b58\5\u00c0a\2\u0b54\u0b55\7\6\2\2\u0b55"
          + "\u0b57\5\u00c0a\2\u0b56\u0b54\3\2\2\2\u0b57\u0b5a\3\2\2\2\u0b58\u0b56"
          + "\3\2\2\2\u0b58\u0b59\3\2\2\2\u0b59\u0b5c\3\2\2\2\u0b5a\u0b58\3\2\2\2\u0b5b"
          + "\u0b51\3\2\2\2\u0b5b\u0b5c\3\2\2\2\u0b5c\u0b67\3\2\2\2\u0b5d\u0b5e\t&"
          + "\2\2\u0b5e\u0b5f\7 \2\2\u0b5f\u0b64\5Z.\2\u0b60\u0b61\7\6\2\2\u0b61\u0b63"
          + "\5Z.\2\u0b62\u0b60\3\2\2\2\u0b63\u0b66\3\2\2\2\u0b64\u0b62\3\2\2\2\u0b64"
          + "\u0b65\3\2\2\2\u0b65\u0b68\3\2\2\2\u0b66\u0b64\3\2\2\2\u0b67\u0b5d\3\2"
          + "\2\2\u0b67\u0b68\3\2\2\2\u0b68\u0b6a\3\2\2\2\u0b69\u0b47\3\2\2\2\u0b69"
          + "\u0b5b\3\2\2\2\u0b6a\u0b6c\3\2\2\2\u0b6b\u0b6d\5\u00f8}\2\u0b6c\u0b6b"
          + "\3\2\2\2\u0b6c\u0b6d\3\2\2\2\u0b6d\u0b6e\3\2\2\2\u0b6e\u0b70\7\5\2\2\u0b6f"
          + "\u0b41\3\2\2\2\u0b6f\u0b42\3\2\2\2\u0b6f\u0b46\3\2\2\2\u0b70\u00f7\3\2"
          + "\2\2\u0b71\u0b72\7\u00b6\2\2\u0b72\u0b82\5\u00fa~\2\u0b73\u0b74\7\u00ca"
          + "\2\2\u0b74\u0b82\5\u00fa~\2\u0b75\u0b76\7\u00b6\2\2\u0b76\u0b77\7\34\2"
          + "\2\u0b77\u0b78\5\u00fa~\2\u0b78\u0b79\7\23\2\2\u0b79\u0b7a\5\u00fa~\2"
          + "\u0b7a\u0b82\3\2\2\2\u0b7b\u0b7c\7\u00ca\2\2\u0b7c\u0b7d\7\34\2\2\u0b7d"
          + "\u0b7e\5\u00fa~\2\u0b7e\u0b7f\7\23\2\2\u0b7f\u0b80\5\u00fa~\2\u0b80\u0b82"
          + "\3\2\2\2\u0b81\u0b71\3\2\2\2\u0b81\u0b73\3\2\2\2\u0b81\u0b75\3\2\2\2\u0b81"
          + "\u0b7b\3\2\2\2\u0b82\u00f9\3\2\2\2\u0b83\u0b84\7\u00f4\2\2\u0b84\u0b8b"
          + "\t\'\2\2\u0b85\u0b86\7:\2\2\u0b86\u0b8b\7\u00c9\2\2\u0b87\u0b88\5\u00c0"
          + "a\2\u0b88\u0b89\t\'\2\2\u0b89\u0b8b\3\2\2\2\u0b8a\u0b83\3\2\2\2\u0b8a"
          + "\u0b85\3\2\2\2\u0b8a\u0b87\3\2\2\2\u0b8b\u00fb\3\2\2\2\u0b8c\u0b91\5\u0100"
          + "\u0081\2\u0b8d\u0b8e\7\6\2\2\u0b8e\u0b90\5\u0100\u0081\2\u0b8f\u0b8d\3"
          + "\2\2\2\u0b90\u0b93\3\2\2\2\u0b91\u0b8f\3\2\2\2\u0b91\u0b92\3\2\2\2\u0b92"
          + "\u00fd\3\2\2\2\u0b93\u0b91\3\2\2\2\u0b94\u0b99\5\u0100\u0081\2\u0b95\u0b99"
          + "\7^\2\2\u0b96\u0b99\7\u0084\2\2\u0b97\u0b99\7\u00c3\2\2\u0b98\u0b94\3"
          + "\2\2\2\u0b98\u0b95\3\2\2\2\u0b98\u0b96\3\2\2\2\u0b98\u0b97\3\2\2\2\u0b99"
          + "\u00ff\3\2\2\2\u0b9a\u0b9f\5\u0106\u0084\2\u0b9b\u0b9c\7\7\2\2\u0b9c\u0b9e"
          + "\5\u0106\u0084\2\u0b9d\u0b9b\3\2\2\2\u0b9e\u0ba1\3\2\2\2\u0b9f\u0b9d\3"
          + "\2\2\2\u0b9f\u0ba0\3\2\2\2\u0ba0\u0101\3\2\2\2\u0ba1\u0b9f\3\2\2\2\u0ba2"
          + "\u0ba3\5\u0106\u0084\2\u0ba3\u0ba4\5\u0104\u0083\2\u0ba4\u0103\3\2\2\2"
          + "\u0ba5\u0ba6\7\u0110\2\2\u0ba6\u0ba8\5\u0106\u0084\2\u0ba7\u0ba5\3\2\2"
          + "\2\u0ba8\u0ba9\3\2\2\2\u0ba9\u0ba7\3\2\2\2\u0ba9\u0baa\3\2\2\2\u0baa\u0bad"
          + "\3\2\2\2\u0bab\u0bad\3\2\2\2\u0bac\u0ba7\3\2\2\2\u0bac\u0bab\3\2\2\2\u0bad"
          + "\u0105\3\2\2\2\u0bae\u0bb2\5\u0108\u0085\2\u0baf\u0bb0\6\u0084\22\2\u0bb0"
          + "\u0bb2\5\u0112\u008a\2\u0bb1\u0bae\3\2\2\2\u0bb1\u0baf\3\2\2\2\u0bb2\u0107"
          + "\3\2\2\2\u0bb3\u0bba\7\u0123\2\2\u0bb4\u0bba\5\u010a\u0086\2\u0bb5\u0bb6"
          + "\6\u0085\23\2\u0bb6\u0bba\5\u0110\u0089\2\u0bb7\u0bb8\6\u0085\24\2\u0bb8"
          + "\u0bba\5\u0114\u008b\2\u0bb9\u0bb3\3\2\2\2\u0bb9\u0bb4\3\2\2\2\u0bb9\u0bb5"
          + "\3\2\2\2\u0bb9\u0bb7\3\2\2\2\u0bba\u0109\3\2\2\2\u0bbb\u0bbc\7\u0124\2"
          + "\2\u0bbc\u010b\3\2\2\2\u0bbd\u0bbf\6\u0087\25\2\u0bbe\u0bc0\7\u0110\2"
          + "\2\u0bbf\u0bbe\3\2\2\2\u0bbf\u0bc0\3\2\2\2\u0bc0\u0bc1\3\2\2\2\u0bc1\u0be9"
          + "\7\u011e\2\2\u0bc2\u0bc4\6\u0087\26\2\u0bc3\u0bc5\7\u0110\2\2\u0bc4\u0bc3"
          + "\3\2\2\2\u0bc4\u0bc5\3\2\2\2\u0bc5\u0bc6\3\2\2\2\u0bc6\u0be9\7\u011f\2"
          + "\2\u0bc7\u0bc9\6\u0087\27\2\u0bc8\u0bca\7\u0110\2\2\u0bc9\u0bc8\3\2\2"
          + "\2\u0bc9\u0bca\3\2\2\2\u0bca\u0bcb\3\2\2\2\u0bcb\u0be9\t(\2\2\u0bcc\u0bce"
          + "\7\u0110\2\2\u0bcd\u0bcc\3\2\2\2\u0bcd\u0bce\3\2\2\2\u0bce\u0bcf\3\2\2"
          + "\2\u0bcf\u0be9\7\u011d\2\2\u0bd0\u0bd2\7\u0110\2\2\u0bd1\u0bd0\3\2\2\2"
          + "\u0bd1\u0bd2\3\2\2\2\u0bd2\u0bd3\3\2\2\2\u0bd3\u0be9\7\u011a\2\2\u0bd4"
          + "\u0bd6\7\u0110\2\2\u0bd5\u0bd4\3\2\2\2\u0bd5\u0bd6\3\2\2\2\u0bd6\u0bd7"
          + "\3\2\2\2\u0bd7\u0be9\7\u011b\2\2\u0bd8\u0bda\7\u0110\2\2\u0bd9\u0bd8\3"
          + "\2\2\2\u0bd9\u0bda\3\2\2\2\u0bda\u0bdb\3\2\2\2\u0bdb\u0be9\7\u011c\2\2"
          + "\u0bdc\u0bde\7\u0110\2\2\u0bdd\u0bdc\3\2\2\2\u0bdd\u0bde\3\2\2\2\u0bde"
          + "\u0bdf\3\2\2\2\u0bdf\u0be9\7\u0121\2\2\u0be0\u0be2\7\u0110\2\2\u0be1\u0be0"
          + "\3\2\2\2\u0be1\u0be2\3\2\2\2\u0be2\u0be3\3\2\2\2\u0be3\u0be9\7\u0120\2"
          + "\2\u0be4\u0be6\7\u0110\2\2\u0be5\u0be4\3\2\2\2\u0be5\u0be6\3\2\2\2\u0be6"
          + "\u0be7\3\2\2\2\u0be7\u0be9\7\u0122\2\2\u0be8\u0bbd\3\2\2\2\u0be8\u0bc2"
          + "\3\2\2\2\u0be8\u0bc7\3\2\2\2\u0be8\u0bcd\3\2\2\2\u0be8\u0bd1\3\2\2\2\u0be8"
          + "\u0bd5\3\2\2\2\u0be8\u0bd9\3\2\2\2\u0be8\u0bdd\3\2\2\2\u0be8\u0be1\3\2"
          + "\2\2\u0be8\u0be5\3\2\2\2\u0be9\u010d\3\2\2\2\u0bea\u0beb\7\u00f2\2\2\u0beb"
          + "\u0bf2\5\u00e2r\2\u0bec\u0bf2\5 \21\2\u0bed\u0bf2\5\u00e0q\2\u0bee\u0bef"
          + "\t)\2\2\u0bef\u0bf0\7\u0098\2\2\u0bf0\u0bf2\7\u0099\2\2\u0bf1\u0bea\3"
          + "\2\2\2\u0bf1\u0bec\3\2\2\2\u0bf1\u0bed\3\2\2\2\u0bf1\u0bee\3\2\2\2\u0bf2"
          + "\u010f\3\2\2\2\u0bf3\u0bf4\t*\2\2\u0bf4\u0111\3\2\2\2\u0bf5\u0bf6\t+\2"
          + "\2\u0bf6\u0113\3\2\2\2\u0bf7\u0bf8\t,\2\2\u0bf8\u0115\3\2\2\2\u018f\u011a"
          + "\u0133\u0138\u0140\u0148\u014a\u015e\u0162\u0168\u016b\u016e\u0175\u017a"
          + "\u017d\u0184\u0190\u0199\u019b\u019f\u01a2\u01a9\u01b4\u01b6\u01be\u01c3"
          + "\u01c6\u01cc\u01d7\u0217\u0220\u0224\u022a\u022e\u0233\u0239\u0245\u024d"
          + "\u0253\u0260\u0265\u0275\u027c\u0280\u0286\u0295\u0299\u029f\u02a5\u02a8"
          + "\u02ab\u02b1\u02b5\u02bd\u02bf\u02c8\u02cb\u02d4\u02d9\u02df\u02e6\u02e9"
          + "\u02ef\u02fa\u02fd\u0301\u0306\u030b\u0312\u0315\u0318\u031f\u0324\u032d"
          + "\u0335\u033b\u033e\u0341\u0347\u034b\u034f\u0353\u0355\u035d\u0365\u036b"
          + "\u0371\u0374\u0378\u037b\u037f\u039b\u039e\u03a2\u03a8\u03ab\u03ae\u03b4"
          + "\u03bc\u03c1\u03c7\u03cd\u03d9\u03dc\u03e3\u03f4\u03fd\u0400\u0406\u040f"
          + "\u0416\u0419\u0423\u0427\u042e\u04a2\u04aa\u04b2\u04bb\u04c5\u04c9\u04cc"
          + "\u04d2\u04d8\u04e4\u04f0\u04f5\u04fe\u0506\u050d\u050f\u0514\u0518\u051d"
          + "\u0522\u0527\u052a\u052f\u0533\u0538\u053a\u053e\u0547\u054f\u0558\u055f"
          + "\u0568\u056d\u0570\u0583\u0585\u058e\u0595\u0598\u059f\u05a3\u05a9\u05b1"
          + "\u05bc\u05c7\u05ce\u05d4\u05e1\u05e8\u05ef\u05fb\u0603\u0609\u060c\u0618"
          + "\u0620\u0626\u0630\u0633\u063c\u063f\u0648\u064b\u0654\u0657\u065a\u065f"
          + "\u0661\u066d\u0674\u067b\u067e\u0680\u068c\u0690\u0694\u069a\u069e\u06a6"
          + "\u06aa\u06ad\u06b0\u06b3\u06b7\u06bb\u06be\u06c2\u06c7\u06cb\u06ce\u06d1"
          + "\u06d4\u06d6\u06e2\u06e5\u06e9\u06f3\u06f7\u06f9\u06fc\u0700\u0706\u070a"
          + "\u0715\u071f\u072b\u073a\u073f\u0746\u0756\u075b\u0768\u076d\u0775\u077b"
          + "\u077f\u0788\u0797\u079c\u07a8\u07ad\u07b5\u07b8\u07bc\u07ca\u07d7\u07dc"
          + "\u07e0\u07e3\u07e8\u07f1\u07f4\u07f9\u0800\u0803\u080b\u0812\u0819\u081c"
          + "\u0821\u0824\u0829\u082d\u0830\u0833\u0839\u083e\u0843\u0855\u0857\u085a"
          + "\u0865\u086e\u0875\u087d\u0884\u0888\u0890\u0898\u089e\u08a6\u08b2\u08b5"
          + "\u08bb\u08bf\u08c1\u08ca\u08d6\u08d8\u08df\u08e6\u08ec\u08f2\u08f4\u08fb"
          + "\u0903\u0909\u0910\u0916\u091a\u091c\u0923\u092c\u0939\u093e\u0942\u0950"
          + "\u0952\u095a\u095c\u0960\u0968\u0971\u0977\u097f\u0984\u0990\u0995\u0998"
          + "\u099e\u09a2\u09a7\u09ac\u09b1\u09b7\u09cc\u09ce\u09d7\u09db\u09e4\u09e8"
          + "\u09fa\u09fd\u0a05\u0a0e\u0a25\u0a30\u0a37\u0a3a\u0a43\u0a47\u0a53\u0a6c"
          + "\u0a73\u0a76\u0a85\u0a89\u0a93\u0a95\u0aa2\u0aa4\u0ab1\u0ab5\u0abc\u0ac1"
          + "\u0ac9\u0acd\u0ad2\u0ae3\u0ae7\u0af0\u0af4\u0af6\u0afd\u0b04\u0b07\u0b0a"
          + "\u0b11\u0b18\u0b1b\u0b22\u0b2a\u0b2d\u0b3a\u0b4e\u0b58\u0b5b\u0b64\u0b67"
          + "\u0b69\u0b6c\u0b6f\u0b81\u0b8a\u0b91\u0b98\u0b9f\u0ba9\u0bac\u0bb1\u0bb9"
          + "\u0bbf\u0bc4\u0bc9\u0bcd\u0bd1\u0bd5\u0bd9\u0bdd\u0be1\u0be5\u0be8\u0bf1";
  public static final String _serializedATN =
      Utils.join(new String[] { _serializedATNSegment0, _serializedATNSegment1 }, "");
  public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());

  static {
    _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
    for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
      _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
    }
  }
}