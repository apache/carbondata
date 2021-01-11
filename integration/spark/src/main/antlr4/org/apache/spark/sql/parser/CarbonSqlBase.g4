/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/**
 * Code ported from Apache Spark
 * spark/sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4
 */
grammar CarbonSqlBase;

@parser::members {
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
}

@lexer::members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }

  /**
   * This method will be called when we see '/*' and try to match it as a bracketed comment.
   * If the next character is '+', it should be parsed as hint later, and we cannot match
   * it as a bracketed comment.
   *
   * Returns true if the next character is '+'.
   */
  public boolean isHint() {
    int nextChar = _input.LA(1);
    if (nextChar == '+') {
      return true;
    } else {
      return false;
    }
  }
}

singleStatement
    : statement ';'* EOF
    ;

singleExpression
    : namedExpression EOF
    ;

singleTableIdentifier
    : tableIdentifier EOF
    ;

singleMultipartIdentifier
    : multipartIdentifier EOF
    ;

singleFunctionIdentifier
    : functionIdentifier EOF
    ;

statement
    : query                                                            #statementDefault
    | ctes? dmlStatementNoWith                                         #dmlStatement
    ;

query
    : ctes? queryTerm queryOrganization
    ;

ctes
    : WITH namedQuery (',' namedQuery)*
    ;

namedQuery
    : name=errorCapturingIdentifier (columnAliases=identifierList)? AS? '(' query ')'
    ;

constantList
    : '(' constant (',' constant)* ')'
    ;

nestedConstantList
    : '(' constantList (',' constantList)* ')'
    ;

resource
    : identifier STRING
    ;

dmlStatementNoWith
    : DELETE FROM multipartIdentifier tableAlias whereClause?                      #deleteFromTable
    | UPDATE multipartIdentifier tableAlias setClause whereClause?                 #updateTable
    | MERGE INTO target=multipartIdentifier targetAlias=tableAlias
        USING (source=multipartIdentifier |
          '(' sourceQuery=query')') sourceAlias=tableAlias
        ON mergeCondition=booleanExpression
        matchedClause*
        notMatchedClause*                                                          #mergeIntoTable
    ;

mergeInto
    : MERGE INTO target=multipartIdentifier targetAlias=tableAlias
              USING (source=multipartIdentifier |
                '(' sourceQuery=query')') sourceAlias=tableAlias
              ON mergeCondition=booleanExpression
              matchedClause*
              notMatchedClause*
    ;

queryOrganization
    : (LIMIT (limit=expression))?
    ;

queryTerm
    : queryPrimary                                                                       #queryTermDefault
    ;

queryPrimary
    : querySpecification                                                    #queryPrimaryDefault
    | '(' query ')'                                                         #subquery
    ;

fromStatementBody
    : selectClause
      whereClause?
      havingClause?
      queryOrganization
    ;

querySpecification
    : selectClause
      fromClause?
      whereClause?
      havingClause?
    ;

selectClause
    : SELECT (hints+=hint)* setQuantifier? namedExpressionSeq
    ;

setClause
    : SET assignmentList
    ;

matchedClause
    : WHEN MATCHED (AND matchedCond=booleanExpression)? THEN matchedAction
    ;
notMatchedClause
    : WHEN NOT MATCHED (AND notMatchedCond=booleanExpression)? THEN notMatchedAction
    ;

matchedAction
    : DELETE
    | UPDATE SET ASTERISK
    | UPDATE SET assignmentList
    ;

notMatchedAction
    : INSERT ASTERISK
    | INSERT '(' columns=multipartIdentifierList ')'
        VALUES '(' expression (',' expression)* ')'
    ;

assignmentList
    : assignment (',' assignment)*
    ;

assignment
    : key=multipartIdentifier EQ value=expression
    ;

whereClause
    : WHERE booleanExpression
    ;

havingClause
    : HAVING booleanExpression
    ;

hint
    : '/*+' hintStatements+=hintStatement (','? hintStatements+=hintStatement)* '*/'
    ;

hintStatement
    : hintName=identifier
    | hintName=identifier '(' parameters+=primaryExpression (',' parameters+=primaryExpression)* ')'
    ;

fromClause
    : FROM relation (',' relation)*
    ;

setQuantifier
    : DISTINCT
    ;

relation
    : relationPrimary
    ;

identifierList
    : '(' identifierSeq ')'
    ;

identifierSeq
    : ident+=errorCapturingIdentifier (',' ident+=errorCapturingIdentifier)*
    ;

relationPrimary
    : multipartIdentifier  #tableName
    | inlineTable                             #inlineTableDefault2
    | functionTable                           #tableValuedFunction
    ;

inlineTable
    : VALUES expression (',' expression)* tableAlias
    ;

functionTable
    : funcName=errorCapturingIdentifier '(' (expression (',' expression)*)? ')' tableAlias
    ;

tableAlias
    : (AS? strictIdentifier identifierList?)?
    ;

multipartIdentifierList
    : multipartIdentifier (',' multipartIdentifier)*
    ;

multipartIdentifier
    : parts+=errorCapturingIdentifier ('.' parts+=errorCapturingIdentifier)*
    ;

tableIdentifier
    : (db=errorCapturingIdentifier '.')? table=errorCapturingIdentifier
    ;

functionIdentifier
    : (db=errorCapturingIdentifier '.')? function=errorCapturingIdentifier
    ;

namedExpression
    : expression (AS? (name=errorCapturingIdentifier | identifierList))?
    ;

namedExpressionSeq
    : namedExpression (',' namedExpression)*
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                        #logicalNot
    | valueExpression predicate?                                   #predicated
    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
    ;

predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | IS NOT? kind=NULL
    | IS NOT? kind=(TRUE | FALSE)
    | IS NOT? kind=DISTINCT FROM right=valueExpression
    ;

valueExpression
    : primaryExpression                                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS | TILDE) valueExpression                                        #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT | DIV) right=valueExpression #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS | CONCAT_PIPE) right=valueExpression       #arithmeticBinary
    | left=valueExpression operator=AMPERSAND right=valueExpression                          #arithmeticBinary
    | left=valueExpression operator=HAT right=valueExpression                                #arithmeticBinary
    | left=valueExpression operator=PIPE right=valueExpression                               #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                          #comparison
    ;

primaryExpression
    : name=(CURRENT_DATE | CURRENT_TIMESTAMP)                                                  #currentDatetime
    | constant                                                                                 #constantDefault
    | ASTERISK                                                                                 #star
    | qualifiedName '.' ASTERISK                                                               #star
    | '(' namedExpression (',' namedExpression)+ ')'                                           #rowConstructor
    | '(' query ')'                                                                            #subqueryExpression
    | identifier '->' expression                                                               #lambda
    | '(' identifier (',' identifier)+ ')' '->' expression                                     #lambda
    | value=primaryExpression '[' index=valueExpression ']'                                    #subscript
    | identifier                                                                               #columnReference
    | base=primaryExpression '.' fieldName=identifier                                          #dereference
    | '(' expression ')'                                                                       #parenthesizedExpression
    ;

constant
    : NULL                                                                                     #nullLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | NEQJ | LT | LTE | GT | GTE | NSEQ
    ;

booleanValue
    : TRUE | FALSE
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

errorCapturingIdentifier
    : identifier errorCapturingIdentifierExtra
    ;

// extra left-factoring grammar
errorCapturingIdentifierExtra
    : (MINUS identifier)+    #errorIdent
    |                        #realIdent
    ;

identifier
    : strictIdentifier
    | {!SQL_standard_keyword_behavior}? strictNonReserved
    ;

strictIdentifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | {SQL_standard_keyword_behavior}? ansiNonReserved #unquotedIdentifier
    | {!SQL_standard_keyword_behavior}? nonReserved    #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

number
    : {!legacy_exponent_literal_as_decimal_enabled}? MINUS? EXPONENT_VALUE #exponentLiteral
    | {!legacy_exponent_literal_as_decimal_enabled}? MINUS? DECIMAL_VALUE  #decimalLiteral
    | {legacy_exponent_literal_as_decimal_enabled}? MINUS? (EXPONENT_VALUE | DECIMAL_VALUE) #legacyDecimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

ansiNonReserved
//--ANSI-NON-RESERVED-START
    : BETWEEN
    | CURRENT
    | DELETE
    | DIV
    | IF
    | INSERT
    | LIKE
    | LIMIT
    | MATCHED
    | MERGE
    | NO
    | NULLS
    | OF
    | SET
    | SETMINUS
    | SETS
    | SORT
    | SORTED
    | TRUE
    | UPDATE
    | VALUES
//--ANSI-NON-RESERVED-END
    ;

strictNonReserved
    : JOIN
    | LEFT
    | ON
    | RIGHT
    | SEMI
    | SETMINUS
    | UNION
    | USING
    ;

nonReserved
    : AND
    | AS
    | BETWEEN
    | CURRENT
    | CURRENT_DATE
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURRENT_USER
    | DELETE
    | DISTINCT
    | DIV
    | ELSE
    | END
    | FALSE
    | FROM
    | IN
    | INSERT
    | INTO
    | IS
    | LIKE
    | LIMIT
    | MATCHED
    | MERGE
    | NO
    | NOT
    | NULL
    | NULLS
    | OF
    | OR
    | ORDER
    | SELECT
    | SET
    | SETS
    | SORT
    | SORTED
    | THEN
    | TIME
    | TO
    | TRUE
    | UPDATE
    | VALUES
    | WHEN
    | WHERE
//--DEFAULT-NON-RESERVED-END
    ;

//============================
// Start of the keywords list
//============================
AND: 'AND';
AS: 'AS';
BETWEEN: 'BETWEEN';
CURRENT: 'CURRENT';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIME: 'CURRENT_TIME';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DELETE: 'DELETE';
DISTINCT: 'DISTINCT';
DIV: 'DIV';
ELSE: 'ELSE';
END: 'END';
FALSE: 'FALSE';
FROM: 'FROM';
HAVING: 'HAVING';
IF: 'IF';
IN: 'IN';
INSERT: 'INSERT';
INTO: 'INTO';
IS: 'IS';
JOIN: 'JOIN';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
MATCHED: 'MATCHED';
MERGE: 'MERGE';
NO: 'NO';
NOT: 'NOT' | '!';
NULL: 'NULL';
NULLS: 'NULLS';
OF: 'OF';
ON: 'ON';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
RIGHT: 'RIGHT';
SELECT: 'SELECT';
SEMI: 'SEMI';
SET: 'SET';
SETMINUS: 'MINUS';
SETS: 'SETS';
SORT: 'SORT';
SORTED: 'SORTED';
THEN: 'THEN';
TIME: 'TIME';
TO: 'TO';
TRUE: 'TRUE';
UNION: 'UNION';
UPDATE: 'UPDATE';
USING: 'USING';
VALUES: 'VALUES';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';

EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LT  : '<';
LTE : '<=' | '!>';
GT  : '>';
GTE : '>=' | '!<';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
TILDE: '~';
AMPERSAND: '&';
PIPE: '|';
CONCAT_PIPE: '||';
HAT: '^';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
