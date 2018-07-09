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
 * copy from SqlBase.g4 of Presto and Spark.
 */

grammar Expression;

parseFilter
 : booleanExpression EOF
 ;

booleanExpression
    : predicate
    | left=booleanExpression operator=AND right=booleanExpression
    | left=booleanExpression operator=OR right=booleanExpression
    | '(' booleanExpression ')'
    ;

predicate
    : left=primaryExpression comparisonOperator right=primaryExpression
    | left=primaryExpression NOT? BETWEEN lower=primaryExpression AND upper=primaryExpression
    | left=primaryExpression NOT? IN '(' primaryExpression (',' primaryExpression)* ')'
    | left=primaryExpression IS NOT? NULL
    ;

primaryExpression
    : constant                                           #constantDefault
    | identifier                                         #columnReference
    | base=identifier '.' fieldName=identifier           #dereference
    | '(' booleanExpression ')'                          #parenthesizedExpression
    ;

constant
    : NULL                                               #nullLiteral
    | number                                             #numericLiteral
    | booleanValue                                       #booleanLiteral
    | STRING+                                            #stringLiteral
    ;

identifier
    : IDENTIFIER                                         #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER                              #backQuotedIdentifier
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? DECIMAL_VALUE                               #decimalLiteral
    | MINUS? INTEGER_VALUE                               #integerLiteral
    | MINUS? BIGINT_LITERAL                              #bigIntLiteral
    | MINUS? SMALLINT_LITERAL                            #smallIntLiteral
    | MINUS? TINYINT_LITERAL                             #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL                              #doubleLiteral
    | MINUS? BIGDECIMAL_LITERAL                          #bigDecimalLiteral
    ;

AND: 'AND';
BETWEEN: 'BETWEEN';
FALSE: 'FALSE';
IN: 'IN';
IS: 'IS';
NOT: 'NOT';
NULL: 'NULL';
OR: 'OR';
TRUE: 'TRUE';

EQ  : '=';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

MINUS: '-';

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

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D'
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD'
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

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;