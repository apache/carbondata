// Generated from /home/david/Documents/code/carbondata/store/horizon/src/main/anltr/Expression.g4 by ANTLR 4.7
package org.apache.carbondata.horizon.antlr.gen;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ExpressionLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, AND=5, BETWEEN=6, FALSE=7, IN=8, IS=9, 
		NOT=10, NULL=11, OR=12, TRUE=13, EQ=14, NEQ=15, LT=16, LTE=17, GT=18, 
		GTE=19, MINUS=20, STRING=21, BIGINT_LITERAL=22, SMALLINT_LITERAL=23, TINYINT_LITERAL=24, 
		INTEGER_VALUE=25, DECIMAL_VALUE=26, DOUBLE_LITERAL=27, BIGDECIMAL_LITERAL=28, 
		IDENTIFIER=29, BACKQUOTED_IDENTIFIER=30, WS=31, UNRECOGNIZED=32;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "AND", "BETWEEN", "FALSE", "IN", "IS", 
		"NOT", "NULL", "OR", "TRUE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "MINUS", 
		"STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE", 
		"DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "DECIMAL_DIGITS", "EXPONENT", "DIGIT", "LETTER", 
		"WS", "UNRECOGNIZED"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "','", "'.'", "'AND'", "'BETWEEN'", "'FALSE'", "'IN'", 
		"'IS'", "'NOT'", "'NULL'", "'OR'", "'TRUE'", "'='", null, "'<'", "'<='", 
		"'>'", "'>='", "'-'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, "AND", "BETWEEN", "FALSE", "IN", "IS", "NOT", 
		"NULL", "OR", "TRUE", "EQ", "NEQ", "LT", "LTE", "GT", "GTE", "MINUS", 
		"STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE", 
		"DECIMAL_VALUE", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER", 
		"BACKQUOTED_IDENTIFIER", "WS", "UNRECOGNIZED"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
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


	public ExpressionLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "Expression.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\"\u012f\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6"+
		"\3\6\3\6\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\7\3\b\3\b\3\b\3\b\3\b\3\b\3\t\3"+
		"\t\3\t\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\r\3\r\3\r"+
		"\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\20\3\20\5\20\u0083\n\20"+
		"\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\24\3\24\3\24\3\25\3\25\3\26\3\26"+
		"\3\26\3\26\7\26\u0095\n\26\f\26\16\26\u0098\13\26\3\26\3\26\3\26\3\26"+
		"\3\26\7\26\u009f\n\26\f\26\16\26\u00a2\13\26\3\26\5\26\u00a5\n\26\3\27"+
		"\6\27\u00a8\n\27\r\27\16\27\u00a9\3\27\3\27\3\30\6\30\u00af\n\30\r\30"+
		"\16\30\u00b0\3\30\3\30\3\31\6\31\u00b6\n\31\r\31\16\31\u00b7\3\31\3\31"+
		"\3\32\6\32\u00bd\n\32\r\32\16\32\u00be\3\33\6\33\u00c2\n\33\r\33\16\33"+
		"\u00c3\3\33\3\33\3\33\3\33\5\33\u00ca\n\33\5\33\u00cc\n\33\3\34\6\34\u00cf"+
		"\n\34\r\34\16\34\u00d0\3\34\5\34\u00d4\n\34\3\34\3\34\3\34\3\34\5\34\u00da"+
		"\n\34\3\34\3\34\5\34\u00de\n\34\3\35\6\35\u00e1\n\35\r\35\16\35\u00e2"+
		"\3\35\5\35\u00e6\n\35\3\35\3\35\3\35\3\35\3\35\5\35\u00ed\n\35\3\35\3"+
		"\35\3\35\5\35\u00f2\n\35\3\36\3\36\3\36\6\36\u00f7\n\36\r\36\16\36\u00f8"+
		"\3\37\3\37\3\37\3\37\7\37\u00ff\n\37\f\37\16\37\u0102\13\37\3\37\3\37"+
		"\3 \6 \u0107\n \r \16 \u0108\3 \3 \7 \u010d\n \f \16 \u0110\13 \3 \3 "+
		"\6 \u0114\n \r \16 \u0115\5 \u0118\n \3!\3!\5!\u011c\n!\3!\6!\u011f\n"+
		"!\r!\16!\u0120\3\"\3\"\3#\3#\3$\6$\u0128\n$\r$\16$\u0129\3$\3$\3%\3%\2"+
		"\2&\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35"+
		"\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36"+
		";\37= ?\2A\2C\2E\2G!I\"\3\2\t\4\2))^^\4\2$$^^\3\2bb\4\2--//\3\2\62;\3"+
		"\2C\\\5\2\13\f\17\17\"\"\2\u014b\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2"+
		"\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2"+
		"\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2"+
		"\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2"+
		"\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2"+
		"\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2"+
		"\3K\3\2\2\2\5M\3\2\2\2\7O\3\2\2\2\tQ\3\2\2\2\13S\3\2\2\2\rW\3\2\2\2\17"+
		"_\3\2\2\2\21e\3\2\2\2\23h\3\2\2\2\25k\3\2\2\2\27o\3\2\2\2\31t\3\2\2\2"+
		"\33w\3\2\2\2\35|\3\2\2\2\37\u0082\3\2\2\2!\u0084\3\2\2\2#\u0086\3\2\2"+
		"\2%\u0089\3\2\2\2\'\u008b\3\2\2\2)\u008e\3\2\2\2+\u00a4\3\2\2\2-\u00a7"+
		"\3\2\2\2/\u00ae\3\2\2\2\61\u00b5\3\2\2\2\63\u00bc\3\2\2\2\65\u00cb\3\2"+
		"\2\2\67\u00dd\3\2\2\29\u00f1\3\2\2\2;\u00f6\3\2\2\2=\u00fa\3\2\2\2?\u0117"+
		"\3\2\2\2A\u0119\3\2\2\2C\u0122\3\2\2\2E\u0124\3\2\2\2G\u0127\3\2\2\2I"+
		"\u012d\3\2\2\2KL\7*\2\2L\4\3\2\2\2MN\7+\2\2N\6\3\2\2\2OP\7.\2\2P\b\3\2"+
		"\2\2QR\7\60\2\2R\n\3\2\2\2ST\7C\2\2TU\7P\2\2UV\7F\2\2V\f\3\2\2\2WX\7D"+
		"\2\2XY\7G\2\2YZ\7V\2\2Z[\7Y\2\2[\\\7G\2\2\\]\7G\2\2]^\7P\2\2^\16\3\2\2"+
		"\2_`\7H\2\2`a\7C\2\2ab\7N\2\2bc\7U\2\2cd\7G\2\2d\20\3\2\2\2ef\7K\2\2f"+
		"g\7P\2\2g\22\3\2\2\2hi\7K\2\2ij\7U\2\2j\24\3\2\2\2kl\7P\2\2lm\7Q\2\2m"+
		"n\7V\2\2n\26\3\2\2\2op\7P\2\2pq\7W\2\2qr\7N\2\2rs\7N\2\2s\30\3\2\2\2t"+
		"u\7Q\2\2uv\7T\2\2v\32\3\2\2\2wx\7V\2\2xy\7T\2\2yz\7W\2\2z{\7G\2\2{\34"+
		"\3\2\2\2|}\7?\2\2}\36\3\2\2\2~\177\7>\2\2\177\u0083\7@\2\2\u0080\u0081"+
		"\7#\2\2\u0081\u0083\7?\2\2\u0082~\3\2\2\2\u0082\u0080\3\2\2\2\u0083 \3"+
		"\2\2\2\u0084\u0085\7>\2\2\u0085\"\3\2\2\2\u0086\u0087\7>\2\2\u0087\u0088"+
		"\7?\2\2\u0088$\3\2\2\2\u0089\u008a\7@\2\2\u008a&\3\2\2\2\u008b\u008c\7"+
		"@\2\2\u008c\u008d\7?\2\2\u008d(\3\2\2\2\u008e\u008f\7/\2\2\u008f*\3\2"+
		"\2\2\u0090\u0096\7)\2\2\u0091\u0095\n\2\2\2\u0092\u0093\7^\2\2\u0093\u0095"+
		"\13\2\2\2\u0094\u0091\3\2\2\2\u0094\u0092\3\2\2\2\u0095\u0098\3\2\2\2"+
		"\u0096\u0094\3\2\2\2\u0096\u0097\3\2\2\2\u0097\u0099\3\2\2\2\u0098\u0096"+
		"\3\2\2\2\u0099\u00a5\7)\2\2\u009a\u00a0\7$\2\2\u009b\u009f\n\3\2\2\u009c"+
		"\u009d\7^\2\2\u009d\u009f\13\2\2\2\u009e\u009b\3\2\2\2\u009e\u009c\3\2"+
		"\2\2\u009f\u00a2\3\2\2\2\u00a0\u009e\3\2\2\2\u00a0\u00a1\3\2\2\2\u00a1"+
		"\u00a3\3\2\2\2\u00a2\u00a0\3\2\2\2\u00a3\u00a5\7$\2\2\u00a4\u0090\3\2"+
		"\2\2\u00a4\u009a\3\2\2\2\u00a5,\3\2\2\2\u00a6\u00a8\5C\"\2\u00a7\u00a6"+
		"\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00a7\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa"+
		"\u00ab\3\2\2\2\u00ab\u00ac\7N\2\2\u00ac.\3\2\2\2\u00ad\u00af\5C\"\2\u00ae"+
		"\u00ad\3\2\2\2\u00af\u00b0\3\2\2\2\u00b0\u00ae\3\2\2\2\u00b0\u00b1\3\2"+
		"\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b3\7U\2\2\u00b3\60\3\2\2\2\u00b4\u00b6"+
		"\5C\"\2\u00b5\u00b4\3\2\2\2\u00b6\u00b7\3\2\2\2\u00b7\u00b5\3\2\2\2\u00b7"+
		"\u00b8\3\2\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00ba\7[\2\2\u00ba\62\3\2\2\2"+
		"\u00bb\u00bd\5C\"\2\u00bc\u00bb\3\2\2\2\u00bd\u00be\3\2\2\2\u00be\u00bc"+
		"\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\64\3\2\2\2\u00c0\u00c2\5C\"\2\u00c1"+
		"\u00c0\3\2\2\2\u00c2\u00c3\3\2\2\2\u00c3\u00c1\3\2\2\2\u00c3\u00c4\3\2"+
		"\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00c6\5A!\2\u00c6\u00cc\3\2\2\2\u00c7\u00c9"+
		"\5? \2\u00c8\u00ca\5A!\2\u00c9\u00c8\3\2\2\2\u00c9\u00ca\3\2\2\2\u00ca"+
		"\u00cc\3\2\2\2\u00cb\u00c1\3\2\2\2\u00cb\u00c7\3\2\2\2\u00cc\66\3\2\2"+
		"\2\u00cd\u00cf\5C\"\2\u00ce\u00cd\3\2\2\2\u00cf\u00d0\3\2\2\2\u00d0\u00ce"+
		"\3\2\2\2\u00d0\u00d1\3\2\2\2\u00d1\u00d3\3\2\2\2\u00d2\u00d4\5A!\2\u00d3"+
		"\u00d2\3\2\2\2\u00d3\u00d4\3\2\2\2\u00d4\u00d5\3\2\2\2\u00d5\u00d6\7F"+
		"\2\2\u00d6\u00de\3\2\2\2\u00d7\u00d9\5? \2\u00d8\u00da\5A!\2\u00d9\u00d8"+
		"\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00db\3\2\2\2\u00db\u00dc\7F\2\2\u00dc"+
		"\u00de\3\2\2\2\u00dd\u00ce\3\2\2\2\u00dd\u00d7\3\2\2\2\u00de8\3\2\2\2"+
		"\u00df\u00e1\5C\"\2\u00e0\u00df\3\2\2\2\u00e1\u00e2\3\2\2\2\u00e2\u00e0"+
		"\3\2\2\2\u00e2\u00e3\3\2\2\2\u00e3\u00e5\3\2\2\2\u00e4\u00e6\5A!\2\u00e5"+
		"\u00e4\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00e7\3\2\2\2\u00e7\u00e8\7D"+
		"\2\2\u00e8\u00e9\7F\2\2\u00e9\u00f2\3\2\2\2\u00ea\u00ec\5? \2\u00eb\u00ed"+
		"\5A!\2\u00ec\u00eb\3\2\2\2\u00ec\u00ed\3\2\2\2\u00ed\u00ee\3\2\2\2\u00ee"+
		"\u00ef\7D\2\2\u00ef\u00f0\7F\2\2\u00f0\u00f2\3\2\2\2\u00f1\u00e0\3\2\2"+
		"\2\u00f1\u00ea\3\2\2\2\u00f2:\3\2\2\2\u00f3\u00f7\5E#\2\u00f4\u00f7\5"+
		"C\"\2\u00f5\u00f7\7a\2\2\u00f6\u00f3\3\2\2\2\u00f6\u00f4\3\2\2\2\u00f6"+
		"\u00f5\3\2\2\2\u00f7\u00f8\3\2\2\2\u00f8\u00f6\3\2\2\2\u00f8\u00f9\3\2"+
		"\2\2\u00f9<\3\2\2\2\u00fa\u0100\7b\2\2\u00fb\u00ff\n\4\2\2\u00fc\u00fd"+
		"\7b\2\2\u00fd\u00ff\7b\2\2\u00fe\u00fb\3\2\2\2\u00fe\u00fc\3\2\2\2\u00ff"+
		"\u0102\3\2\2\2\u0100\u00fe\3\2\2\2\u0100\u0101\3\2\2\2\u0101\u0103\3\2"+
		"\2\2\u0102\u0100\3\2\2\2\u0103\u0104\7b\2\2\u0104>\3\2\2\2\u0105\u0107"+
		"\5C\"\2\u0106\u0105\3\2\2\2\u0107\u0108\3\2\2\2\u0108\u0106\3\2\2\2\u0108"+
		"\u0109\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010e\7\60\2\2\u010b\u010d\5"+
		"C\"\2\u010c\u010b\3\2\2\2\u010d\u0110\3\2\2\2\u010e\u010c\3\2\2\2\u010e"+
		"\u010f\3\2\2\2\u010f\u0118\3\2\2\2\u0110\u010e\3\2\2\2\u0111\u0113\7\60"+
		"\2\2\u0112\u0114\5C\"\2\u0113\u0112\3\2\2\2\u0114\u0115\3\2\2\2\u0115"+
		"\u0113\3\2\2\2\u0115\u0116\3\2\2\2\u0116\u0118\3\2\2\2\u0117\u0106\3\2"+
		"\2\2\u0117\u0111\3\2\2\2\u0118@\3\2\2\2\u0119\u011b\7G\2\2\u011a\u011c"+
		"\t\5\2\2\u011b\u011a\3\2\2\2\u011b\u011c\3\2\2\2\u011c\u011e\3\2\2\2\u011d"+
		"\u011f\5C\"\2\u011e\u011d\3\2\2\2\u011f\u0120\3\2\2\2\u0120\u011e\3\2"+
		"\2\2\u0120\u0121\3\2\2\2\u0121B\3\2\2\2\u0122\u0123\t\6\2\2\u0123D\3\2"+
		"\2\2\u0124\u0125\t\7\2\2\u0125F\3\2\2\2\u0126\u0128\t\b\2\2\u0127\u0126"+
		"\3\2\2\2\u0128\u0129\3\2\2\2\u0129\u0127\3\2\2\2\u0129\u012a\3\2\2\2\u012a"+
		"\u012b\3\2\2\2\u012b\u012c\b$\2\2\u012cH\3\2\2\2\u012d\u012e\13\2\2\2"+
		"\u012eJ\3\2\2\2#\2\u0082\u0094\u0096\u009e\u00a0\u00a4\u00a9\u00b0\u00b7"+
		"\u00be\u00c3\u00c9\u00cb\u00d0\u00d3\u00d9\u00dd\u00e2\u00e5\u00ec\u00f1"+
		"\u00f6\u00f8\u00fe\u0100\u0108\u010e\u0115\u0117\u011b\u0120\u0129\3\2"+
		"\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}