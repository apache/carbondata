/*
// $Id: //open/mondrian/src/main/mondrian/tui/CmdRunner.java#59 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */

package com.huawei.unibi.molap.engine.mondriantest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import mondrian.olap.Category;
import mondrian.olap.Connection;
import mondrian.olap.Cube;
import mondrian.olap.Dimension;
import mondrian.olap.DriverManager;
import mondrian.olap.FunTable;
import mondrian.olap.Hierarchy;
import mondrian.olap.Member;
import mondrian.olap.MondrianProperties;
import mondrian.olap.OlapElement;
import mondrian.olap.Parameter;
import mondrian.olap.Query;
import mondrian.olap.Result;
import mondrian.olap.Util;
import mondrian.olap.fun.FunInfo;
import mondrian.olap.type.TypeUtil;
import mondrian.rolap.RolapConnectionProperties;
import mondrian.rolap.RolapCube;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.eigenbase.util.property.Property;

/**
 * Command line utility which reads and executes MDX commands.
 * 
 * <p>
 * TODO: describe how to use this class.
 * </p>
 * 
 * @author Richard Emberson
 * @version $Id: //open/mondrian/src/main/mondrian/tui/CmdRunner.java#59 $
 */
public class MultiUserTest
{

    /**
     * 
     */
    private static final String nl = Util.nl;

    /**
     * 
     */
    private static boolean RELOAD_CONNECTION = true;

    /**
     * 
     */
    private static String CATALOG_NAME = "FoodMart";

    /**
     * 
     */
    private static final Map<Object, String> paraNameValues = new HashMap<Object, String>();

    /**
     * 
     */
    private static String[][] commentDelim;

    /**
     * 
     */
    private static char[] commentStartChars;

    /**
     * 
     */
    private static boolean allowNestedComments;

    /**
     * 
     */
    private final Options options;

    /**
     * 
     */
    private long queryTime;

    /**
     * 
     */
    private long totalQueryTime;

    /**
     * 
     */
    private String filename;

    /**
     * 
     */
    private String mdxCmd;

    /**
     * 
     */
    private String mdxResult;

    /**
     * 
     */
    private String error;

    /**
     * 
     */
    private String stack;

    /**
     * 
     */
    private String connectString;

    /**
     * 
     */
    private Connection connection;

    /**
     * 
     */
    private final PrintWriter out;

    static
    {
        setDefaultCommentState();
    }

    /**
     * Creates a <code>CmdRunner</code>.
     * 
     * @param options
     *            Option set, or null to use default options
     * @param out
     *            Output writer, or null to use {@link System#out}.
     */
    public MultiUserTest(Options options, PrintWriter out)
    {
        if(options == null)
        {
            options = new Options();
        }
        this.options = options;
        this.filename = null;
        this.mdxResult = null;
        this.error = null;
        this.queryTime = -1;
        if(out == null)
        {
            out = new PrintWriter(System.out);
        }
        this.out = out;
    }

    /**
     * @param timeQueries
     */
    public void setTimeQueries(boolean timeQueries)
    {
        this.options.timeQueries = timeQueries;
    }

    /**
     * @return
     */
    public boolean getTimeQueries()
    {
        return options.timeQueries;
    }

    /**
     * @return
     */
    public long getQueryTime()
    {
        return queryTime;
    }

    /**
     * @return
     */
    public long getTotalQueryTime()
    {
        return totalQueryTime;
    }

    /**
     * 
     */
    public void noCubeCaching()
    {
        Cube[] cubes = getCubes();
        for(Cube cube : cubes)
        {
            RolapCube rcube = (RolapCube)cube;
            rcube.setCacheAggregations(false);
        }
    }

    void setError(String s)
    {
        this.error = s;
    }

    void setError(Throwable t)
    {
        this.error = formatError(t);
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        this.stack = sw.toString();
    }

    void clearError()
    {
        this.error = null;
        this.stack = null;
    }

    private String formatError(Throwable mex)
    {
        String message = mex.getMessage();
        if(message == null)
        {
            message = mex.toString();
        }
        if(mex.getCause() != null && mex.getCause() != mex)
        {
            message = message + nl + formatError(mex.getCause());
        }
        return message;
    }

    /**
     * @param buf
     */
    public static void listPropertyNames(StringBuilder buf)
    {
        PropertyInfo propertyInfo = new PropertyInfo(MondrianProperties.instance());
        for(int i = 0;i < propertyInfo.size();i++)
        {
            buf.append(propertyInfo.getProperty(i).getPath());
            buf.append(nl);
        }
    }

    /**
     * @param buf
     */
    public static void listPropertiesAll(StringBuilder buf)
    {
        PropertyInfo propertyInfo = new PropertyInfo(MondrianProperties.instance());
        for(int i = 0;i < propertyInfo.size();i++)
        {
            String propertyName = propertyInfo.getPropertyName(i);
            String propertyValue = propertyInfo.getProperty(i).getString();
            buf.append(propertyName);
            buf.append('=');
            buf.append(propertyValue);
            buf.append(nl);
        }
    }

    /**
     * Returns the value of a property, or null if it is not set.
     */
    private static String getPropertyValue(String propertyName)
    {
        final Property property = PropertyInfo.lookupProperty(MondrianProperties.instance(), propertyName);
        return property.isSet() ? property.getString() : null;
    }

    /**
     * @param propertyName
     * @param buf
     */
    public static void listProperty(String propertyName, StringBuilder buf)
    {
        buf.append(getPropertyValue(propertyName));
    }

    /**
     * @param propertyName
     * @return
     */
    public static boolean isProperty(String propertyName)
    {
        final Property property = PropertyInfo.lookupProperty(MondrianProperties.instance(), propertyName);
        return property != null;
    }

    /**
     * @param name
     * @param value
     * @return
     */
    public static boolean setProperty(String name, String value)
    {
        final Property property = PropertyInfo.lookupProperty(MondrianProperties.instance(), name);
        String oldValue = property.getString();
        if(!Util.equals(oldValue, value))
        {
            property.setString(value);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * @param query
     */
    public void loadParameters(Query query)
    {
        Parameter[] params = query.getParameters();
        for(Parameter param : params)
        {
            loadParameter(query, param);
        }
    }

    /**
     * Looks up the definition of a property with a given name.
     */
    private static class PropertyInfo
    {
        /**
         * 
         */
        private final List<Property> propertyList = new ArrayList<Property>();

        /**
         * 
         */
        private final List<String> propertyNameList = new ArrayList<String>();

        PropertyInfo(MondrianProperties properties)
        {
            final Class<? extends Object> clazz = properties.getClass();
            final Field[] fields = clazz.getFields();
            for(Field field : fields)
            {
                if(!Modifier.isPublic(field.getModifiers()) || Modifier.isStatic(field.getModifiers())
                        || !Property.class.isAssignableFrom(field.getType()))
                {
                    continue;
                }
                final Property property;
                try
                {
                    property = (Property)field.get(properties);
                }
                catch(IllegalAccessException e)
                {
                    continue;
                }
                propertyList.add(property);
                propertyNameList.add(field.getName());
            }
        }

        /**
         * @return
         */
        public int size()
        {
            return propertyList.size();
        }

        /**
         * @param i
         * @return
         */
        public Property getProperty(int i)
        {
            return propertyList.get(i);
        }

        /**
         * @param i
         * @return
         */
        public String getPropertyName(int i)
        {
            return propertyNameList.get(i);
        }

        /**
         * Looks up the definition of a property with a given name.
         */
        public static Property lookupProperty(MondrianProperties properties, String propertyName)
        {
            final Class<? extends Object> clazz = properties.getClass();
            final Field field;
            try
            {
                field = clazz.getField(propertyName);
            }
            catch(NoSuchFieldException e)
            {
                return null;
            }
            //
            if(!Modifier.isPublic(field.getModifiers()) || Modifier.isStatic(field.getModifiers())
                    || !Property.class.isAssignableFrom(field.getType()))
            {
                return null;
            }
            //
            try
            {
                return (Property)field.get(properties);
            }
            catch(IllegalAccessException e)
            {
                return null;
            }
        }
    }

    /**    
     */
    private static class Expr
    {
        /**
         
         */
        enum Type {
            /**
             * 
             */
            STRING, /**
             * 
             */
            NUMERIC, /**
             * 
             */
            MEMBER
        }

        /**
         * 
         */
        final Object value;

        /**
         * 
         */
        final Type type;

        Expr(Object value, Type type)
        {
            this.value = value;
            this.type = type;
        }
    }

    /**
     * @param query
     * @param param
     */
    public void loadParameter(Query query, Parameter param)
    {
        int category = TypeUtil.typeToCategory(param.getType());
        String name = param.getName();
        String value = MultiUserTest.paraNameValues.get(name);
        debug("loadParameter: name=" + name + ", value=" + value);
        if(value == null)
        {
            return;
        }
        //
        Expr expr = parseParameter(value);
        if(expr == null)
        {
            return;
        }
        Expr.Type type = expr.type;
        // found the parameter with the given name in the query
        switch(category)
        {
        case Category.Numeric:
            if(type != Expr.Type.NUMERIC)
            {
                String msg = "For parameter named \"" + name + "\" of Catetory.Numeric, " + "the value was type \""
                        + type + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;
        //
        case Category.String:
            if(type != Expr.Type.STRING)
            {
                String msg = "For parameter named \"" + name + "\" of Catetory.String, " + "the value was type \""
                        + type + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;
        //
        case Category.Member:
            if(type != Expr.Type.MEMBER)
            {
                String msg = "For parameter named \"" + name + "\" of Catetory.Member, " + "the value was type \""
                        + type + "\"";
                throw new IllegalArgumentException(msg);
            }
            break;
        //
        default:
            throw Util.newInternal("unexpected category " + category);
        }
        query.setParameter(param.getName(), String.valueOf(expr.value));
    }

    /**
     * 
     */
    static NumberFormat nf = NumberFormat.getInstance();

    // this is taken from JPivot
    public Expr parseParameter(String value)
    {
        // is it a String (enclose in double or single quotes ?
        String trimmed = value.trim();
        int len = trimmed.length();
        if(trimmed.charAt(0) == '"' && trimmed.charAt(len - 1) == '"')
        {
            debug("parseParameter. STRING_TYPE: " + trimmed);
            return new Expr(trimmed.substring(1, trimmed.length() - 1), Expr.Type.STRING);
        }
        if(trimmed.charAt(0) == '\'' && trimmed.charAt(len - 1) == '\'')
        {
            debug("parseParameter. STRING_TYPE: " + trimmed);
            return new Expr(trimmed.substring(1, trimmed.length() - 1), Expr.Type.STRING);
        }

        // is it a Number ?
        Number number = null;
        try
        {
            number = nf.parse(trimmed);
        }
        catch(ParseException pex)
        {
            out.println(pex.getMessage());
        }
        if(number != null)
        {
            debug("parseParameter. NUMERIC_TYPE: " + number);
            return new Expr(number, Expr.Type.NUMERIC);
        }

        debug("parseParameter. MEMBER_TYPE: " + trimmed);
        Query query = this.connection.parseQuery(this.mdxCmd);
        // dont have to execute
        // this.connection.execute(query);

        // assume member, dimension, hierarchy, level
        OlapElement element = Util.lookup(query, Util.parseIdentifier(trimmed));

        debug("parseParameter. exp=" + ((element == null) ? "null" : element.getClass().getName()));

        if(element instanceof Member)
        {
            Member member = (Member)element;
            return new Expr(member, Expr.Type.MEMBER);
        }
        else if(element instanceof mondrian.olap.Level)
        {
            mondrian.olap.Level level = (mondrian.olap.Level)element;
            return new Expr(level, Expr.Type.MEMBER);
        }
        else if(element instanceof Hierarchy)
        {
            Hierarchy hier = (Hierarchy)element;
            return new Expr(hier, Expr.Type.MEMBER);
        }
        else if(element instanceof Dimension)
        {
            Dimension dim = (Dimension)element;
            return new Expr(dim, Expr.Type.MEMBER);
        }
        return null;
    }

    public static void listParameterNameValues(StringBuilder buf)
    {
        for(Map.Entry<Object, String> e : MultiUserTest.paraNameValues.entrySet())
        {
            buf.append(e.getKey());
            buf.append('=');
            buf.append(e.getValue());
            buf.append(nl);
        }
    }

    public static void listParam(String name, StringBuilder buf)
    {
        String v = MultiUserTest.paraNameValues.get(name);
        buf.append(v);
    }

    public static boolean isParam(String name)
    {
        String v = MultiUserTest.paraNameValues.get(name);
        return (v != null);
    }

    public static void setParameter(String name, String value)
    {
        if(name == null)
        {
            MultiUserTest.paraNameValues.clear();
        }
        else
        {
            if(value == null)
            {
                MultiUserTest.paraNameValues.remove(name);
            }
            else
            {
                MultiUserTest.paraNameValues.put(name, value);
            }
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // cubes
    //
    public Cube[] getCubes()
    {
        Connection conn = getConnection();
        return conn.getSchemaReader().withLocus().getCubes();
    }

    public Cube getCube(String name)
    {
        Cube[] cubes = getCubes();
        for(Cube cube : cubes)
        {
            if(cube.getName().equals(name))
            {
                return cube;
            }
        }
        return null;
    }

    public void listCubeName(StringBuilder buf)
    {
        Cube[] cubes = getCubes();
        for(Cube cube : cubes)
        {
            buf.append(cube.getName());
            buf.append(nl);
        }
    }

    public void listCubeAttribues(String name, StringBuilder buf)
    {
        Cube cube = getCube(name);
        //
        if(cube == null)
        {
            buf.append("No cube found with name \"");
            buf.append(name);
            buf.append("\"");
        }
        else
        {
            //
            RolapCube rcube = (RolapCube)cube;
            buf.append("facttable=");
            buf.append(rcube.getStar().getFactTable().getAlias());
            buf.append(nl);
            buf.append("caching=");
            buf.append(rcube.isCacheAggregations());
            buf.append(nl);
        }
    }

    public void executeCubeCommand(String cubename, String command, StringBuilder buf)
    {
        Cube cube = getCube(cubename);
        //
        if(cube == null)
        {
            buf.append("No cube found with name \"");
            buf.append(cubename);
            buf.append("\"");
        }
        else
        {
            //
            if("clearCache".equals(command))
            {
                RolapCube rcube = (RolapCube)cube;
                rcube.clearCachedAggregations();
            }
            else
            {
                //
                buf.append("For cube \"");
                buf.append(cubename);
                buf.append("\" there is no command \"");
                buf.append(command);
                buf.append("\"");
            }
        }
    }

    public void setCubeAttribute(String cubename, String name, String value, StringBuilder buf)
    {
        Cube cube = getCube(cubename);
        //
        if(cube == null)
        {
            buf.append("No cube found with name \"");
            buf.append(cubename);
            buf.append("\"");
        }
        else
        {
            //
            if("caching".equals(name))
            {
                RolapCube rcube = (RolapCube)cube;
                boolean isCache = Boolean.valueOf(value);
                rcube.setCacheAggregations(isCache);
            }
            else
            {
                //
                buf.append("For cube \"");
                buf.append(cubename);
                buf.append("\" there is no attribute \"");
                buf.append(name);
                buf.append("\"");
            }
        }
    }

    //
    // ///////////////////////////////////////////////////////////////////////

    /**
     * Executes a query and returns the result as a string.
     * 
     * @param queryString
     *            MDX query text
     * @return result String
     */
    public String execute(String queryString)
    {
        long t1 = System.currentTimeMillis();
        Result result = runQuery(queryString, true);
        out.println("Total : " + (System.currentTimeMillis() - t1));
        // System.out.println("Actual Total : "
        // + HbaseMultiDimMolapExecutor.totalTime);
        if(this.options.highCardResults)
        {
            return highCardToString(result);
        }
        else
        {
            return toString(result);
        }
    }

    /**
     * Executes a query and returns the result.
     * 
     * @param queryString
     *            MDX query text
     * @return a {@link Result} object
     */
    public Result runQuery(String queryString, boolean loadParams)
    {
        debug("CmdRunner.runQuery: TOP");
        Result result = null;
        long start = System.currentTimeMillis();
        try
        {
            //
            this.connection = getConnection();
            debug("CmdRunner.runQuery: AFTER getConnection");
            Query query = this.connection.parseQuery(queryString);
            debug("CmdRunner.runQuery: AFTER parseQuery");
            if(loadParams)
            {
                //
                loadParameters(query);
            }
            start = System.currentTimeMillis();
            result = this.connection.execute(query);
        }
        finally
        {
            //
            queryTime = (System.currentTimeMillis() - start);
            totalQueryTime += queryTime;
            debug("CmdRunner.runQuery: BOTTOM");
        }
        return result;
    }

    /**
     * Converts a {@link Result} object to a string
     * 
     * @return String version of mondrian Result object.
     */
    public String toString(Result result)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        result.print(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * Converts a {@link Result} object to a string printing to standard output
     * directly, without buffering.
     * 
     * @return null String since output is dump directly to stdout.
     */
    public String highCardToString(Result result)
    {
        result.print(new PrintWriter(System.out, true));
        return null;
    }

    public void makeConnectString()
    {
        String connectString = MultiUserTest.getConnectStringProperty();
        debug("CmdRunner.makeConnectString: connectString=" + connectString);

        Util.PropertyList connectProperties;
        if(connectString == null || "".equals(connectString))
        {
            // create new and add provider
            connectProperties = new Util.PropertyList();
            connectProperties.put(RolapConnectionProperties.Provider.name(), "mondrian");
        }
        else
        {
            // load with existing connect string
            connectProperties = Util.parseConnectString(connectString);
        }

        // override jdbc url
        String jdbcURL = MultiUserTest.getJdbcURLProperty();

        debug("CmdRunner.makeConnectString: jdbcURL=" + jdbcURL);

        if(jdbcURL != null)
        {
            // add jdbc url to connect string
            connectProperties.put(RolapConnectionProperties.Jdbc.name(), jdbcURL);
        }

        // override jdbc drivers
        String jdbcDrivers = MultiUserTest.getJdbcDriversProperty();

        debug("CmdRunner.makeConnectString: jdbcDrivers=" + jdbcDrivers);
        if(jdbcDrivers != null)
        {
            // add jdbc drivers to connect string
            connectProperties.put(RolapConnectionProperties.JdbcDrivers.name(), jdbcDrivers);
        }

        // override catalog url
        String catalogURL = MultiUserTest.getCatalogURLProperty();

        debug("CmdRunner.makeConnectString: catalogURL=" + catalogURL);

        if(catalogURL != null)
        {
            // add catalog url to connect string
            connectProperties.put(RolapConnectionProperties.Catalog.name(), catalogURL);
        }

        // override JDBC user
        String jdbcUser = MultiUserTest.getJdbcUserProperty();

        debug("CmdRunner.makeConnectString: jdbcUser=" + jdbcUser);

        if(jdbcUser != null)
        {
            // add user to connect string
            connectProperties.put(RolapConnectionProperties.JdbcUser.name(), jdbcUser);
        }

        // override JDBC password
        String jdbcPassword = MultiUserTest.getJdbcPasswordProperty();

        debug("CmdRunner.makeConnectString: jdbcPassword=" + jdbcPassword);

        if(jdbcPassword != null)
        {
            // add password to connect string
            connectProperties.put(RolapConnectionProperties.JdbcPassword.name(), jdbcPassword);
        }

        if(options.roleName != null)
        {
            connectProperties.put(RolapConnectionProperties.Role.name(), options.roleName);
        }

        debug("CmdRunner.makeConnectString: connectProperties=" + connectProperties);

        this.connectString = connectProperties.toString();
    }

    /**
     * Gets a connection to Mondrian.
     * 
     * @return Mondrian {@link Connection}
     */
    public Connection getConnection()
    {
        return getConnection(MultiUserTest.RELOAD_CONNECTION);
    }

    /**
     * Gets a Mondrian connection, creating a new one if fresh is true.
     * 
     * @return mondrian Connection.
     */
    public synchronized Connection getConnection(boolean fresh)
    {
        // FIXME: fresh is currently ignored.
        if(this.connectString == null)
        {
            makeConnectString();
        }
        if(this.connection == null)
        {
            this.connection = DriverManager.getConnection(this.connectString, null);
        }
        return this.connection;
    }

    public String getConnectString()
    {
        return getConnectString(MultiUserTest.RELOAD_CONNECTION);
    }

    public synchronized String getConnectString(boolean fresh)
    {
        if(this.connectString == null)
        {
            makeConnectString();
        }
        return this.connectString;
    }

    // ///////////////////////////////////////////////////////////////////////
    // ///////////////////////////////////////////////////////////////////////
    //
    // static methods
    //
    // ///////////////////////////////////////////////////////////////////////
    // ///////////////////////////////////////////////////////////////////////

    protected void debug(String msg)
    {
        if(options.debug)
        {
            out.println(msg);
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    // properties
    // ///////////////////////////////////////////////////////////////////////
    protected static String getConnectStringProperty()
    {
        return MondrianProperties.instance().TestConnectString.get();
    }

    protected static String getJdbcURLProperty()
    {
        return MondrianProperties.instance().FoodmartJdbcURL.get();
    }

    protected static String getJdbcUserProperty()
    {
        return MondrianProperties.instance().TestJdbcUser.get();
    }

    protected static String getJdbcPasswordProperty()
    {
        return MondrianProperties.instance().TestJdbcPassword.get();
    }

    protected static String getCatalogURLProperty()
    {
        return MondrianProperties.instance().CatalogURL.get();
    }

    protected static String getJdbcDriversProperty()
    {
        return MondrianProperties.instance().JdbcDrivers.get();
    }

    // ///////////////////////////////////////////////////////////////////////
    // command loop
    // ///////////////////////////////////////////////////////////////////////

    protected void commandLoop(boolean interactive) throws IOException
    {
        commandLoop(new BufferedReader(new InputStreamReader(System.in)), interactive);
    }

    protected void commandLoop(File file) throws IOException
    {
        // If we open a stream, then we close it.
        FileReader in = new FileReader(file);
        try
        {
            commandLoop(new BufferedReader(in), false);
        }
        finally
        {
            try
            {
                in.close();
            }
            catch(Exception ex)
            {
                out.println(ex.getMessage());
            }
        }
    }

    protected void commandLoop(String mdxCmd, boolean interactive) throws IOException
    {
        StringReader is = new StringReader(mdxCmd);
        commandLoop(is, interactive);
    }

    /**
     * 
     */
    private static final String COMMAND_PROMPT_START = "> ";

    /**
     * 
     */
    private static final String COMMAND_PROMPT_MID = "? ";

    /**
     * The Command Loop where lines are read from the InputStream and
     * interpreted. If interactive then prompts are printed.
     * 
     * @param in
     *            Input reader (preferably buffered)
     * @param interactive
     *            Whether the session is interactive
     */
    protected void commandLoop(Reader in, boolean interactive)
    {
        StringBuilder buf = new StringBuilder(2048);
        boolean inMdxCmd = false;
        String resultString = null;
        try
        {
            for(;;)
            {
                if(resultString != null)
                {
                    printResults(resultString);
                    printQueryTime();
                    resultString = null;
                    buf.setLength(0);
                }
                else if(interactive && (error != null))
                {
                    printResults(error);
                    printQueryTime();
                }
                if(interactive)
                {
                    if(inMdxCmd)
                    {
                        out.print(COMMAND_PROMPT_MID);
                    }
                    else
                    {
                        out.print(COMMAND_PROMPT_START);
                    }
                    out.flush();
                }
                if(!inMdxCmd)
                {
                    buf.setLength(0);
                }
                String line;

                line = readLine(in, inMdxCmd);

                if(line != null)
                {
                    line = line.trim();
                }
                debug("line=" + line);

                if(!inMdxCmd)
                {
                    // If not in the middle of reading an mdx query and
                    // we reach end of file on the stream, then we are over.
                    if(line == null)
                    {
                        return;
                    }
                }

                // If not reading an mdx query, then check if the line is a
                // user command.
                if(!inMdxCmd)
                {
                    String cmd = line;
                    if(cmd.startsWith("help"))
                    {
                        resultString = executeHelp(cmd);
                    }
                    else if(cmd.startsWith("set"))
                    {
                        resultString = executeSet(cmd);
                    }
                    else if(cmd.startsWith("log"))
                    {
                        resultString = executeLog(cmd);
                    }
                    else if(cmd.startsWith("file"))
                    {
                        resultString = executeFile(cmd);
                    }
                    else if(cmd.startsWith("list"))
                    {
                        resultString = executeList(cmd);
                    }
                    else if(cmd.startsWith("func"))
                    {
                        resultString = executeFunc(cmd);
                    }
                    else if(cmd.startsWith("param"))
                    {
                        resultString = executeParam(cmd);
                    }
                    else if(cmd.startsWith("cube"))
                    {
                        resultString = executeCube(cmd);
                    }
                    else if(cmd.startsWith("error"))
                    {
                        resultString = executeError(cmd);
                    }
                    else if(cmd.startsWith("echo"))
                    {
                        resultString = executeEcho(cmd);
                    }
                    else if(cmd.startsWith("expr"))
                    {
                        resultString = executeExpr(cmd);
                    }
                    else if("=".equals(cmd))
                    {
                        resultString = reExecuteMdxCmd();
                    }
                    else if(cmd.startsWith("exit"))
                    {
                        break;
                    }
                    if(resultString != null)
                    {
                        inMdxCmd = false;
                        continue;
                    }
                }

                // Are we ready to execute an mdx query.
                if((line == null)
                        || ((line.length() == 1) && ((line.charAt(0) == EXECUTE_CHAR) || (line.charAt(0) == CANCEL_CHAR))))
                {
                    // If EXECUTE_CHAR, then execute, otherwise its the
                    // CANCEL_CHAR and simply empty buffer.
                    if((line == null) || (line.charAt(0) == EXECUTE_CHAR))
                    {
                        String mdxCmd = buf.toString().trim();
                        debug("mdxCmd=\"" + mdxCmd + "\"");
                        long t1 = System.currentTimeMillis();
                        resultString = executeMdxCmd(mdxCmd);
                        out.println("Total : " + (System.currentTimeMillis() - t1) / 1000);
                    }

                    inMdxCmd = false;

                }
                else if(line.length() > 0)
                {
                    // OK, just add the line to the mdx query we are building.
                    inMdxCmd = true;

                    if(line.endsWith(SEMI_COLON_STRING))
                    {
                        // Remove the ';' character.
                        buf.append(line.substring(0, line.length() - 1));
                        String mdxCmd = buf.toString().trim();
                        debug("mdxCmd=\"" + mdxCmd + "\"");
                        resultString = executeMdxCmd(mdxCmd);
                        inMdxCmd = false;
                    }
                    else
                    {
                        buf.append(line);
                        // add carriage return so that query keeps formatting
                        buf.append(nl);
                    }
                }
            }
        }
        catch(IOException e)
        {
            throw new RuntimeException("Exception while reading command line", e);
        }
    }

    protected void printResults(String resultString)
    {
        if(resultString != null)
        {
            resultString = resultString.trim();
            if(resultString.length() > 0)
            {
                out.println(resultString);
                out.flush();
            }
        }
    }

    protected void printQueryTime()
    {
        if(options.timeQueries && (queryTime != -1))
        {
            out.println("time[" + queryTime + "ms]");
            out.flush();
            queryTime = -1;
        }
    }

    /**
     * Gather up a line ending in '\n' or EOF. Returns null if at EOF. Strip out
     * comments. If a comment character appears within a string then its not a
     * comment. Strings are defined with "\"" or "'" characters. Also, a string
     * can span more than one line (a nice little complication). So, if we read
     * a string, then we consume the whole string as part of the "line"
     * returned, including EOL characters. If an escape character is seen '\\',
     * then it and the next character is added to the line regardless of what
     * the next character is.
     */
    protected static String readLine(Reader reader, boolean inMdxCmd) throws IOException
    {
        StringBuilder buf = new StringBuilder(128);
        StringBuilder line = new StringBuilder(128);
        int offset;
        int i = getLine(reader, line);
        boolean inName = false;

        for(offset = 0;offset < line.length();offset++)
        {
            char c = line.charAt(offset);

            if(c == ESCAPE_CHAR)
            {
                buf.append(ESCAPE_CHAR);
                buf.append(line.charAt(++offset));
            }
            else if(!inName && ((c == STRING_CHAR_1) || (c == STRING_CHAR_2)))
            {
                i = readString(reader, line, offset, buf, i);
                offset = 0;
            }
            else
            {
                int commentType = -1;

                if(c == BRACKET_START)
                {
                    inName = true;
                }
                else if(c == BRACKET_END)
                {
                    inName = false;
                }
                else if(!inName)
                {
                    // check if we have the start of a comment block
                    // check if we have the start of a comment block
                    for(int x = 0;x < commentDelim.length;x++)
                    {
                        if(c != commentStartChars[x])
                        {
                            continue;
                        }
                        String startComment = commentDelim[x][0];
                        boolean foundCommentStart = true;
                        for(int j = 1;j + offset < line.length() && j < startComment.length();j++)
                        {
                            if(line.charAt(j + offset) != startComment.charAt(j))
                            {
                                foundCommentStart = false;
                            }
                        }

                        if(foundCommentStart)
                        {
                            if(x == 0)
                            {
                                // A '#' must be the first character on a line
                                if(offset == 0)
                                {
                                    commentType = x;
                                    break;
                                }
                            }
                            else
                            {
                                commentType = x;
                                break;
                            }
                        }
                    }
                }

                // -1 means no comment
                if(commentType == -1)
                {
                    buf.append(c);
                }
                else
                {
                    // check for comment to end of line comment
                    if(commentDelim[commentType][1] == null)
                    {
                        break;
                    }
                    else
                    {
                        // handle delimited comment block
                        i = readBlock(reader, line, offset, commentDelim[commentType][0], commentDelim[commentType][1],
                                false, false, buf, i);
                        offset = 0;
                    }
                }
            }
        }

        if(i == -1 && buf.length() == 0)
        {
            return null;
        }
        else
        {
            return buf.toString();
        }
    }

    /**
     * Read the next line of input. Return the terminating character, -1 for end
     * of file, or \n or \r. Add \n and \r to the end of the buffer to be
     * included in strings and comment blocks.
     */
    protected static int getLine(Reader reader, StringBuilder line) throws IOException
    {
        line.setLength(0);
        for(;;)
        {
            int i = reader.read();

            if(i == -1)
            {
                return i;
            }

            line.append((char)i);

            if(i == '\n' || i == '\r')
            {
                return i;
            }
        }
    }

    /**
     * Start of a string, read all of it even if it spans more than one line
     * adding each line's <cr> to the buffer.
     */
    protected static int readString(Reader reader, StringBuilder line, int offset, StringBuilder buf, int i)
            throws IOException
    {
        String delim = line.substring(offset, offset + 1);
        return readBlock(reader, line, offset, delim, delim, true, true, buf, i);
    }

    /**
     * Start of a delimted block, read all of it even if it spans more than one
     * line adding each line's <cr> to the buffer.
     * 
     * A delimited block is a delimited comment (/\* ... *\/), or a string.
     */
    protected static int readBlock(Reader reader, StringBuilder line, int offset, final String startDelim,
            final String endDelim, final boolean allowEscape, final boolean addToBuf, StringBuilder buf, int i)
            throws IOException
    {
        int depth = 1;
        if(addToBuf)
        {
            buf.append(startDelim);
        }
        offset += startDelim.length();

        for(;;)
        {
            // see if we are at the end of the block
            if(line.substring(offset).startsWith(endDelim))
            {
                if(addToBuf)
                {
                    buf.append(endDelim);
                }
                offset += endDelim.length();
                --depth;
                if(depth == 0)
                {
                    break;
                }
                // check for nested block
            }
            else if(allowNestedComments && line.substring(offset).startsWith(startDelim))
            {
                if(addToBuf)
                {
                    buf.append(startDelim);
                }
                offset += startDelim.length();
                depth++;
            }
            else if(offset < line.length())
            {
                // not at the end of line, so eat the next char
                char c = line.charAt(offset++);
                if(allowEscape && c == ESCAPE_CHAR)
                {
                    if(addToBuf)
                    {
                        buf.append(ESCAPE_CHAR);
                    }

                    if(offset < line.length())
                    {
                        if(addToBuf)
                        {
                            buf.append(line.charAt(offset));
                        }
                        offset++;
                    }
                }
                else if(addToBuf)
                {
                    buf.append(c);
                }
            }
            else
            {
                // finished a line; read in the next one and continue
                if(i == -1)
                {
                    break;
                }
                i = getLine(reader, line);

                // line will always contain EOL marker at least, unless at EOF
                offset = 0;
                if(line.length() == 0)
                {
                    break;
                }
            }
        }

        // remove to the end of the string, so caller starts at offset 0
        if(offset > 0)
        {
            line.delete(0, offset - 1);
        }

        return i;
    }

    // ///////////////////////////////////////////////////////////////////////
    // xmla file
    // ///////////////////////////////////////////////////////////////////////

    /**
     * This is called to process a file containing XMLA as the contents of SOAP
     * xml.
     * 
     */
    protected void processSoapXmla(File file, int validateXmlaResponse) throws Exception
    {
        String catalogURL = MultiUserTest.getCatalogURLProperty();
        Map<String, String> catalogNameUrls = new HashMap<String, String>();
        catalogNameUrls.put(CATALOG_NAME, catalogURL);
        //
        long start = System.currentTimeMillis();
        //
        byte[] bytes = null;
        try
        {
            bytes = XmlaSupport.processSoapXmla(file, getConnectString(), catalogNameUrls, null);
        }
        finally
        {
            queryTime = (System.currentTimeMillis() - start);
            totalQueryTime += queryTime;
        }
        //
        String response = new String(bytes);
        out.println(response);
        //
        switch(validateXmlaResponse)
        {
        case VALIDATE_NONE:
            break;
        //
        case VALIDATE_TRANSFORM:
            XmlaSupport.validateSchemaSoapXmla(bytes);
            out.println("XML Data is Valid");
            break;
        //
        case VALIDATE_XPATH:
            XmlaSupport.validateSoapXmlaUsingXpath(bytes);
            out.println("XML Data is Valid");
            break;
        }
    }

    /**
     * This is called to process a file containing XMLA xml.
     * 
     */
    protected void processXmla(File file, int validateXmlaResponce) throws Exception
    {
        String catalogURL = MultiUserTest.getCatalogURLProperty();
        Map<String, String> catalogNameUrls = new HashMap<String, String>();
        catalogNameUrls.put(CATALOG_NAME, catalogURL);

        long start = System.currentTimeMillis();
        //
        byte[] bytes = null;
        try
        {
            bytes = XmlaSupport.processXmla(file, getConnectString(), catalogNameUrls);
        }
        finally
        {
            queryTime = (System.currentTimeMillis() - start);
            totalQueryTime += queryTime;
        }
        //
        String response = new String(bytes);
        out.println(response);
        //
        switch(validateXmlaResponce)
        {
        case VALIDATE_NONE:
            break;
        //
        case VALIDATE_TRANSFORM:
            XmlaSupport.validateSchemaXmla(bytes);
            out.println("XML Data is Valid");
            break;
        //
        case VALIDATE_XPATH:
            XmlaSupport.validateXmlaUsingXpath(bytes);
            out.println("XML Data is Valid");
            break;
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    // user commands and help messages
    // ///////////////////////////////////////////////////////////////////////
    /**
     * 
     */
    private static final String INDENT = "  ";

    /**
     * 
     */
    private static final int UNKNOWN_CMD = 0x0000;

    /**
     * 
     */
    private static final int HELP_CMD = 0x0001;

    /**
     * 
     */
    private static final int SET_CMD = 0x0002;

    /**
     * 
     */
    private static final int LOG_CMD = 0x0004;

    /**
     * 
     */
    private static final int FILE_CMD = 0x0008;

    /**
     * 
     */
    private static final int LIST_CMD = 0x0010;

    /**
     * 
     */
    private static final int MDX_CMD = 0x0020;

    /**
     * 
     */
    private static final int FUNC_CMD = 0x0040;

    /**
     * 
     */
    private static final int PARAM_CMD = 0x0080;

    /**
     * 
     */
    private static final int CUBE_CMD = 0x0100;

    /**
     * 
     */
    private static final int ERROR_CMD = 0x0200;

    /**
     * 
     */
    private static final int ECHO_CMD = 0x0400;

    /**
     * 
     */
    private static final int EXPR_CMD = 0x0800;

    /**
     * 
     */
    private static final int EXIT_CMD = 0x1000;

    /**
     * 
     */
    private static final int ALL_CMD = HELP_CMD | SET_CMD | LOG_CMD | FILE_CMD | LIST_CMD | MDX_CMD | FUNC_CMD
            | PARAM_CMD | CUBE_CMD | ERROR_CMD | ECHO_CMD | EXPR_CMD | EXIT_CMD;

    /**
     * 
     */
    private static final char ESCAPE_CHAR = '\\';

    /**
     * 
     */
    private static final char EXECUTE_CHAR = '=';

    /**
     * 
     */
    private static final char CANCEL_CHAR = '~';

    /**
     * 
     */
    private static final char STRING_CHAR_1 = '"';

    /**
     * 
     */
    private static final char STRING_CHAR_2 = '\'';

    /**
     * 
     */
    private static final char BRACKET_START = '[';

    /**
     * 
     */
    private static final char BRACKET_END = ']';

    /**
     * 
     */
    private static final String SEMI_COLON_STRING = ";";

    // ////////////////////////////////////////////////////////////////////////
    // help
    // ////////////////////////////////////////////////////////////////////////
    protected static String executeHelp(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");
        //
        int cmd = UNKNOWN_CMD;

        if(tokens.length == 1)
        {
            buf.append("Commands:");
            cmd = ALL_CMD;
            //
        }
        else if(tokens.length == 2)
        {
            String cmdName = tokens[1];
            //
            if("help".equals(cmdName))
            {
                cmd = HELP_CMD;
            }
            else if("set".equals(cmdName))
            {
                cmd = SET_CMD;
            }
            //
            else if("log".equals(cmdName))
            {
                cmd = LOG_CMD;
            }
            else if("file".equals(cmdName))
            {
                cmd = FILE_CMD;
            }
            //
            else if("list".equals(cmdName))
            {
                cmd = LIST_CMD;
            }
            else if("func".equals(cmdName))
            {
                cmd = FUNC_CMD;
            }
            else if("param".equals(cmdName))
            {
                cmd = PARAM_CMD;
            }
            //
            else if("cube".equals(cmdName))
            {
                cmd = CUBE_CMD;
            }
            else if("error".equals(cmdName))
            {
                cmd = ERROR_CMD;
            }
            else if("echo".equals(cmdName))
            {
                cmd = ECHO_CMD;
            }
            //
            else if("exit".equals(cmdName))
            {
                cmd = EXIT_CMD;
            }
            else
            {
                cmd = UNKNOWN_CMD;
            }
        }
        //
        if(cmd == UNKNOWN_CMD)
        {
            buf.append("Unknown help command: ");
            buf.append(mdxCmd);
            buf.append(nl);
            buf.append("Type \"help\" for list of commands");
        }
        //
        if((cmd & HELP_CMD) != 0)
        {
            // help
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("help");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Prints this text");
        }
        //
        if((cmd & SET_CMD) != 0)
        {
            // set
            buf.append(nl);
            appendSet(buf);
        }

        if((cmd & LOG_CMD) != 0)
        {
            // set
            buf.append(nl);
            appendLog(buf);
        }

        if((cmd & FILE_CMD) != 0)
        {
            // file
            buf.append(nl);
            appendFile(buf);
        }
        if((cmd & LIST_CMD) != 0)
        {
            // list
            buf.append(nl);
            appendList(buf);
        }
        //
        if((cmd & MDX_CMD) != 0)
        {
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("<mdx query> <cr> ( '");
            buf.append(EXECUTE_CHAR);
            buf.append("' | '");
            buf.append(CANCEL_CHAR);
            buf.append("' ) <cr>");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Execute or cancel mdx query.");
            buf.append(nl);
            appendIndent(buf, 2);//
            buf.append("An mdx query may span one or more lines.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("After the last line of the query has been entered,");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("on the next line a single execute character, '");
            buf.append(EXECUTE_CHAR);
            buf.append("', may be entered");
            buf.append(nl);
            appendIndent(buf, 3);//
            buf.append("followed by a carriage return.");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("The lone '");
            buf.append(EXECUTE_CHAR);//
            buf.append("' informs the interpreter that the query has");
            buf.append(nl);
            appendIndent(buf, 3);//
            buf.append("has been entered and is ready to execute.");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("At anytime during the entry of a query the cancel");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("character, '");
            buf.append(CANCEL_CHAR);
            buf.append("', may be entered alone on a line.");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("This removes all of the query text from the");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("the command interpreter.");
            buf.append(nl);
            appendIndent(buf, 2);//
            buf.append("Queries can also be ended by using a semicolon ';'");
            buf.append(nl);
            appendIndent(buf, 3);
            buf.append("at the end of a line.");
        }
        if((cmd & FUNC_CMD) != 0)
        {
            buf.append(nl);
            appendFunc(buf);
        }
        //
        if((cmd & PARAM_CMD) != 0)
        {
            buf.append(nl);
            appendParam(buf);
        }
        //
        if((cmd & CUBE_CMD) != 0)
        {
            buf.append(nl);
            appendCube(buf);
        }
        //
        if((cmd & ERROR_CMD) != 0)
        {
            buf.append(nl);
            appendError(buf);
        }

        if((cmd & ECHO_CMD) != 0)
        {
            buf.append(nl);
            appendEcho(buf);
        }
        //
        if((cmd & EXPR_CMD) != 0)
        {
            buf.append(nl);
            appendExpr(buf);
        }
        //
        if(cmd == ALL_CMD)
        {
            // reexecute
            buf.append(nl);
            appendIndent(buf, 1);
            buf.append("= <cr>");
            buf.append(nl);
            appendIndent(buf, 2);
            buf.append("Re-Execute mdx query.");
        }

        if((cmd & EXIT_CMD) != 0)
        {
            // exit
            buf.append(nl);
            appendExit(buf);
        }
        //
        return buf.toString();
    }

    protected static void appendIndent(StringBuilder buf, int i)
    {
        while(i-- > 0)
        {
            buf.append(MultiUserTest.INDENT);
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // set
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendSet(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("set [ property[=value ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no args, prints all mondrian properties and values.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"property\" prints property's value.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"property=value\" set property to that value.");
    }

    protected String executeSet(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(400);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            // list all properties
            listPropertiesAll(buf);
            //
        }
        else if(tokens.length == 2)
        {//

            String arg = tokens[1];
            int index = arg.indexOf('=');
            if(index == -1)
            {//
                listProperty(arg, buf);
            }
            else
            {//
                String[] nv = arg.split("=");
                String name = nv[0];
                String value = nv[1];
                if(isProperty(name))
                {
                    try
                    {//
                        if(setProperty(name, value))
                        {
                            this.connectString = null;
                        }
                    }
                    catch(Exception ex)
                    {
                        setError(ex);
                    }
                }
                else
                {//
                    buf.append("Bad property name:");
                    buf.append(name);
                    buf.append(nl);
                }
            }
            //
        }
        else
        {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // log
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendLog(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("log [ classname[=level ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no args, prints the current log level of all classes.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"classname\" prints the current log level of the class.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"classname=level\" set log level to new value.");
    }

    protected String executeLog(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            //
            Enumeration e = LogManager.getCurrentLoggers();
            while(e.hasMoreElements())
            {
                Logger logger = (Logger)e.nextElement();
                buf.append(logger.getName());
                buf.append(':');
                buf.append(logger.getLevel());
                buf.append(nl);
            }
            //
        }
        else if(tokens.length == 2)
        {
            //
            String arg = tokens[1];
            int index = arg.indexOf('=');
            if(index == -1)
            {
                Logger logger = LogManager.exists(arg);
                if(logger == null)
                {
                    //
                    buf.append("Bad log name: ");
                    buf.append(arg);
                    buf.append(nl);
                }
                else
                {
                    //
                    buf.append(logger.getName());
                    buf.append(':');
                    buf.append(logger.getLevel());
                    buf.append(nl);
                }
            }
            else
            {
                //
                String[] nv = arg.split("=");
                String classname = nv[0];
                String levelStr = nv[1];

                Logger logger = LogManager.getLogger(classname);

                if(logger == null)
                {
                    buf.append("Bad log name: ");
                    buf.append(classname);
                    buf.append(nl);
                }
                else
                {
                    //
                    Level level = Level.toLevel(levelStr, null);
                    if(level == null)
                    {
                        buf.append("Bad log level: ");
                        buf.append(levelStr);
                        buf.append(nl);
                    }
                    else
                    {
                        logger.setLevel(level);
                    }
                }
            }

        }
        else
        {
            //
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }
        //
        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // file
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendFile(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("file [ filename | '=' ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no args, prints the last filename executed.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"filename\", read and execute filename .");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"=\" character, re-read and re-execute previous filename .");
    }

    protected String executeFile(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(512);
        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            if(this.filename != null)
            {
                buf.append(this.filename);
            }

        }
        else if(tokens.length == 2)
        {
            String token = tokens[1];
            String nameOfFile = null;
            if((token.length() == 1) && (token.charAt(0) == EXECUTE_CHAR))
            {
                // file '='
                if(this.filename == null)
                {
                    buf.append("Bad command usage: \"");
                    buf.append(mdxCmd);
                    buf.append("\", no file to re-execute");
                    buf.append(nl);
                    appendFile(buf);
                }
                else
                {
                    nameOfFile = this.filename;
                }
            }
            else
            {
                // file filename
                nameOfFile = token;
            }

            if(nameOfFile != null)
            {
                this.filename = nameOfFile;

                try
                {
                    commandLoop(new File(this.filename));
                }
                catch(IOException ex)
                {
                    setError(ex);
                    buf.append("Error: ").append(ex);
                }
            }

        }
        else
        {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendFile(buf);
        }
        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // list
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendList(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("list [ cmd | result ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no arguments, list previous cmd and result");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"cmd\" argument, list the last mdx query cmd.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"result\" argument, list the last mdx query result.");
    }

    protected String executeList(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            if(this.mdxCmd != null)
            {
                //
                buf.append(this.mdxCmd);
                if(mdxResult != null)
                {
                    buf.append(nl);
                    buf.append(mdxResult);
                }
            }
            //
            else if(mdxResult != null)
            {
                buf.append(mdxResult);
            }

        }
        else if(tokens.length == 2)
        {
            String arg = tokens[1];
            if("cmd".equals(arg))
            {
                if(this.mdxCmd != null)
                {
                    buf.append(this.mdxCmd);
                }
            }
            else if("result".equals(arg))
            {
                //
                if(mdxResult != null)
                {
                    buf.append(mdxResult);
                }
            }
            else
            {
                //
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append(nl);
                appendList(buf);
            }
        }
        else
        {
            //
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // func
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendFunc(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("func [ name ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no arguments, list all defined function names");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"name\" argument, display the functions:");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("name, description, and syntax");
    }

    protected String executeFunc(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        final FunTable funTable = getConnection().getSchema().getFunTable();
        if(tokens.length == 1)
        {
            // prints names only once
            List<FunInfo> funInfoList = funTable.getFunInfoList();
            Iterator<FunInfo> it = funInfoList.iterator();
            String prevName = null;
            while(it.hasNext())
            {
                FunInfo fi = it.next();
                String name = fi.getName();
                if(prevName == null || !prevName.equals(name))
                {
                    buf.append(name);
                    buf.append(nl);
                    prevName = name;
                }
            }

        }
        else if(tokens.length == 2)
        {
            String funcname = tokens[1];
            List<FunInfo> funInfoList = funTable.getFunInfoList();
            List<FunInfo> matches = new ArrayList<FunInfo>();

            for(FunInfo fi : funInfoList)
            {
                if(fi.getName().equalsIgnoreCase(funcname))
                {
                    matches.add(fi);
                }
            }

            if(matches.size() == 0)
            {
                buf.append("Bad function name \"");
                buf.append(funcname);
                buf.append("\", usage:");
                buf.append(nl);
                appendList(buf);
            }
            else
            {
                Iterator<FunInfo> it = matches.iterator();
                boolean doname = true;
                while(it.hasNext())
                {
                    FunInfo fi = it.next();
                    if(doname)
                    {
                        buf.append(fi.getName());
                        buf.append(nl);
                        doname = false;
                    }

                    appendIndent(buf, 1);
                    buf.append(fi.getDescription());
                    buf.append(nl);

                    String[] sigs = fi.getSignatures();
                    if(sigs == null)
                    {
                        appendIndent(buf, 2);
                        buf.append("Signature: ");
                        buf.append("NONE");
                        buf.append(nl);
                    }
                    else
                    {
                        for(String sig : sigs)
                        {
                            appendIndent(buf, 2);
                            buf.append(sig);
                            buf.append(nl);
                        }
                    }
                    /*
                     * appendIndent(buf, 1); buf.append("Return Type: "); int
                     * returnType = fi.getReturnTypes(); if (returnType >= 0) {
                     * buf.append(cat.getName(returnType)); } else {
                     * buf.append("NONE"); } buf.append(nl); int[][] paramsArray
                     * = fi.getParameterTypes(); if (paramsArray == null) {
                     * appendIndent(buf, 1); buf.append("Paramter Types: ");
                     * buf.append("NONE"); buf.append(nl);
                     * 
                     * } else { for (int j = 0; j < paramsArray.length; j++) {
                     * int[] params = paramsArray[j]; appendIndent(buf, 1);
                     * buf.append("Paramter Types: "); for (int k = 0; k <
                     * params.length; k++) { int param = params[k];
                     * buf.append(cat.getName(param)); buf.append(' '); }
                     * buf.append(nl); } }
                     */
                }
            }
        }
        else
        {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // param
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendParam(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("param [ name[=value ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With no argumnts, all param name/value pairs are printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"name\" argument, the value of the param is printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"name=value\" sets the parameter with name to value.");
        buf.append(nl);
        appendIndent(buf, 3);
        //
        buf.append(" If name is null, then unsets all parameters");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append(" If value is null, then unsets the parameter associated with " + "value");
    }

    protected String executeParam(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            // list all properties
            listParameterNameValues(buf);

        }
        else if(tokens.length == 2)
        {
            //
            String arg = tokens[1];
            int index = arg.indexOf('=');
            if(index == -1)
            {
                if(isParam(arg))
                {
                    listParam(arg, buf);
                }
                else
                {
                    //
                    buf.append("Bad parameter name:");
                    buf.append(arg);
                    buf.append(nl);
                }
            }
            else
            {
                //
                String[] nv = arg.split("=");
                String name = (nv.length == 0) ? null : nv[0];
                String value = (nv.length == 2) ? nv[1] : null;
                setParameter(name, value);
            }

        }
        else
        {
            //
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // cube
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendCube(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("cube [ cubename [ name [=value | command] ] ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no argumnts, all cubes are listed by name.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename\" argument, cube attribute name/values for:");
        buf.append(nl);
        appendIndent(buf, 3);
        //
        buf.append("fact table (readonly)");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("aggregate caching (readwrite)");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("are printed");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename name=value\" sets the readwrite attribute with " + "name to value.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"cubename command\" executes the commands:");
        buf.append(nl);
        appendIndent(buf, 3);
        //
        buf.append("clearCache");
    }

    protected String executeCube(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            // list all properties
            listCubeName(buf);
        }
        else if(tokens.length == 2)
        {
            String cubename = tokens[1];
            listCubeAttribues(cubename, buf);

        }
        else if(tokens.length == 3)
        {
            String cubename = tokens[1];
            String arg = tokens[2];
            int index = arg.indexOf('=');
            if(index == -1)
            {
                // its a commnd
                executeCubeCommand(cubename, arg, buf);
            }
            else
            {
                String[] nv = arg.split("=");
                String name = (nv.length == 0) ? null : nv[0];
                String value = (nv.length == 2) ? nv[1] : null;
                setCubeAttribute(cubename, name, value, buf);
            }

        }
        else
        {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendSet(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // error
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendError(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("error [ msg | stack ] <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With no argumnts, both message and stack are printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("With \"msg\" argument, the Error message is printed.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("With \"stack\" argument, the Error stack trace is printed.");
    }

    protected String executeError(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(200);

        String[] tokens = mdxCmd.split("\\s+");

        if(tokens.length == 1)
        {
            if(error != null)
            {
                //
                buf.append(error);
                if(stack != null)
                {
                    buf.append(nl);
                    buf.append(stack);
                }
            }
            else if(stack != null)
            {
                buf.append(stack);
            }
            //

        }
        else if(tokens.length == 2)
        {
            String arg = tokens[1];
            if("msg".equals(arg))
            {
                if(error != null)
                {
                    buf.append(error);
                }
            }
            else if("stack".equals(arg))
            {
                if(stack != null)
                {
                    buf.append(stack);
                }
            }
            else
            {
                buf.append("Bad sub command usage:");
                buf.append(mdxCmd);
                buf.append(nl);
                appendList(buf);
            }
        }
        else
        {
            buf.append("Bad command usage: \"");
            buf.append(mdxCmd);
            buf.append('"');
            buf.append(nl);
            appendList(buf);
        }

        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // echo
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendEcho(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("echo text <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("echo text to standard out.");
    }

    protected String executeEcho(String mdxCmd)
    {
        try
        {
            String resultString = (mdxCmd.length() == 4) ? "" : mdxCmd.substring(4);
            return resultString;
        }
        catch(Exception ex)
        {
            setError(ex);
            // return error;
            return null;
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // expr
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendExpr(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("expr cubename expression<cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        //
        buf.append("evaluate an expression against a cube.");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("where: ");
        buf.append(nl);
        appendIndent(buf, 3);
        //
        buf.append("cubename is single word or string using [], '' or \"\"");
        buf.append(nl);
        appendIndent(buf, 3);
        buf.append("expression is string using \"\"");
    }

    protected String executeExpr(String mdxCmd)
    {
        StringBuilder buf = new StringBuilder(256);

        mdxCmd = (mdxCmd.length() == 5) ? "" : mdxCmd.substring(5);

        String regex = "(\"[^\"]+\"|'[^\']+'|\\[[^\\]]+\\]|[^\\s]+)\\s+.*";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(mdxCmd);
        boolean b = m.matches();
        //
        if(!b)
        {
            buf.append("Could not parse into \"cubename expression\" command:");
            buf.append(nl);
            buf.append(mdxCmd);
            String msg = buf.toString();
            setError(msg);
            return msg;
        }
        else
        {
            //
            String cubeName = m.group(1);
            String expression = mdxCmd.substring(cubeName.length() + 1);

            if(cubeName.charAt(0) == '"')
            {
                cubeName = cubeName.substring(1, cubeName.length() - 1);
            }
            else if(cubeName.charAt(0) == '\'')
            {
                cubeName = cubeName.substring(1, cubeName.length() - 1);
            }
            else if(cubeName.charAt(0) == '[')
            {
                cubeName = cubeName.substring(1, cubeName.length() - 1);
            }
            //
            int len = expression.length();
            if(expression.charAt(0) == '"')
            {
                if(expression.charAt(len - 1) != '"')
                {
                    buf.append("Missing end '\"' in expression:");
                    buf.append(nl);
                    buf.append(expression);
                    String msg = buf.toString();
                    setError(msg);
                    return msg;
                }
                expression = expression.substring(1, len - 1);
                //
            }
            else if(expression.charAt(0) == '\'')
            {
                if(expression.charAt(len - 1) != '\'')
                {
                    buf.append("Missing end \"'\" in expression:");
                    buf.append(nl);
                    buf.append(expression);
                    String msg = buf.toString();
                    setError(msg);
                    return msg;
                }
                expression = expression.substring(1, len - 1);
            }
            //
            Cube cube = getCube(cubeName);
            if(cube == null)
            {
                buf.append("No cube found with name \"");
                buf.append(cubeName);
                buf.append("\"");
                String msg = buf.toString();
                setError(msg);
                return msg;
                //
            }
            else
            {
                try
                {
                    if(cubeName.indexOf(' ') >= 0)
                    {
                        if(cubeName.charAt(0) != '[')
                        {
                            cubeName = Util.quoteMdxIdentifier(cubeName);
                        }
                    }
                    final char c = '\'';
                    if(expression.indexOf('\'') != -1)
                    {
                        // make sure all "'" are escaped
                        int start = 0;
                        int index = expression.indexOf('\'', start);
                        if(index == 0)
                        {
                            // error: starts with "'"
                            buf.append("Double \"''\" starting expression:");
                            buf.append(nl);
                            buf.append(expression);
                            String msg = buf.toString();
                            setError(msg);
                            return msg;
                        }
                        while(index != -1)
                        {
                            if(expression.charAt(index - 1) != '\\')
                            {
                                // error
                                buf.append("Non-escaped \"'\" in expression:");
                                buf.append(nl);
                                buf.append(expression);
                                String msg = buf.toString();
                                setError(msg);
                                return msg;
                            }
                            start = index + 1;
                            index = expression.indexOf('\'', start);
                        }
                    }

                    // taken from FoodMartTest code
                    StringBuilder queryStringBuf = new StringBuilder(64);
                    queryStringBuf.append("with member [Measures].[Foo] as ");
                    queryStringBuf.append(c);
                    queryStringBuf.append(expression);
                    queryStringBuf.append(c);
                    queryStringBuf.append(" select {[Measures].[Foo]} on columns from ");
                    queryStringBuf.append(cubeName);
                    //
                    String queryString = queryStringBuf.toString();

                    Result result = runQuery(queryString, true);
                    String resultString = result.getCell(new int[]{0}).getFormattedValue();
                    mdxResult = resultString;
                    clearError();
                    //
                    buf.append(resultString);
                }
                catch(Exception ex)
                {
                    setError(ex);
                    buf.append("Error: ").append(ex);
                }
            }
        }
        return buf.toString();
    }

    // ////////////////////////////////////////////////////////////////////////
    // exit
    // ////////////////////////////////////////////////////////////////////////
    protected static void appendExit(StringBuilder buf)
    {
        appendIndent(buf, 1);
        buf.append("exit <cr>");
        buf.append(nl);
        appendIndent(buf, 2);
        buf.append("Exit mdx command interpreter.");
    }

    protected String reExecuteMdxCmd()
    {
        if(this.mdxCmd == null)
        {
            return "No command to execute";
        }
        else
        {
            return executeMdxCmd(this.mdxCmd);
        }
    }

    /**
     * @param mdxCmd
     * @return
     */
    public String executeMdxCmd(String mdxCmd)
    {
        this.mdxCmd = mdxCmd;
        try
        {
            //
            String resultString = execute(mdxCmd);
            mdxResult = resultString;
            clearError();
            return resultString;
        }
        catch(Exception ex)
        {
            setError(ex);
            // return error;
            return null;
        }
    }

    // ///////////////////////////////////////////////////////////////////////
    // helpers
    // ///////////////////////////////////////////////////////////////////////
    protected static void loadPropertiesFromFile(String propFile) throws IOException
    {
        MondrianProperties.instance().load(new FileInputStream(propFile));
    }

    // ///////////////////////////////////////////////////////////////////////
    // main
    // ///////////////////////////////////////////////////////////////////////

    /**
     * Prints a usage message.
     * 
     * @param msg
     *            Prefix to the message
     * @param out
     *            Output stream
     */
    protected static void usage(String msg, PrintStream out)
    {
        StringBuilder buf = new StringBuilder(256);
        //
        if(msg != null)
        {
            buf.append(msg);
            buf.append(nl);
        }
        //
        buf.append("Usage: mondrian.tui.CmdRunner args" + nl + "  args:" + nl
                + "  -h               : print this usage text" + nl
                + "  -H               : ready to print out high cardinality" + nl + "                     dimensions"
                + nl + "  -d               : enable local debugging" + nl + "  -t               : time each mdx query"
                + nl + "  -nocache         : turn off in-memory aggregate caching" + nl
                + "                     for all cubes regardless of setting" + nl + "                     in schema"
                + nl + "  -rc              : do NOT reload connections each query" + nl
                + "                     (default is to reload connections)" + nl
                + "  -p propertyfile  : load mondrian properties" + nl
                + "  -r role_name     : set the connections role name" + nl
                + "  -f mdx_filename+ : execute mdx in one or more files" + nl
                + "  -x xmla_filename+: execute XMLA in one or more files"
                + "                     the XMLA request has no SOAP wrapper" + nl + "  -xs soap_xmla_filename+ "
                + "                   : execute Soap XMLA in one or more files"
                + "                     the XMLA request has a SOAP wrapper" + nl
                + "  -vt              : validate xmla response using transforms"
                + "                     only used with -x or -xs flags" + nl
                + "  -vx              : validate xmla response using xpaths"
                + "                     only used with -x or -xs flags" + nl + "  mdx_cmd          : execute mdx_cmd"
                + nl);
        //
        out.println(buf.toString());
    }

    /**
     * Set the default comment delimiters for CmdRunner. These defaults are # to
     * end of line plus all the comment delimiters in Scanner.
     */
    private static void setDefaultCommentState()
    {
        allowNestedComments = mondrian.olap.Scanner.getNestedCommentsState();
        String[][] scannerCommentsDelimiters = mondrian.olap.Scanner.getCommentDelimiters();
        commentDelim = new String[scannerCommentsDelimiters.length + 1][2];
        commentStartChars = new char[scannerCommentsDelimiters.length + 1];

        // CmdRunner has extra delimiter; # to end of line
        commentDelim[0][0] = "#";
        commentDelim[0][1] = null;
        commentStartChars[0] = commentDelim[0][0].charAt(0);

        // copy all the rest of the delimiters
        for(int x = 0;x < scannerCommentsDelimiters.length;x++)
        {
            commentDelim[x + 1][0] = scannerCommentsDelimiters[x][0];
            commentDelim[x + 1][1] = scannerCommentsDelimiters[x][1];
            commentStartChars[x + 1] = commentDelim[x + 1][0].charAt(0);
        }
    }

    /**
     * 
     */
    private static final int DO_MDX = 1;

    /**
     * 
     */
    private static final int DO_XMLA = 2;

    /**
     * 
     */
    private static final int DO_SOAP_XMLA = 3;

    /**
     * 
     */
    private static final int VALIDATE_NONE = 1;

    /**
     * 
     */
    private static final int VALIDATE_TRANSFORM = 2;

    /**
     * 
     */
    private static final int VALIDATE_XPATH = 3;

    /**
     */
    private static class Options
    {
        /**
         * 
         */
        private boolean debug = false;

        /**
         * 
         */
        private boolean timeQueries;

        /**
         * 
         */
        private boolean noCache = false;

        /**
         * 
         */
        private String roleName;

        /**
         * 
         */
        private int validateXmlaResponse = VALIDATE_NONE;

        /**
         * 
         */
        private final List<String> filenames = new ArrayList<String>();

        /**
         * 
         */
        private int doingWhat = DO_MDX;

        /**
         * 
         */
        private String singleMdxCmd;

        /**
         * 
         */
        private boolean highCardResults;
    }

    /*
     * private void printTotalQueryTime() { if(options.timeQueries) { // only
     * print if different if(totalQueryTime != queryTime) { out.println("total["
     * + totalQueryTime + "ms]"); } } out.flush(); }
     */

    // private static Options parseOptions(String[] args) throws BadOption,
    // IOException
    // {
    // final Options options = new Options();
    // for(int i = 0;i < args.length;i++)
    // {
    // String arg = args[i];
    // if(arg.equals("-h"))
    // {
    // throw new BadOption(null);
    // }
    // else if(arg.equals("-H"))
    // {
    // options.highCardResults = true;
    //
    // }
    // else if(arg.equals("-d"))
    // {
    // options.debug = true;
    //
    // }
    // else if(arg.equals("-t"))
    // {
    // options.timeQueries = true;
    //
    // }
    // else if(arg.equals("-nocache"))
    // {
    // options.noCache = true;
    //
    // }
    // else if(arg.equals("-rc"))
    // {
    // MultiUserTest.RELOAD_CONNECTION = false;
    //
    // }
    // else if(arg.equals("-vt"))
    // {
    // options.validateXmlaResponse = VALIDATE_TRANSFORM;
    //
    // }
    // else if(arg.equals("-vx"))
    // {
    // options.validateXmlaResponse = VALIDATE_XPATH;
    //
    // }
    // else if(arg.equals("-f"))
    // {
    // i++;
    // if(i == args.length)
    // {
    // throw new BadOption("no mdx filename given");
    // }
    // options.filenames.add(args[i]);
    //
    // }
    // else if(arg.equals("-x"))
    // {
    // i++;
    // if(i == args.length)
    // {
    // throw new BadOption("no XMLA filename given");
    // }
    // options.doingWhat = DO_XMLA;
    // options.filenames.add(args[i]);
    //
    // }
    // else if(arg.equals("-xs"))
    // {
    // i++;
    // if(i == args.length)
    // {
    // throw new BadOption("no XMLA filename given");
    // }
    // options.doingWhat = DO_SOAP_XMLA;
    // options.filenames.add(args[i]);
    //
    // }
    // else if(arg.equals("-p"))
    // {
    // i++;
    // if(i == args.length)
    // {
    // throw new BadOption("no mondrian properties file given");
    // }
    // String propFile = args[i];
    // loadPropertiesFromFile(propFile);
    //
    // }
    // else if(arg.equals("-r"))
    // {
    // i++;
    // if(i == args.length)
    // {
    // throw new BadOption("no role name given");
    // }
    // options.roleName = args[i];
    // }
    // else if(!options.filenames.isEmpty())
    // {
    // options.filenames.add(arg);
    // }
    // else
    // {
    // options.singleMdxCmd = arg;
    // }
    // }
    // return options;
    // }

    // private static class BadOption extends Exception
    // {
    // BadOption(String msg)
    // {
    // super(msg);
    // }
    //
    // BadOption(String msg, Exception ex)
    // {
    // super(msg, ex);
    // }
    // }

    /**
     */
    private static class QueryDetails
    {
        /**
         * 
         */
        private int queryid;

        /**
         * 
         */
        private long timeTaken;

        QueryDetails(int queryid, long timeTaken)
        {
            this.queryid = queryid;
            this.timeTaken = timeTaken;
        }
    }

    /**
     */
    private static class MDXExecuterThread implements Runnable
    {
        /**
         * 
         */
        private Map<Integer, String> queryList;

        /**
         * 
         */
        private MultiUserTest runner;

        /**
         * 
         */
        private Map<Integer, List<QueryDetails>> timeLoagMap;

        /**
         * 
         */
        private int userId;

        MDXExecuterThread(Map<Integer, String> queryList, Map<Integer, List<QueryDetails>> timeLoagMap,
                int userId)
        {
            this.queryList = queryList;
            runner = new MultiUserTest(null, null);
            StringBuilder buf = new StringBuilder("cube");
            runner.listCubeName(buf);
            this.timeLoagMap = timeLoagMap;
            this.userId = userId;

        }

        @Override
        public void run()
        {
            // Map<Integer, Long> queryTime = new HashMap<Integer, Long>();
            List<QueryDetails> listQueryDetail = new ArrayList<QueryDetails>();
            for(int i = 1;i <= queryList.size();i++)
            {
                long currentTimeMillis = System.currentTimeMillis();
                String string = queryList.get(i);
                if(null == string)
                {
                    continue;
                }
                runner.executeMdxCmd(string);
                // Long long1 = queryTime.get(indexArray[i]);
                QueryDetails queryDetails = new QueryDetails(i, (System.currentTimeMillis() - currentTimeMillis));
                listQueryDetail.add(queryDetails);
                // if(long1==null)
                // {
                // queryTime.put(indexArray[i], (System.currentTimeMillis() -
                // currentTimeMillis));
                // }
                // else
                // {
                // long1+=(System.currentTimeMillis() - currentTimeMillis);
                //
                // queryTime.put(indexArray[i], long1);
                // }
            }
            timeLoagMap.put(this.userId, listQueryDetail);
        }

        // private int[] getIndexArray(int size)
        // {
        // int a[] = new int[size];
        // int len = 0;
        // for(int idx = 1;idx <= size;++idx)
        // {
        // a[len++] = 1 + (int)(Math.random() * ((size - 1) + 1));
        // }
        // return a;
        // }
    }

    public static void main(String[] args) throws Exception
    {

        String queryfilepath = MondrianProperties.instance().getProperty("queryfilepath");
        String numberofusers = MondrianProperties.instance().getProperty("numberofusers");
        String logfilepath = MondrianProperties.instance().getProperty("logfilepath");
        Map<Integer, String> queryListFromFile = QueryFileReader.getQueryListFromFile(queryfilepath);
        System.out.println("List Size: " + queryListFromFile.size());
        int numberofUsers = Integer.parseInt(numberofusers);
        ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(numberofUsers);

        Map<Integer, List<QueryDetails>> timeLoagMap = new ConcurrentHashMap<Integer, List<QueryDetails>>();

        long startTime = System.currentTimeMillis();
        for(int i = 0;i < numberofUsers;i++)
        {

            threadPoolExecutor.submit(new MDXExecuterThread(queryListFromFile, timeLoagMap, i));
        }

        threadPoolExecutor.shutdown();
        threadPoolExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
        long endTime = System.currentTimeMillis();
        FileWriter fstream = new FileWriter(logfilepath, false);
        BufferedWriter out = new BufferedWriter(fstream);
        System.out.println("Map Size: " + timeLoagMap.size());
        Set<Entry<Integer, List<QueryDetails>>> entrySet = timeLoagMap.entrySet();
        Iterator<Entry<Integer, List<QueryDetails>>> iterator = entrySet.iterator();

        while(iterator.hasNext())
        {
            Entry<Integer, List<QueryDetails>> next = iterator.next();
            Integer userId = next.getKey();
            List<QueryDetails> value = next.getValue();
            Iterator<QueryDetails> iterator3 = value.iterator();
            long totalTime = 0;
            while(iterator3.hasNext())
            {
                out.write("UserId: " + userId);
                out.write(" , ");
                QueryDetails next3 = iterator3.next();
                out.write("QueryId: " + next3.queryid);
                out.write(" , ");
                out.write("Time Taken(miliseconds): " + next3.timeTaken);
                totalTime += next3.timeTaken;
                out.write(Util.nl);
            }
            out.write("UserId: " + userId + " totalTime: " + totalTime);
            out.write(Util.nl);
        }
        out.write("Total time taken to execute all querys (seconds): " + ((endTime - startTime) / 1000));
        out.flush();
        out.close();

        // Options options;
        // try {
        // options = parseOptions(args);
        // } catch (BadOption badOption) {
        // usage(badOption.getMessage(), System.out);
        // Throwable t = badOption.getCause();
        // if (t != null) {
        // System.out.println(t);
        // t.printStackTrace();
        // }
        // return;
        // }
        //
        // CmdRunner cmdRunner =
        // new CmdRunner(options, new PrintWriter(System.out));
        // if (options.noCache) {
        // cmdRunner.noCubeCaching();
        // }
        //
        // if (!options.filenames.isEmpty()) {
        // for (String filename : options.filenames) {
        // cmdRunner.filename = filename;
        // switch (options.doingWhat) {
        // case DO_MDX:
        // // its a file containing mdx
        // cmdRunner.commandLoop(new File(filename));
        // break;
        // case DO_XMLA:
        // // its a file containing XMLA
        // cmdRunner.processXmla(
        // new File(filename),
        // options.validateXmlaResponse);
        // break;
        // default:
        // // its a file containing SOAP XMLA
        // cmdRunner.processSoapXmla(
        // new File(filename),
        // options.validateXmlaResponse);
        // break;
        // }
        // if (cmdRunner.error != null) {
        // System.err.println(filename);
        // System.err.println(cmdRunner.error);
        // if (cmdRunner.stack != null) {
        // System.err.println(cmdRunner.stack);
        // }
        // cmdRunner.printQueryTime();
        // cmdRunner.clearError();
        // }
        // }
        // } else if (options.singleMdxCmd != null) {
        // cmdRunner.commandLoop(options.singleMdxCmd, false);
        // if (cmdRunner.error != null) {
        // System.err.println(cmdRunner.error);
        // if (cmdRunner.stack != null) {
        // System.err.println(cmdRunner.stack);
        // }
        // }
        // } else {
        // cmdRunner.commandLoop(true);
        // }
        // cmdRunner.printTotalQueryTime();
    }

}

// End CmdRunner.java
