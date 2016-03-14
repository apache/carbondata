/*
// $Id: //open/mondrian/src/main/mondrian/tui/MockServletContext.java#8 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */

package com.huawei.unibi.molap.engine.mondriantest;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

/**
 * Partial implementation of the {@link ServletContext} where just enough is
 * present to allow for communication between Mondrian's XMLA code and other
 * code in the same JVM.
 * 
 * <p>
 * Currently it is used in both the CmdRunner and in XMLA JUnit tests. If you
 * need to add to this implementation, please do so.
 * 
 * @author Richard M. Emberson
 * @version $Id: //open/mondrian/src/main/mondrian/tui/MockServletContext.java#8
 *          $
 */
public class MockServletContext implements ServletContext
{

    /**
     * 
     */
    public static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";

    /**
     * 
     */
    public static final String PARAM_CHAR_ENCODING = "CharacterEncoding";

    /**
     * 
     */
    public static final String PARAM_CALLBACKS = "Callbacks";

    /**
     * 
     */
    private Map<String, URL> resources;

    /**
     * 
     */
    private Map<String, Object> attributes;

    /**
     * 
     */
    private int majorVersion;

    /**
     * 
     */
    private int minorVersion;

    /**
     * 
     */
    private Properties parameters;

    public MockServletContext()
    {
        this.majorVersion = 1;
        this.minorVersion = 1;
        this.resources = Collections.emptyMap();
        this.attributes = Collections.emptyMap();
        this.parameters = new Properties();
    }

    /**
     * Returns a ServletContext object that corresponds to a specified URL on
     * the server.
     * 
     */
    public ServletContext getContext(String s)
    {
        // TODO
        return null;
    }

    /**
     * Returns the major version of the Java Servlet API that this servlet
     * container supports.
     * 
     */
    public int getMajorVersion()
    {
        return this.majorVersion;
    }

    /**
     * Returns the minor version of the Servlet API that this servlet container
     * supports.
     * 
     */
    public int getMinorVersion()
    {
        return this.minorVersion;
    }

    /**
     * Returns the MIME type of the specified file, or null if the MIME type is
     * not known.
     * 
     */
    public String getMimeType(String s)
    {
        // TODO
        return null;
    }

    /**
     *
     *
     */
    public Set getResourcePaths(String s)
    {
        // TODO
        return null;
    }

    /**
     * Returns a URL to the resource that is mapped to a specified path.
     * 
     */
    public URL getResource(String name) throws MalformedURLException
    {
        return resources.get(name);
    }

    /**
     * Returns the resource located at the named path as an InputStream object.
     * 
     */
    public InputStream getResourceAsStream(String s)
    {
        // TODO
        return null;
    }

    /**
     * Returns a RequestDispatcher object that acts as a wrapper for the
     * resource located at the given path.
     * 
     */
    public RequestDispatcher getRequestDispatcher(String s)
    {
        // TODO
        return null;
    }

    /**
     * Returns a RequestDispatcher object that acts as a wrapper for the named
     * servlet.
     * 
     */
    public RequestDispatcher getNamedDispatcher(String s)
    {
        // TODO
        return null;
    }

    public Servlet getServlet(String s) throws ServletException
    {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    public Enumeration getServlets()
    {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    public Enumeration getServletNames()
    {
        // method is deprecated as of Servlet API 2.1
        return null;
    }

    /**
     * Writes the specified message to a servlet log file, usually an event log.
     * 
     */
    public void log(String s)
    {
        // TODO
    }

    /**
     * Deprecated. As of Java Servlet API 2.1, use log(String message, Throwable
     * throwable) instead.
     * 
     * This method was originally defined to write an exception's stack trace
     * and an explanatory error message to the servlet log file.
     * 
     * @deprecated Method log is deprecated
     */
    public void log(Exception exception, String s)
    {
        log(s, exception);
    }

    /**
     * Writes an explanatory message and a stack trace for a given Throwable
     * exception to the servlet log file.
     * 
     */
    public void log(String s, Throwable throwable)
    {
        // TODO
    }

    /**
     * Returns a String containing the real path for a given virtual path.
     * 
     */
    public String getRealPath(String path)
    {
        return path;
    }

    /**
     * Returns the name and version of the servlet container on which the
     * servlet is running.
     * 
     */
    public String getServerInfo()
    {
        // TODO
        return null;
    }

    /**
     * Returns a String containing the value of the named context-wide
     * initialization parameter, or null if the parameter does not exist.
     * 
     */
    public String getInitParameter(String name)
    {
        return parameters.getProperty(name);
    }

    /**
     * Returns the names of the context's initialization parameters as an
     * Enumeration of String objects, or an empty Enumeration if the context has
     * no initialization parameters.
     * 
     */
    public Enumeration getInitParameterNames()
    {
        return parameters.propertyNames();
    }

    /**
     *
     *
     */
    public Object getAttribute(String s)
    {
        return this.attributes.get(s);
    }

    /**
     * Returns an Enumeration containing the attribute names available within
     * this servlet context.
     * 
     */
    public Enumeration getAttributeNames()
    {
        // TODO
        return Collections.enumeration(this.attributes.keySet());
    }

    /**
     * Binds an object to a given attribute name in this servlet context.
     * 
     */
    public void setAttribute(String s, Object obj)
    {
        if(this.attributes == Collections.EMPTY_MAP)
        {
            this.attributes = new HashMap<String, Object>();
        }
        this.attributes.put(s, obj);
    }

    /**
     * Removes the attribute with the given name from the servlet context.
     * 
     */
    public void removeAttribute(String s)
    {
        this.attributes.remove(s);
    }

    /**
     *
     *
     */
    public String getServletContextName()
    {
        // TODO
        return null;
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    // ///////////////////////////////////////////////////////////////////////
    public void setMajorVersion(int majorVersion)
    {
        this.majorVersion = majorVersion;
    }

    public void setMinorVersion(int minorVersion)
    {
        this.minorVersion = minorVersion;
    }

    public void addResource(String name, URL url)
    {
        if(this.resources == Collections.EMPTY_MAP)
        {
            this.resources = new HashMap<String, URL>();
        }
        this.resources.put(name, url);
    }

    public void addInitParameter(String name, String value)
    {
        if(value != null)
        {
            this.parameters.setProperty(name, value);
        }
    }
}

// End MockServletContext.java
