/*
// $Id: //open/mondrian/src/main/mondrian/tui/MockServletConfig.java#7 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */

package com.huawei.unibi.molap.engine.mondriantest;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;

/**
 * This is a partial implementation of the ServletConfig where just enough is
 * present to allow for communication between Mondrian's XMLA code and other
 * code in the same JVM. Currently it is used in both the CmdRunner and in XMLA
 * JUnit tests.
 * <p>
 * If you need to add to this implementation, please do so.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version $Id: //open/mondrian/src/main/mondrian/tui/MockServletConfig.java#7
 *          $
 */
public class MockServletConfig implements ServletConfig
{
    /**
     * 
     */
    private String servletName;

    /**
     * 
     */
    private Map<String, String> initParams;

    /**
     * 
     */
    private ServletContext servletContext;

    public MockServletConfig()
    {
        this(null);
    }

    public MockServletConfig(ServletContext servletContext)
    {
        this.initParams = new HashMap<String, String>();
        this.servletContext = servletContext;
    }

    /**
     * Returns the name of this servlet instance.
     * 
     */
    public String getServletName()
    {
        return servletName;
    }

    /**
     * Returns a reference to the ServletContext in which the servlet is
     * executing.
     * 
     */
    public ServletContext getServletContext()
    {
        return servletContext;
    }

    /**
     * Returns a String containing the value of the named initialization
     * parameter, or null if the parameter does not exist.
     * 
     */
    public String getInitParameter(String key)
    {
        return initParams.get(key);
    }

    /**
     * Returns the names of the servlet's initialization parameters as an
     * Enumeration of String objects, or an empty Enumeration if the servlet has
     * no initialization parameters.
     * 
     */
    public Enumeration getInitParameterNames()
    {
        return Collections.enumeration(initParams.keySet());
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    // ///////////////////////////////////////////////////////////////////////
    public void setServletName(String servletName)
    {
        this.servletName = servletName;
    }

    public void addInitParameter(String key, String value)
    {
        if(value != null)
        {
            this.initParams.put(key, value);
        }
    }

    public void setServletContext(ServletContext servletContext)
    {
        this.servletContext = servletContext;
    }
}

// End MockServletConfig.java
