/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
// $Id: //open/mondrian/src/main/mondrian/tui/MockHttpServletRequest.java#15 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */

package com.huawei.unibi.molap.engine.mondriantest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * Partial implementation of the {@link HttpServletRequest} where just enough is
 * present to allow for communication between Mondrian's XMLA code and other
 * code in the same JVM.
 * 
 * <p>
 * Currently it is used in both the CmdRunner and in XMLA JUnit tests. If you
 * need to add to this implementation, please do so.
 * 
 * @author Richard M. Emberson
 * @version $Id:
 *          //open/mondrian/src/main/mondrian/tui/MockHttpServletRequest.java#15
 *          $
 */
public class MockHttpServletRequest implements HttpServletRequest
{


    /**
     * 
     */
    public final static String DATE_FORMAT_HEADER = "EEE, d MMM yyyy HH:mm:ss Z";

    /**
      
     */
    private static class MockRequestDispatcher implements RequestDispatcher
    {
        /**
         * 
         */
        private ServletRequest forwardedRequest;

        /**
         * 
         */
        private ServletResponse forwardedResponse;

        /**
         * 
         */
        private ServletRequest includedRequest;

        /**
         * 
         */
        private ServletResponse includedResponse;

        /**
         * 
         */
        private String path;

        MockRequestDispatcher()
        {
        }

        public void setPath(String path)
        {
            this.path = path;
        }

        public String getPath()
        {
            return this.path;
        }

        public void forward(ServletRequest request, ServletResponse response) throws ServletException, IOException
        {
            this.forwardedRequest = request;
            this.forwardedResponse = response;
        }

        public void include(ServletRequest request, ServletResponse response) throws ServletException, IOException
        {
            this.includedRequest = request;
            this.includedResponse = response;
        }

        public ServletRequest getForwardedRequest()
        {
            return this.forwardedRequest;
        }

        public ServletResponse getForwardedResponse()
        {
            return this.forwardedResponse;
        }

        public ServletRequest getIncludedRequest()
        {
            return this.includedRequest;
        }

        public ServletResponse getIncludedResponse()
        {
            return this.includedResponse;
        }
    }

    /**
     
     */
    private static class MockServletInputStream extends ServletInputStream
    {
        /**
         * 
         */
        private ByteArrayInputStream stream;

        /**
         * @param data
         */
        MockServletInputStream(byte[] data)
        {
            stream = new ByteArrayInputStream(data);
        }

        /**
         * @see java.io.InputStream#read()
         */
        public int read() throws IOException
        {
            return stream.read();
        }
    }

    /**
     * 
     */
    private HttpSession session;

    // private ByteArrayInputStream bin;
    /**
     * 
     */
    private Map<String, String[]> parameters;

    /**
     * 
     */
    private Map<String, RequestDispatcher> requestDispatchers;

    /**
     * 
     */
    private List<Locale> locales;

    /**
     * 
     */
    private String serverName;

    /**
     * 
     */
    private String charEncoding;

    /**
     * 
     */
    private String method;

    /**
     * 
     */
    private String pathInfo;

    /**
     * 
     */
    private String pathTranslated;

    /**
     * 
     */
    private String contextPath;

    /**
     * 
     */
    private String queryString;

    /**
     * 
     */
    private String remoteUser;

    /**
     * 
     */
    private String requestedSessionId;

    /**
     * 
     */
    private String servletPath;

    /**
     * 
     */
    private String scheme;

    /**
     * 
     */
    private String localName;

    /**
     * 
     */
    private String localAddr;

    /**
     * 
     */
    private String authType;

    /**
     * 
     */
    private String protocol;

    /**
     * 
     */
    private String schema;

    /**
     * 
     */
    private Principal principal;

    /**
     * 
     */
    private List<Cookie> cookies;

    /**
     * 
     */
    private boolean requestedSessionIdIsFromCookie;

    /**
     * 
     */
    private int remotePort;

    /**
     * 
     */
    private int localPort;

    /**
     * 
     */
    private int serverPort;

    /**
     * 
     */
    private String remoteAddr;

    /**
     * 
     */
    private String remoteHost;

    /**
     * 
     */
    private Map<String, Object> attributes;

    /**
     * 
     */
    private final LinkedHashMap<String, List<String>> headers;

    /**
     * 
     */
    private boolean sessionCreated;

    /**
     * 
     */
    private String requestedURI;

    /**
     * 
     */
    private StringBuffer requestUrl;

    /**
     * 
     */
    private String bodyContent;

    /**
     * 
     */
    private Map<String, Boolean> roles;

    public MockHttpServletRequest()
    {
        this(new byte[0]);
    }

    public MockHttpServletRequest(byte[] bytes)
    {
        this(new String(bytes));
    }

    public MockHttpServletRequest(String bodyContent)
    {
        this.bodyContent = bodyContent;
        this.attributes = Collections.emptyMap();
        // this.bin = new ByteArrayInputStream(bytes);
        this.headers = new LinkedHashMap<String, List<String>>();
        this.requestDispatchers = new HashMap<String, RequestDispatcher>();
        this.parameters = new HashMap<String, String[]>();
        this.cookies = new ArrayList<Cookie>();
        this.locales = new ArrayList<Locale>();
        this.roles = new HashMap<String, Boolean>();
        this.requestedSessionIdIsFromCookie = true;
        this.method = "GET";
        this.protocol = "HTTP/1.1";
        this.serverName = "localhost";
        this.serverPort = 8080;
        this.scheme = "http";
        this.remoteHost = "localhost";
        this.remoteAddr = "127.0.0.1";
        this.localAddr = "127.0.0.1";
        this.localName = "localhost";
        this.localPort = 8080;
        this.remotePort = 5000;

        this.sessionCreated = false;
    }

    /**
     * Returns the value of the named attribute as an Object, or null if no
     * attribute of the given name exists.
     * 
     */
    public Object getAttribute(String name)
    {
        return this.attributes.get(name);
    }

    /**
     * to this request.
     * 
     */
    public Enumeration getAttributeNames()
    {
        return Collections.enumeration(attributes.keySet());
    }

    /**
     * Returns the name of the character encoding used in the body of this
     * request.
     * 
     */
    public String getCharacterEncoding()
    {
        return charEncoding;
    }

    /**
     *
     *
     */
    public void setCharacterEncoding(String charEncoding) throws UnsupportedEncodingException
    {
        this.charEncoding = charEncoding;
    }

    /**
     * Returns the length, in bytes, of the request body and made available by
     * the input stream, or -1 if the length is not known.
     * 
     */
    public int getContentLength()
    {
        return getIntHeader("Content-Length");
    }

    /**
     * Returns the MIME type of the body of the request, or null if the type is
     * not known.
     * 
     */
    public String getContentType()
    {
        return getHeader("Content-Type");
    }

    /**
     * Retrieves the body of the request as binary data using a
     * ServletInputStream.
     * 
     * @throws IOException
     */
    public ServletInputStream getInputStream() throws IOException
    {
        return new MockServletInputStream(bodyContent.getBytes());
    }

    /**
     * Returns the value of a request parameter as a String, or null if the
     * parameter does not exist.
     * 
     */
    public String getParameter(String name)
    {
        String[] values = getParameterValues(name);
        return (null != values && 0 < values.length) ? values[0] : null;
    }

    /**
     * Returns an Enumeration of String objects containing the names of the
     * parameters contained in this request.
     * 
     */
    public Enumeration getParameterNames()
    {
        return Collections.enumeration(parameters.keySet());
    }

    /**
     * Returns an array of String objects containing all of the values the given
     * request parameter has, or null if the parameter does not exist.
     * 
     */
    public String[] getParameterValues(String name)
    {
        return parameters.get(name);
    }

    /**
     * Returns the name and version of the protocol the request uses in the form
     * protocol/majorVersion.minorVersion, for example, HTTP/1.1.
     * 
     */
    public String getProtocol()
    {
        return protocol;
    }

    /**
     * Returns the name of the scheme used to make this request, for example,
     * http, https, or ftp.
     * 
     */
    public String getScheme()
    {
        return schema;
    }

    /**
     * Returns the host name of the server that received the request.
     * 
     */
    public String getServerName()
    {
        return serverName;
    }

    /**
     * Returns the port number on which this request was received.
     * 
     */
    public int getServerPort()
    {
        return serverPort;
    }

    /**
     * Retrieves the body of the request as character data using a
     * BufferedReader.
     * 
     * @throws IOException
     */
    public BufferedReader getReader() throws IOException
    {
        return (bodyContent == null) ? null : new BufferedReader(new StringReader(bodyContent));
    }

    /**
     * Returns the Internet Protocol (IP) address of the client that sent the
     * request.
     * 
     */
    public String getRemoteAddr()
    {
        return remoteAddr;
    }

    /**
     * Returns the fully qualified name of the client that sent the request, or
     * the IP address of the client if the name cannot be determined.
     * 
     */
    public String getRemoteHost()
    {
        return remoteHost;
    }

    /**
     * Stores an attribute in this request.
     * 
     */
    public void setAttribute(String name, Object obj)
    {
        if(attributes.equals(Collections.EMPTY_MAP))
        {
            attributes = new HashMap<String, Object>();
        }
        this.attributes.put(name, obj);
    }

    /**
     * Removes an attribute from this request.
     * 
     */
    public void removeAttribute(String name)
    {
        this.attributes.remove(name);
    }

    /**
     * Returns the preferred Locale that the client will accept content in,
     * based on the Accept-Language header.
     * 
     */
    public Locale getLocale()
    {
        return (locales.size() < 1) ? Locale.getDefault() : locales.get(0);
    }

    /**
     * Returns an Enumeration of Locale objects indicating, in decreasing order
     * starting with the preferred locale, the locales that are acceptable to
     * the client based on the Accept-Language header.
     * 
     */
    public Enumeration getLocales()
    {
        return Collections.enumeration(locales);
    }

    /**
     * Returns a boolean indicating whether this request was made using a secure
     * channel, such as HTTPS.
     * 
     */
    public boolean isSecure()
    {
        String scheme = getScheme();
        return (scheme == null) ? false : "https".equals(scheme);
    }

    /**
     * Returns a RequestDispatcher object that acts as a wrapper for the
     * resource located at the given path.
     * 
     */
    public RequestDispatcher getRequestDispatcher(String path)
    {
        RequestDispatcher dispatcher = requestDispatchers.get(path);
        if(dispatcher == null)
        {
            dispatcher = new MockRequestDispatcher();
            setRequestDispatcher(path, dispatcher);
        }
        return dispatcher;
    }

    /**
     * Deprecated. As of Version 2.1 of the Java Servlet API, use
     * ServletContext.getRealPath(java.lang.String) instead.
     * 
     * @deprecated Method getRealPath is deprecated
     * 
     */
    public String getRealPath(String path)
    {
        HttpSession session = getSession();
        return (session == null) ? null : session.getServletContext().getRealPath(path);
    }

    /**
     *
     *
     */
    public int getRemotePort()
    {
        return remotePort;
    }

    /**
     *
     *
     */
    public String getLocalName()
    {
        return localName;
    }

    /**
     *
     *
     */
    public String getLocalAddr()
    {
        return localAddr;
    }

    /**
     *
     *
     */
    public int getLocalPort()
    {
        return localPort;
    }

    /**
     * Returns the name of the authentication scheme used to protect the
     * servlet, for example, "BASIC" or "SSL," or null if the servlet was not
     * protected.
     * 
     */
    public String getAuthType()
    {
        return authType;
    }

    /**
     * Returns an array containing all of the Cookie objects the client sent
     * with this request.
     * 
     */
    public Cookie[] getCookies()
    {
        return cookies.toArray(new Cookie[cookies.size()]);
    }

    /**
     * Returns the value of the specified request header as a long value that
     * represents a Date object.
     * 
     */
    public long getDateHeader(String name)
    {
        String header = getHeader(name);
        if(header == null)
        {
            return -1;
        }
        try
        {
            Date dateValue = new SimpleDateFormat(DATE_FORMAT_HEADER, Locale.US).parse(header);
            return dateValue.getTime();
        }
        catch(ParseException exc)
        {
            throw new IllegalArgumentException(exc.getMessage());
        }
    }

    /**
     * Returns the value of the specified request header as a String.
     * 
     */
    public String getHeader(String name)
    {
        List<String> headerList = headers.get(name);

        return ((headerList == null) || (headerList.size() == 0)) ? null : headerList.get(0);
    }

    /**
     * Returns all the values of the specified request header as an Enumeration
     * of String objects.
     * 
     */
    public Enumeration getHeaders(String name)
    {
        List<String> headerList = headers.get(name);
        return (headerList == null) ? null : Collections.enumeration(headerList);
    }

    /**
     * Returns an enumeration of all the header names this request contains.
     * 
     */
    public Enumeration getHeaderNames()
    {
        return Collections.enumeration(headers.keySet());
    }

    /**
     * Returns the value of the specified request header as an int.
     * 
     */
    public int getIntHeader(String name)
    {
        String header = getHeader(name);
        return (header == null) ? -1 : Integer.parseInt(header);
    }

    /**
     * Returns the name of the HTTP method with which this request was made, for
     * example, GET, POST, or PUT.
     * 
     */
    public String getMethod()
    {
        return this.method;
    }

    /**
     * Returns any extra path information associated with the URL the client
     * sent when it made this request.
     * 
     */
    public String getPathInfo()
    {
        return pathInfo;
    }

    /**
     * Returns any extra path information after the servlet name but before the
     * query string, and translates it to a real path.
     * 
     */
    public String getPathTranslated()
    {
        return pathTranslated;
    }

    /**
     * Returns the portion of the request URI that indicates the context of the
     * request.
     * 
     */
    public String getContextPath()
    {
        return contextPath;
    }

    /**
     * Returns the query string that is contained in the request URL after the
     * path.
     * 
     */
    public String getQueryString()
    {
        return queryString;
    }

    /**
     * Returns the login of the user making this request, if the user has been
     * authenticated, or null if the user has not been authenticated.
     * 
     */
    public String getRemoteUser()
    {
        return remoteUser;
    }

    /**
     * Returns a boolean indicating whether the authenticated user is included
     * in the specified logical "role".
     * 
     */
    public boolean isUserInRole(String role)
    {
        return roles.get(role);
    }

    /**
     * Returns a java.security.Principal object containing the name of the
     * current authenticated user.
     * 
     */
    public Principal getUserPrincipal()
    {
        return principal;
    }

    /**
     * Returns the session ID specified by the client.
     * 
     */
    public String getRequestedSessionId()
    {
        HttpSession session = getSession();
        return (session == null) ? null : session.getId();
    }

    /**
     * Returns the part of this request's URL from the protocol name up to the
     * query string in the first line of the HTTP request.
     * 
     */
    public String getRequestURI()
    {
        return requestedURI;
    }

    /**
     *
     *
     */
    public StringBuffer getRequestURL()
    {
        return requestUrl;
    }

    /**
     * Returns the part of this request's URL that calls the servlet.
     * 
     */
    public String getServletPath()
    {
        return servletPath;
    }

    /**
     * Returns the current HttpSession associated with this request or, if if
     * there is no current session and create is true, returns a new session.
     * 
     */
    public HttpSession getSession(boolean create)
    {
        if(!create && !sessionCreated)
        {
            return null;
        }
        return getSession();
    }

    /**
     * Returns the current session associated with this request, or if the
     * request does not have a session, creates one.
     * 
     */
    public HttpSession getSession()
    {
        sessionCreated = true;
        return session;
    }

    /**
     * Checks whether the requested session ID is still valid.
     * 
     */
    public boolean isRequestedSessionIdValid()
    {
        HttpSession session = getSession();
        return (session != null);
    }

    /**
     * Checks whether the requested session ID came in as a cookie.
     * 
     */
    public boolean isRequestedSessionIdFromCookie()
    {
        return requestedSessionIdIsFromCookie;
    }

    /**
     * Checks whether the requested session ID came in as part of the request
     * URL.
     * 
     */
    public boolean isRequestedSessionIdFromURL()
    {
        return !requestedSessionIdIsFromCookie;
    }

    public boolean isRequestedSessionIdFromUrl()
    {
        // deprecated as of version 2.1 of Servlet API.
        return isRequestedSessionIdFromURL();
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    // ///////////////////////////////////////////////////////////////////////
    /*
     * public void setBytes(byte[] bytes) { this.bin = new
     * ByteArrayInputStream(bytes); }
     */
    /**
     *
     *
     */
    public Map getParameterMap()
    {
        return Collections.unmodifiableMap(parameters);
    }

    public void setServerName(String serverName)
    {
        this.serverName = serverName;
    }

    public void setRemoteHost(String remoteHost)
    {
        this.remoteHost = remoteHost;
    }

    public void setRemoteAddr(String remoteAddr)
    {
        this.remoteAddr = remoteAddr;
    }

    public void setMethod(String method)
    {
        this.method = method;
    }

    public void setPathInfo(String pathInfo)
    {
        this.pathInfo = pathInfo;
    }

    public void setPathTranslated(String pathTranslated)
    {
        this.pathTranslated = pathTranslated;
    }

    public void setContextPath(String contextPath)
    {
        this.contextPath = contextPath;
    }

    public void setQueryString(String queryString)
    {
        this.queryString = queryString;
    }

    public void setRemoteUser(String remoteUser)
    {
        this.remoteUser = remoteUser;
    }

    public void setRequestedSessionId(String requestedSessionId)
    {
        this.requestedSessionId = requestedSessionId;
    }

    public void setRequestURI(String requestedURI)
    {
        this.requestedURI = requestedURI;
    }

    public void setServletPath(String servletPath)
    {
        this.servletPath = servletPath;
    }

    public void setLocalName(String localName)
    {
        this.localName = localName;
    }

    public void setLocalAddr(String localAddr)
    {
        this.localAddr = localAddr;
    }

    public void setAuthType(String authType)
    {
        this.authType = authType;
    }

    public void setProtocol(String protocol)
    {
        this.protocol = protocol;
    }

    public void setScheme(String schema)
    {
        this.schema = schema;
    }

    public void setRemotePort(int remotePort)
    {
        this.remotePort = remotePort;
    }

    public void setLocalPort(int localPort)
    {
        this.localPort = localPort;
    }

    public void setServerPort(int serverPort)
    {
        this.serverPort = serverPort;
    }

    public void setContentType(String contentType)
    {
        setHeader("Content-Type", contentType);
    }

    public void setHeader(String name, String value)
    {
        List<String> valueList = headers.get(name);
        if(valueList == null)
        {
            valueList = new ArrayList<String>();
            headers.put(name, valueList);
        }
        valueList.add(value);
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // helpers
    //
    // ///////////////////////////////////////////////////////////////////////

    public void clearParameters()
    {
        parameters.clear();
    }

    public void setupAddParameter(String key, String[] values)
    {
        parameters.put(key, values);
    }

    public void setupAddParameter(String key, String value)
    {
        setupAddParameter(key, new String[]{value});
    }

    public void clearAttributes()
    {
        attributes.clear();
    }

    public void setSession(HttpSession session)
    {
        this.session = session;
    }

    public Map<String, RequestDispatcher> getRequestDispatcherMap()
    {
        return Collections.unmodifiableMap(requestDispatchers);
    }

    public void setRequestDispatcher(String path, RequestDispatcher dispatcher)
    {
        if(dispatcher instanceof MockRequestDispatcher)
        {
            ((MockRequestDispatcher)dispatcher).setPath(path);
        }
        requestDispatchers.put(path, dispatcher);
    }

    public void addLocale(Locale locale)
    {
        locales.add(locale);
    }

    public void addLocales(List<Locale> localeList)
    {
        locales.addAll(localeList);
    }

    public void addHeader(String key, String value)
    {
        List<String> valueList = headers.get(key);
        if(valueList == null)
        {
            valueList = new ArrayList<String>();
            headers.put(key, valueList);
        }
        valueList.add(value);
    }

    public void clearHeader(String key)
    {
        headers.remove(key);
    }

    public void setRequestURL(String requestUrl)
    {
        this.requestUrl = new StringBuffer(requestUrl);
    }

    public void setUserPrincipal(Principal principal)
    {
        this.principal = principal;
    }

    public void addCookie(Cookie cookie)
    {
        cookies.add(cookie);
    }

    public void setRequestedSessionIdFromCookie(boolean requestedSessionIdIsFromCookie)
    {
        this.requestedSessionIdIsFromCookie = requestedSessionIdIsFromCookie;
    }

    public void setUserInRole(String role, boolean isInRole)
    {
        roles.put(role, isInRole);
    }

    public void setBodyContent(byte[] data)
    {
        setBodyContent(new String(data));
    }

    public void setBodyContent(String bodyContent)
    {
        this.bodyContent = bodyContent;
    }

}

// End MockHttpServletRequest.java
