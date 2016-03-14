/*
// $Id: //open/mondrian/src/main/mondrian/tui/MockHttpServletResponse.java#11 $
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
 */

package com.huawei.unibi.molap.engine.mondriantest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

/**
 * This is a partial implementation of the HttpServletResponse where just enough
 * is present to allow for communication between Mondrian's XMLA code and other
 * code in the same JVM. Currently it is used in both the CmdRunner and in XMLA
 * JUnit tests.
 * <p>
 * If you need to add to this implementation, please do so.
 * 
 * @author Richard M. Emberson
 * @version $Id:
 *          //open/mondrian/src/main/mondrian/tui/MockHttpServletResponse.java
 *          #11 $
 */
public class MockHttpServletResponse implements HttpServletResponse
{

    /**
     * 
     */
    public final static String DATE_FORMAT_HEADER = "EEE, d MMM yyyy HH:mm:ss Z";

    /**
     */
    private static class MockServletOutputStream extends ServletOutputStream
    {
        /**
         * 
         */
        private ByteArrayOutputStream buffer;

        /**
         * 
         */
        private String encoding;

        /**
         * @param size
         */
        MockServletOutputStream(int size)
        {
            this(size, "ISO-8859-1");
        }

        /**
         * @param size
         * @param encoding
         */
        MockServletOutputStream(int size, String encoding)
        {
            buffer = new ByteArrayOutputStream(size);
            this.encoding = encoding;
        }

        /**
         * @param encoding
         */
        public void setEncoding(String encoding)
        {
            this.encoding = encoding;
        }

        /**
         * 
         * @see java.io.OutputStream#write(int)
         */
        public void write(int value) throws IOException
        {
            buffer.write(value);
        }

        /**
         * @return
         * @throws IOException
         */
        public String getContent() throws IOException
        {
            try
            {
                buffer.flush();
                return buffer.toString(encoding);
            }
            catch(IOException exc)
            {
                throw exc;
            }
        }

        /**
         * @return
         * @throws IOException
         */
        public byte[] getBinaryContent() throws IOException
        {
            try
            {
                buffer.flush();
                return buffer.toByteArray();
            }
            catch(IOException exc)
            {
                throw exc;
            }
        }

        /**
         * 
         */
        public void clearContent()
        {
            buffer = new ByteArrayOutputStream();
        }
    }

    /**
     * 
     */
    private PrintWriter writer;

    /**
     * 
     */
    private Locale locale;

    /**
     * 
     */
    private String charEncoding;

    /**
     * 
     */
    private List<Cookie> cookies;

    /**
     * 
     */
    private MockServletOutputStream outputStream;

    /**
     * 
     */
    private int statusCode;

    /**
     * 
     */
    private boolean isCommited;

    /**
     * 
     */
    private String errorMsg;

    /**
     * 
     */
    private int errorCode;

    /**
     * 
     */
    private boolean wasErrorSent;

    /**
     * 
     */
    private boolean wasRedirectSent;

    /**
     * 
     */
    private int bufferSize;

    /**
     * 
     */
    private final Map<String, List<String>> headers;

    public MockHttpServletResponse()
    {
        this.isCommited = false;
        this.cookies = Collections.emptyList();
        this.bufferSize = 8192;
        this.charEncoding = "ISO-8859-1";
        this.errorCode = SC_OK;
        this.statusCode = SC_OK;
        this.headers = new HashMap<String, List<String>>();
        this.outputStream = new MockServletOutputStream(bufferSize);
    }

    /**
     * Returns the name of the charset used for the MIME body sent in this
     * response.
     * 
     */
    public String getCharacterEncoding()
    {
        return charEncoding;
    }

    /**
     * Returns a ServletOutputStream suitable for writing binary data in the
     * response.
     * 
     * @throws IOException
     */
    public ServletOutputStream getOutputStream() throws IOException
    {
        return outputStream;
    }

    /**
     * Returns a PrintWriter object that can send character text to the client.
     * 
     * @throws IOException
     */
    public PrintWriter getWriter() throws IOException
    {
        if(writer == null)
        {
            writer = new PrintWriter(new OutputStreamWriter(outputStream, charEncoding), true);
        }

        return writer;
    }

    public void setCharacterEncoding(String charEncoding)
    {
        this.charEncoding = charEncoding;
        this.outputStream.setEncoding(charEncoding);
    }

    /**
     * Sets the length of the content body in the response In HTTP servlets,
     * this method sets the HTTP Content-Length header.
     * 
     */
    public void setContentLength(int len)
    {
        setIntHeader("Content-Length", len);
    }

    /**
     * Sets the content type of the response being sent to the client.
     * 
     */
    public void setContentType(String contentType)
    {
        setHeader("Content-Type", contentType);
    }

    /**
     * Sets the preferred buffer size for the body of the response.
     * 
     */
    public void setBufferSize(int size)
    {
        this.bufferSize = size;
    }

    /**
     * Returns the actual buffer size used for the response.
     * 
     */
    public int getBufferSize()
    {
        return this.bufferSize;
    }

    /**
     * Forces any content in the buffer to be written to the client.
     * 
     * @throws IOException
     */
    public void flushBuffer() throws IOException
    {
        if(writer != null)
        {
            writer.flush();
        }
        outputStream.flush();
    }

    public void resetBuffer()
    {
        outputStream.clearContent();
    }

    /**
     * Returns a boolean indicating if the response has been committed.
     * 
     */
    public boolean isCommitted()
    {
        return isCommited;
    }

    /**
     * Clears any data that exists in the buffer as well as the status code and
     * headers.
     */
    public void reset()
    {
        headers.clear();
        resetBuffer();
    }

    /**
     * Sets the locale of the response, setting the headers (including the
     * Content-Type's charset) as appropriate.
     * 
     */
    public void setLocale(Locale locale)
    {
        this.locale = locale;
    }

    /**
     * Returns the locale assigned to the response.
     * 
     */
    public Locale getLocale()
    {
        return locale;
    }

    /**
     * Adds the specified cookie to the response.
     * 
     */
    public void addCookie(Cookie cookie)
    {
        if(cookies.isEmpty())
        {
            cookies = new ArrayList<Cookie>();
        }
        cookies.add(cookie);
    }

    /**
     * Returns a boolean indicating whether the named response header has
     * already been set.
     * 
     */
    public boolean containsHeader(String name)
    {
        return headers.containsKey(name);
    }

    /**
     * Encodes the specified URL by including the session ID in it, or, if
     * encoding is not needed, returns the URL unchanged.
     * 
     */
    public String encodeURL(String url)
    {
        return encode(url);
    }

    /**
     * Encodes the specified URL for use in the sendRedirect method or, if
     * encoding is not needed, returns the URL unchanged.
     * 
     */
    public String encodeRedirectURL(String url)
    {
        return encode(url);
    }

    /**
     * @deprecated Method encodeUrl is deprecated
     */

    public String encodeUrl(String s)
    {
        return encodeURL(s);
    }

    /**
     * @deprecated Method encodeRedirectUrl is deprecated
     */

    public String encodeRedirectUrl(String s)
    {
        return encodeRedirectURL(s);
    }

    /**
     * Sends an error response to the client using the specified status code and
     * descriptive message.
     * 
     */
    public void sendError(int code, String msg) throws IOException
    {
        this.errorCode = code;
        this.wasErrorSent = true;
        this.errorMsg = msg;
    }

    /**
     * Sends an error response to the client using the specified status.
     * 
     */
    public void sendError(int code) throws IOException
    {
        this.errorCode = code;
        this.wasErrorSent = true;
    }

    /**
     * Sends a temporary redirect response to the client using the specified
     * redirect location URL.
     * 
     */
    public void sendRedirect(String location) throws IOException
    {
        setHeader("Location", location);
        wasRedirectSent = true;
    }

    /**
     * Sets a response header with the given name and date-value.
     * 
     */
    public void setDateHeader(String name, long date)
    {
        Date dateValue = new Date(date);
        String dateString = DateFormat.getDateInstance().format(dateValue);
        setHeader(name, dateString);
    }

    /**
     * Adds a response header with the given name and date-value.
     * 
     */
    public void addDateHeader(String name, long date)
    {
        Date dateValue = new Date(date);
        String dateString = new SimpleDateFormat(DATE_FORMAT_HEADER, Locale.US).format(dateValue);
        addHeader(name, dateString);
    }

    /**
     * Sets a response header with the given name and value.
     * 
     */
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

    /**
     * Adds a response header with the given name and value.
     * 
     */
    public void addHeader(String name, String value)
    {
        List<String> valueList = headers.get(name);
        if(null == valueList)
        {
            valueList = new ArrayList<String>();
            headers.put(name, valueList);
        }
        valueList.add(value);
    }

    /**
     * Sets a response header with the given name and integer value.
     * 
     */
    public void setIntHeader(String name, int value)
    {
        String stringValue = Integer.toString(value);
        addHeader(name, stringValue);
    }

    /**
     * Adds a response header with the given name and integer value.
     * 
     */
    public void addIntHeader(String name, int value)
    {
        String stringValue = Integer.toString(value);
        addHeader(name, stringValue);
    }

    /**
     * Sets the status code for this response.
     * 
     */
    public void setStatus(int status)
    {
        this.statusCode = status;
    }

    /**
     * @deprecated Method setStatus is deprecated Deprecated. As of version 2.1,
     *             due to ambiguous meaning of the message parameter. To set a
     *             status code use setStatus(int), to send an error with a
     *             description use sendError(int, String). Sets the status code
     *             and message for this response.
     */
    public void setStatus(int status, String s)
    {
        setStatus(status);
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // implementation access
    //
    // ///////////////////////////////////////////////////////////////////////
    public byte[] toByteArray() throws IOException
    {
        return outputStream.getBinaryContent();
    }

    public String getHeader(String name)
    {
        List<String> list = getHeaderList(name);

        return ((list == null) || (list.size() == 0)) ? null : list.get(0);
    }

    public String getContentType()
    {
        return getHeader("Content-Type");
    }

    // ///////////////////////////////////////////////////////////////////////
    //
    // helpers
    //
    // ///////////////////////////////////////////////////////////////////////
    public List<String> getHeaderList(String name)
    {
        return headers.get(name);
    }

    public int getStatusCode()
    {
        return statusCode;
    }

    public int getErrorCode()
    {
        return errorCode;
    }

    public List getCookies()
    {
        return cookies;
    }

    public boolean wasErrorSent()
    {
        return wasErrorSent;
    }

    public boolean wasRedirectSent()
    {
        return wasRedirectSent;
    }

    /*
     * protected void clearHeaders() { this.headers.clear(); }
     */

    protected String encode(String s)
    {
        // TODO
        return s;
    }

}

// End MockHttpServletResponse.java
