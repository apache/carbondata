package org.apache.carbondata.horizon.rest.model.view;

public class Response {
  private long responseId;

  Response(Request request) {
    this.responseId = request.getRequestId();
  }

  public long getResponseId() {
    return responseId;
  }
}
