package dk.aau.cs.skytpf.task;

import java.io.InputStream;

public class BrtpfParseResponseTask {
  private BrtpfHttpRequestTask httpRequestTask;
  private InputStream responseStream;

  public BrtpfParseResponseTask(BrtpfHttpRequestTask httpRequestTask, InputStream responseStream) {
    this.httpRequestTask = httpRequestTask;
    this.responseStream = responseStream;
  }


  public BrtpfHttpRequestTask getHttpRequestTask() {
    return httpRequestTask;
  }


  public InputStream getResponseStream() {
    return responseStream;
  }
}
