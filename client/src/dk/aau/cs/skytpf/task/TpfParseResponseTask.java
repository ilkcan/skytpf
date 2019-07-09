package dk.aau.cs.skytpf.task;

import java.io.InputStream;

public class TpfParseResponseTask {
  private TpfHttpRequestTask httpRequestTask;
  private InputStream responseStream;

  public TpfParseResponseTask(TpfHttpRequestTask httpRequestTask, InputStream responseStream) {
    this.httpRequestTask = httpRequestTask;
    this.responseStream = responseStream;
  }


  public TpfHttpRequestTask getHttpRequestTask() {
    return httpRequestTask;
  }


  public InputStream getResponseStream() {
    return responseStream;
  }
}
