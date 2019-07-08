package dk.aau.cs.skytpf.task;

import java.io.InputStream;

public class SkylineParseResponseTask {
  private SkylineRequestTask skyTPFRequestTask;
  private InputStream responseStream;

  public SkylineParseResponseTask(SkylineRequestTask skyTPFRequestTask, InputStream responseStream) {
    this.skyTPFRequestTask = skyTPFRequestTask;
    this.responseStream = responseStream;
  }


  public SkylineRequestTask getSkyTPFRequestTask() {
    return skyTPFRequestTask;
  }


  public InputStream getResponseStream() {
    return responseStream;
  }
}
