import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AssingmentTCPClient {

  private static int jobCreateTime;
  private static int jobID;
  private static int jobEstimate;
  private static int jobCore;
  private static int jobMem;
  private static int jobDisk;
  private static List<Server> listOfFilteredServers;
  private static String largestServerType;

  public static void main(String args[]) throws Exception {
    Socket s = new Socket("localhost", 50000);
    String[] arr = new String[] { "HELO", "AUTH shubham", "REDY" };
    DataOutputStream dout = new DataOutputStream(s.getOutputStream());
    int jobsSubmitted = 0;
    String str = "", str2 = "";
    while (!str.equals("QUIT")) {
      for (int i = 0; i < arr.length; i++) {
        str = arr[i];
        dout.write((str + "\n").getBytes());
        dout.flush();
        str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
        System.out.println("Server says: " + str2);
      }
      System.out.println("JOBN ==>" + str2);
      if (str2.contains("JOBN")) {
        parstJob(str2);
        String getsCapableString ="GETS Capable " + String.valueOf(jobCore) + " " + String.valueOf(jobMem) + " " + String.valueOf(jobDisk);
        System.out.println("Client Says: " + getsCapableString);
        dout.write((getsCapableString + "\n").getBytes());
        dout.flush();
        str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
        System.out.println("Server Says: " + str2);
        str = "OK";
        dout.write((str + "\n").getBytes());
        dout.flush();
        BufferedReader red = new BufferedReader(new InputStreamReader(s.getInputStream()));
        serverSegregation(red);
        System.out.println("LARGEST SERVER: " + largestServerType);
        System.out.println("Client Says: " + str);
        dout.write((str + "\n").getBytes());
        dout.flush();
        str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
        System.out.println("SERVER SAYS: " + str2);
        while (true) {
          for (Server ser : listOfFilteredServers) {
            String schedString = "SCHD " + jobID + " " + largestServerType + " " + ser.getServerID();
            System.out.println("Client Says: " + schedString);
            dout.write((schedString + "\n").getBytes());
            dout.flush();
            jobsSubmitted++;
            str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
            System.out.println("Server Says: " + str2);
            System.out.println("Client Says: REDY");
            dout.write("REDY\n".getBytes());
            dout.flush();
            str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
            System.out.println("Server says: " + str2);
            while(!str2.contains("JOBN") && !str2.contains("NONE")) {
                System.out.println("Client Says: REDY");
                dout.write("REDY\n".getBytes());
                dout.flush();
                str2 = new BufferedReader(new InputStreamReader(s.getInputStream())).readLine();
                System.out.println("Server Says: " + str2);
            }
            if(str2.contains("NONE")) {
                break;
            } else {
                parstJob(str2);
            }
          }
          if (str2.contains("NONE")) {
            break;
          }
        }
      }
      System.out.println(jobsSubmitted);
      str = "QUIT";
      dout.write((str + "\n").getBytes());
      dout.flush();
    }

    dout.close();
    s.close();
  }

  public static void serverSegregation(BufferedReader reader) throws IOException {
    System.out.println("Start of finding largest server logic:");
    List<Server> listOfUnfilteredServers = new ArrayList<Server>();
    while(reader.ready()) {
        String serverResponse = reader.readLine();
        System.out.println("SERVER INFO: " + serverResponse);
        listOfUnfilteredServers.add(serverInfo(serverResponse));
    }
    int largestCoreCount = 0;
    for(Server server: listOfUnfilteredServers) {
        if(server.getServerCore() > largestCoreCount) {
            largestCoreCount = server.getServerCore();
            largestServerType = server.getServerType();
        }
    }
    listOfFilteredServers = listOfUnfilteredServers.stream().filter(server -> server.getServerType().equalsIgnoreCase(largestServerType))
        .collect(Collectors.toList());
  }

  public static Server serverInfo(String serverString) {
    String[] serverInfoArray = serverString.split(" ");
    Server server = new Server();
    server.setServerType(serverInfoArray[0]);
    server.setServerID(Integer.parseInt(serverInfoArray[1]));
    server.setServerState(serverInfoArray[2]);
    server.setServerStartTime(serverInfoArray[3]);
    server.setServerCore(Integer.parseInt(serverInfoArray[4]));
    server.setServerMemory(Integer.parseInt(serverInfoArray[5]));
    server.setServerWJobs(Integer.parseInt(serverInfoArray[6]));
    server.setServerRJobs(Integer.parseInt(serverInfoArray[7]));
    return server;
  }

  public static void parstJob(String jobMsg) {
    String[] jobInfo = jobMsg.split(" ");
    jobCreateTime = Integer.parseInt(jobInfo[1]);
    jobID = Integer.parseInt(jobInfo[2]);
    jobEstimate = Integer.parseInt(jobInfo[3]);
    jobCore = Integer.parseInt(jobInfo[4]);
    jobMem = Integer.parseInt(jobInfo[5]);
    jobDisk = Integer.parseInt(jobInfo[6]);
  }
}

class Server {

  private String serverType;
  private int serverID;
  private String serverState;
  private String serverStartTime;
  private int serverCore;
  private int serverMemory;
  private int serverDisk;
  private int serverWJobs;
  private int serverRJobs;

  public void setServerType(String serverType) {
    this.serverType = serverType;
  }

  public void setServerID(int serverID) {
    this.serverID = serverID;
  }

  public void setServerState(String serverState) {
    this.serverState = serverState;
  }

  public void setServerStartTime(String serverStartTime) {
    this.serverStartTime = serverStartTime;
  }

  public void setServerCore(int serverCore) {
    this.serverCore = serverCore;
  }

  public void setServerMemory(int serverMemory) {
    this.serverMemory = serverMemory;
  }

  public void setServerDisk(int serverDisk) {
    this.serverDisk = serverDisk;
  }

  public void setServerWJobs(int serverWJobs) {
    this.serverWJobs = serverWJobs;
  }

  public void setServerRJobs(int serverRJobs) {
    this.serverRJobs = serverRJobs;
  }

  public String getServerType() {
    return serverType;
  }

  public int getServerID() {
    return serverID;
  }

  public String getServerState() {
    return serverState;
  }

  public String getServerStartTime() {
    return serverStartTime;
  }

  public int getServerCore() {
    return serverCore;
  }

  public int getServerMemory() {
    return serverMemory;
  }

  public int getServerDisk() {
    return serverDisk;
  }

  public int getServerWJobs() {
    return serverWJobs;
  }

  public int getServerRJobs() {
    return serverRJobs;
  }
}
