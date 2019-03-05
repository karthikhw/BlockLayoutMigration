
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class BlockLayoutMigration {
  private static final String SEP = System.getProperty("file.separator");
  private static final String SUBDIR = "subdir";
  private static final String CURRENT = "current";
  private static final String BLK = "blk_";
  private static final String LAYOUT = "/current/finalized/";
  private static String BLOCKPOOL = "";
  private static String[] dataDir;
  private static int HANDELER = 10;
  public static Queue<File> blockQueue = new LinkedBlockingQueue<File>();


  public void usage() {
    System.out.println("# nohup <JAVA_HOME>/bin/java BlockLayoutMigration <comma-separated-datadir> <block-pool-id> [mover-threads]  &>/vat/tmp/block-layout-migration.log &");
    System.exit(-1);
  }


  static class MoveBlockExecutor implements Runnable {


    private int index = -1;
    private String volume = "";

    public MoveBlockExecutor(int index) {
      this.index = index;
    }

    @Override
    public void run() {
      while (!blockQueue.isEmpty()) {
        File source = blockQueue.poll();
        String blkName = source.getName();
        String[] blkPath = source.getAbsolutePath().split(SEP);
        String blkAbsPath = source.getAbsolutePath();
        int sourceSubdir1Pos = blkPath.length - 3;
        int sourceSubdir2Pos = blkPath.length - 2;
        if (blkName.contains(".meta")) {
          long blockId = Long.parseLong(blkName.split("_")[1]);
          int target1 = (int) ((blockId >> 16) & 0x1F);
          int target2 = (int) ((blockId >> 8) & 0x1F);
          String sourceSubDir1 = blkPath[sourceSubdir1Pos];
          String sourceSubDir2 = blkPath[sourceSubdir2Pos];
          String targetSubDir1 = SUBDIR + target1;
          String targetSubDir2 = SUBDIR + target2;

          if (!sourceSubDir1.equals(targetSubDir1) || !sourceSubDir2.equals(targetSubDir2)) {
            try{
            String path = blkAbsPath.split("subdir")[0];
            //Old Layout Meta file and Block Location
            File oldLayoutMetaFile = new File(path + SEP + sourceSubDir1 + SEP + sourceSubDir2 + SEP + blkName);
            File oldLayoutBlockFile = new File(path + SEP + sourceSubDir1 + SEP + sourceSubDir2 + SEP + BLK + blockId);
            //New Layout Meta file and Block Location
            File newLayoutDir = new File(path + SEP + targetSubDir1 + SEP + targetSubDir2);
            File newLayoutMetaFile = new File(path + SEP + targetSubDir1 + SEP + targetSubDir2 + SEP + blkName);
            File newLayoutBlockFile = new File(path + SEP + targetSubDir1 + SEP + targetSubDir2 + SEP + BLK + blockId);
            if (!newLayoutDir.exists()) {
              System.out.println("Creating new layout directory "+newLayoutDir);
              newLayoutDir.mkdirs();
            }
              if(!newLayoutMetaFile.exists()){
                //Move Meta
                Files.move(oldLayoutMetaFile.toPath(), newLayoutMetaFile.toPath());
                System.out.println("Moved meta file from old layout "+oldLayoutMetaFile +" to new layout "+newLayoutMetaFile);
                //Move Blk.
                Files.move(oldLayoutBlockFile.toPath(),newLayoutBlockFile.toPath());
                System.out.println("Moved block file from old layout "+oldLayoutBlockFile +" to new layout "+newLayoutBlockFile);
              }else{
                oldLayoutMetaFile.deleteOnExit();
                System.out.println("Deleted stale meta file from old layout "+oldLayoutMetaFile);
                oldLayoutBlockFile.deleteOnExit();
                System.out.println("Deleted stale block file from old layout "+oldLayoutBlockFile);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
  }

  static class MoveBlockProducer implements Runnable {

    private String volume = "";

    public MoveBlockProducer(String volume) {
      this.volume = volume + SEP + CURRENT + SEP + BLOCKPOOL + LAYOUT;
    }

    @Override
    public void run() {
populateQueue(new File(volume));
    }
    }

  public static void populateQueue(File vol) {
    try {
      File[] files = vol.listFiles();
      for (File file : files) {
        if (file.isDirectory()) {
          populateQueue(file);
        } else {
        blockQueue.offer(file);
        }
      }
    }catch (Exception e){
      e.printStackTrace();
    }
  }


  public static void main(String[] args) throws InterruptedException {
    BlockLayoutMigration blockLayoutMigration = new BlockLayoutMigration();
    try{

      if (args.length < 2) {
        blockLayoutMigration.usage();
      } else if (args.length == 3) {
        HANDELER = Integer.parseInt(args[2]);
      }
      BLOCKPOOL = args[1];
      dataDir = args[0].split(",");
      boolean isExist = true;
      for (int i = 0; i < dataDir.length; i++) {
        File dir = new File(dataDir[i]);
        File blockpool = new File(dataDir[i] + "/" + CURRENT + "/" + BLOCKPOOL);
        if (!dir.exists()) {
          System.out.println("Volume " + dir + " is not exist");
          isExist = false;
        }
        if (!blockpool.exists()) {
          System.out.println("Blook pool " + blockpool + " is not exist");
          isExist = false;
        }
      }
      if (!isExist) {
        System.exit(-1);
      }
    }catch (NumberFormatException e){
      e.printStackTrace();
      blockLayoutMigration.usage();
    }
      Thread[] producer = new Thread[dataDir.length];
      for (int i = 0; i < dataDir.length; i++) {
        producer[i] = new Thread(new MoveBlockProducer(dataDir[i]));
        producer[i].start();
      }
      Thread.sleep(3000);
      Thread[] executor = new Thread[HANDELER];
      for (int i = 0; i < HANDELER; i++) {
        executor[i] = new Thread(new MoveBlockExecutor(i));
        executor[i].setName("Handler-" + i);
        executor[i].start();
      }
    }
}

