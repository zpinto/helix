package org.apache.helix;

import java.util.Iterator;
import java.util.Set;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.rebalancer.waged.ReadOnlyWagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.ResourceValidationStage;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;

public class ControllerDebug {
  static String clusterName = "ESPRESSO_MSG";
  static String zkaddress = "localhost:2184";

  //  zk-ltx1-espresso.prod.linkedin.com:2181

  public static void main(String[] args) throws Exception {
    HelixManager manager = HelixManagerFactory
        .getZKHelixManager(clusterName, "Test", InstanceType.ADMINISTRATOR, zkaddress);
    manager.connect();
    process(0, manager);
    manager.disconnect();
  }

  protected static void process(double percent, HelixManager manager) {
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    event.addAttribute(AttributeName.changeContext.name(), new NotificationContext(manager));
    event.addAttribute(AttributeName.ControllerDataProvider.name(), new ResourceControllerDataProvider(clusterName));
    ResourceComputationStage computationStage = new ResourceComputationStage();
    ResourceValidationStage validationStage = new ResourceValidationStage();
    CurrentStateComputationStage currentStateComputationStage = new CurrentStateComputationStage();
    BestPossibleStateCalcStage bestPossibleStateCalcStage = new BestPossibleStateCalcStage();
    ReadClusterDataStage readClusterDataStage = new ReadClusterDataStage();
    IntermediateStateCalcStage intermediateStateCalcStage = new IntermediateStateCalcStage();

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(zkaddress));
    ClusterConfig clusterConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    ClusterConfig globalSyncClusterConfig = new ClusterConfig(clusterConfig.getRecord());
    globalSyncClusterConfig.setGlobalRebalanceAsyncMode(true);
    event.addAttribute(AttributeName.STATEFUL_REBALANCER.name(),
        new ReadOnlyWagedRebalancer(new ZkBucketDataAccessor(zkaddress), globalSyncClusterConfig.getClusterName(),
            globalSyncClusterConfig.getGlobalRebalancePreference()));

    System.out.print("First Pipeline");
    runStage(event, readClusterDataStage);
    System.out.println("Read Data Cache Complete");
    runStage(event, computationStage);
    System.out.println("Resource Computation Complete");
    runStage(event, validationStage);
    System.out.println("Resource validation Complete");
    runStage(event, currentStateComputationStage);
    System.out.println("Current state Complete");
    runStage(event, bestPossibleStateCalcStage);
    System.out.println("Best possible Complete");
    runStage(event, new MessageGenerationPhase());
    System.out.println("Message generation Complete");
    runStage(event, new MessageSelectionStage());
    System.out.println("Message selection Complete");
    runStage(event, intermediateStateCalcStage);
    System.out.println("Intermediate Complete");
  }

  protected static void runStage(ClusterEvent event, Stage stage) {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    try {
      stage.process(event);
    } catch (Exception e) {
      e.printStackTrace();
    }
    stage.postProcess();
  }
}