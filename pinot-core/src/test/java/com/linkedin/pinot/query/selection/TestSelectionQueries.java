package com.linkedin.pinot.query.selection;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.common.utils.NamedThreadFactory;
import com.linkedin.pinot.core.block.query.IntermediateResultsBlock;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BDocIdSetOperator;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.operator.query.AggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationFunctionGroupByOperator;
import com.linkedin.pinot.core.operator.query.MAggregationGroupByOperator;
import com.linkedin.pinot.core.operator.query.MSelectionOperator;
import com.linkedin.pinot.core.plan.Plan;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV0;
import com.linkedin.pinot.core.plan.maker.PlanMaker;
import com.linkedin.pinot.core.query.aggregation.CombineService;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.reduce.DefaultReduceService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentColumnarMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestSelectionQueries {

  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File("TestSelectionQueries");
  private static File INDEXES_DIR = new File("TestSelectionQueries_LIST");
  private static String SEGMENT_ID = "test_testTable_15544_15544_";

  public static IndexSegment _indexSegment;
  private static List<IndexSegment> _indexSegmentList;

  public Map<String, DictionaryReader> _dictionaryMap;
  public Map<String, SegmentMetadataImpl> _medataMap;

  @BeforeClass
  public void setup() throws Exception {
    setupSegment();
    File indexSegmentDir = new File(INDEX_DIR, SEGMENT_ID);
    _indexSegment = ColumnarSegmentLoader.load(indexSegmentDir, ReadMode.heap);
    _dictionaryMap = ((IndexSegmentImpl) _indexSegment).getDictionaryMap();
    _medataMap =
        ((SegmentColumnarMetadata) ((IndexSegmentImpl) _indexSegment).getSegmentMetadata()).getColumnMetadataMap();
    _indexSegmentList = new ArrayList<IndexSegment>();
  }

  @AfterClass
  public void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
  }

  private void setupSegment() throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }

  private void setupSegmentList(int numberOfSegments) throws Exception {
    final String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEXES_DIR.exists()) {
      FileUtils.deleteQuietly(INDEXES_DIR);
    }
    INDEXES_DIR.mkdir();

    for (int i = 0; i < numberOfSegments; ++i) {
      final File segmentDir = new File(INDEXES_DIR, "segment_" + i);

      final SegmentGeneratorConfig config =
          SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), segmentDir,
              "daysSinceEpoch", SegmentTimeUnit.days, "test", "testTable");

      final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
      driver.init(config);
      driver.build();

      System.out.println("built at : " + segmentDir.getAbsolutePath());
      _indexSegmentList.add(ColumnarSegmentLoader.load(new File(segmentDir, SEGMENT_ID), ReadMode.heap));
    }
  }

  @Test
  public void testAggregationGroupBys() {
    final List<AggregationFunctionGroupByOperator> aggregationFunctionGroupByOperatorList =
        new ArrayList<AggregationFunctionGroupByOperator>();
    final BDocIdSetOperator docIdSetOperator = new BDocIdSetOperator(null, _indexSegment, 5000);
    final Map<String, DataSource> dataSourceMap = getDataSourceMap();

    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    Selection selection = getSelectionQuery();

    MSelectionOperator selectionOperator = new MSelectionOperator(_indexSegment, selection, projectionOperator);

    final IntermediateResultsBlock block = (IntermediateResultsBlock) selectionOperator.nextBlock();
    PriorityQueue pq = block.getSelectionResult();
    DataSchema dataSchema = block.getSelectionDataSchema();
    System.out.println(dataSchema);
    while (!pq.isEmpty()) {
      Serializable[] row = (Serializable[]) pq.poll();
      System.out.println(getRowFromSerializable(row, dataSchema));
    }
  }

  private String getRowFromSerializable(Serializable[] row, DataSchema dataSchema) {
    String rowString = "";
    if (dataSchema.getColumnType(0) == DataType.STRING) {
      rowString += (String) row[0];
    } else {
      rowString += row[0];
    }
    for (int i = 1; i < row.length; ++i) {
      if (dataSchema.getColumnType(i) == DataType.STRING) {
        rowString += " : " + (String) row[i];
      } else {
        rowString += " : " + row[i];
      }
    }
    return rowString;
  }

  private Selection getSelectionQuery() {
    Selection selection = new Selection();
    List<String> selectionColumns = new ArrayList<String>();
    selectionColumns.add("dim_memberGender");
    selectionColumns.add("dim_memberIndustry");
    selectionColumns.add("met_impressionCount");
    selection.setSelectionColumns(selectionColumns);
    selection.setOffset(0);
    selection.setSize(10);
    List<SelectionSort> selectionSortSequence = new ArrayList<SelectionSort>();
    SelectionSort selectionSort = new SelectionSort();
    selectionSort.setColumn("dim_memberGender");
    selectionSort.setIsAsc(true);
    selectionSortSequence.add(selectionSort);
    selection.setSelectionSortSequence(selectionSortSequence);
    return selection;
  }

  @Test
  public void testInnerSegmentPlanMakerForSelectionNoFilter() throws Exception {
    final BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final PlanNode rootPlanNode = instancePlanMaker.makeInnerSegmentPlan(_indexSegment, brokerRequest);
    rootPlanNode.showTree("");
    // UAggregationGroupByOperator operator = (UAggregationGroupByOperator) rootPlanNode.run();
    final MAggregationGroupByOperator operator = (MAggregationGroupByOperator) rootPlanNode.run();
    final IntermediateResultsBlock resultBlock = (IntermediateResultsBlock) operator.nextBlock();
    System.out.println("RunningTime : " + resultBlock.getTimeUsedMs());
    System.out.println("NumDocsScanned : " + resultBlock.getNumDocsScanned());
    System.out.println("TotalDocs : " + resultBlock.getTotalDocs());

    final SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(brokerRequest.getSelections(), resultBlock.getSelectionDataSchema());

    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:1111"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:2222"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:3333"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:4444"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:5555"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:6666"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:7777"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:8888"), resultBlock.getDataTable());
    instanceResponseMap.put(new ServerInstance("localhost:9999"), resultBlock.getDataTable());
    final PriorityQueue<Serializable[]> reducedResults =
        selectionOperatorService.reduce(instanceResponseMap);
    //    System.out.println("********************************");
    //    for (int i = 0; i < reducedResults.size(); ++i) {
    //      Map<String, Serializable> groupByResult = reducedResults.get(i);
    //      System.out.println(groupByResult);
    //    }
    //    System.out.println("********************************");
    final JSONObject jsonResult = selectionOperatorService.render(reducedResults);
    System.out.println(jsonResult);
  }

  @Test
  public void testInterSegmentSelectionPlanMakerAndRun() throws Exception {
    final int numSegments = 20;
    setupSegmentList(numSegments);
    final PlanMaker instancePlanMaker = new InstancePlanMakerImplV0();
    final BrokerRequest brokerRequest = getSelectionNoFilterBrokerRequest();
    final ExecutorService executorService = Executors.newCachedThreadPool(new NamedThreadFactory("test-plan-maker"));
    final Plan globalPlan = instancePlanMaker.makeInterSegmentPlan(_indexSegmentList, brokerRequest, executorService);
    globalPlan.print();
    globalPlan.execute();
    final DataTable instanceResponse = globalPlan.getInstanceResponse();
    System.out.println(instanceResponse);

    final DefaultReduceService defaultReduceService = new DefaultReduceService();
    final Map<ServerInstance, DataTable> instanceResponseMap = new HashMap<ServerInstance, DataTable>();
    instanceResponseMap.put(new ServerInstance("localhost:0000"), instanceResponse);
    final BrokerResponse brokerResponse = defaultReduceService.reduceOnDataTable(brokerRequest, instanceResponseMap);
    System.out.println(new JSONArray(brokerResponse.getAggregationResults()));
    System.out.println("Time used : " + brokerResponse.getTimeUsedMs());
  }

  private static Map<String, DataSource> getDataSourceMap() {
    final Map<String, DataSource> dataSourceMap = new HashMap<String, DataSource>();
    dataSourceMap.put("dim_memberGender", _indexSegment.getDataSource("dim_memberGender"));
    dataSourceMap.put("dim_memberIndustry", _indexSegment.getDataSource("dim_memberIndustry"));
    dataSourceMap.put("met_impressionCount", _indexSegment.getDataSource("met_impressionCount"));
    return dataSourceMap;
  }

  private BrokerRequest getSelectionNoFilterBrokerRequest() {
    BrokerRequest brokerRequest = new BrokerRequest();
    brokerRequest.setSelections(getSelectionQuery());
    return brokerRequest;
  }

}
