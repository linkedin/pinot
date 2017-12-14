export default function(server) {

  /*
    Seed your development database using your factories.
    This data will not be loaded in your tests.

    Make sure to define a factory for each model you want to create.
  */

  /**
   * Creates a mock anomaly on server start
   */
  server.createList('anomaly', 1);

  const fixtures = ['alertConfigs', 'anomalyFunctions', 'entityApplications', 'metrics', 'queryRelatedMetrics',
                      'timeseriesCompares'];
  server.loadFixtures(...fixtures);
}
