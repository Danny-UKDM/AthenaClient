using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;

namespace AthenaClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var athenaClient = new AmazonAthenaClient(new AmazonAthenaConfig { RegionEndpoint = RegionEndpoint.EUWest1 });

            var queryExecutionId = await SubmitAthenaQuery(athenaClient);

            await WaitForQueryToComplete(athenaClient, queryExecutionId);

            await ProcessResultRows(athenaClient, queryExecutionId);
        }

        private static async Task<string> SubmitAthenaQuery(AmazonAthenaClient athenaClient)
        {
            // Set the DB here
            var queryExecutionContext = new QueryExecutionContext { Database = "dannys_cool_database" };

            // Specify where the result should go in S3
            var resultConfiguration = new ResultConfiguration { OutputLocation = "dannys_cool_bucket" };

            // Send Execution Request to Athena to start query
            var startQueryExecutionRequest = new StartQueryExecutionRequest()
            {
                QueryString = "select count(*) as Count from dannys_cool_table where danny == lad",
                QueryExecutionContext = queryExecutionContext,
                ResultConfiguration = resultConfiguration
            };

            var startQueryExecutionResponse = await athenaClient.StartQueryExecutionAsync(startQueryExecutionRequest);
            return startQueryExecutionResponse.QueryExecutionId;
        }

        private static async Task WaitForQueryToComplete(IAmazonAthena athenaClient, string queryExecutionId)
        {
            var getQueryExecutionRequest = new GetQueryExecutionRequest { QueryExecutionId = queryExecutionId };

            var queryStillRunning = true;

            // Poll Athena for completion state of query.
            while (queryStillRunning)
            {
                var getQueryExecutionResponse = await athenaClient.GetQueryExecutionAsync(getQueryExecutionRequest);
                var queryState = getQueryExecutionResponse.QueryExecution.Status.State;

                if (queryState == QueryExecutionState.FAILED)
                    throw new Exception("Query Failed to run with Error Message: " + getQueryExecutionResponse.QueryExecution.Status.StateChangeReason);
                if (queryState == QueryExecutionState.CANCELLED)
                    throw new Exception("Query was cancelled.");
                if (queryState == QueryExecutionState.SUCCEEDED)
                    queryStillRunning = false;
                else
                {
                    // Sleep an amount of time before retrying again.
                    Thread.Sleep(TimeSpan.FromMilliseconds(1000));
                }

                Console.WriteLine("Current Status is: " + queryState);
            }
        }

        private static async Task ProcessResultRows(AmazonAthenaClient athenaClient, string queryExecutionId)
        {
            // Max Results can be set but if its not set, it will choose the maximum page size
            var getQueryResultsRequest = new GetQueryResultsRequest { QueryExecutionId = queryExecutionId };

            var getQueryResultsResponse = await athenaClient.GetQueryResultsAsync(getQueryResultsRequest);

            while (true)
            {
                var results = getQueryResultsResponse.ResultSet.Rows;
                foreach (var row in results)
                {
                    // Process the row. The first row of the first page holds the column names.
                    ProcessRow(row, getQueryResultsResponse.ResultSet.ResultSetMetadata.ColumnInfo);
                }

                // If nextToken is null, there are no more pages to read. Break out of the loop.
                if (string.IsNullOrEmpty(getQueryResultsResponse.NextToken))
                    break;

                getQueryResultsRequest.NextToken = getQueryResultsResponse.NextToken;
                getQueryResultsResponse = await athenaClient.GetQueryResultsAsync(getQueryResultsRequest);
            }
        }

        private static void ProcessRow(Row row, IEnumerable<ColumnInfo> columnInfoList)
        {
            foreach (var column in columnInfoList)
            {
                switch (column.Type)
                {
                    case "varchar":
                        // Convert and Process as String
                        break;
                    case "tinyint":
                        // Convert and Process as tinyint
                        break;
                    case "smallint":
                        // Convert and Process as smallint
                        break;
                    case "integer":
                        // Convert and Process as integer
                        break;
                    case "bigint":
                        // Convert and Process as bigint
                        break;
                    case "double":
                        // Convert and Process as double
                        break;
                    case "boolean":
                        // Convert and Process as boolean
                        break;
                    case "date":
                        // Convert and Process as date
                        break;
                    case "timestamp":
                        // Convert and Process as timestamp
                        break;
                    default:
                        throw new Exception($"Unexpected Type: {column.Type}");
                }
            }
        }
    }
}
