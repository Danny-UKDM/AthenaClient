using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.Athena;
using Amazon.Athena.Model;

namespace AthenaClient
{
    public class Method2
    {
        private readonly AmazonAthenaClient _amazonAthenaClient;

        public Method2(AmazonAthenaClient amazonAthenaClient) =>
            _amazonAthenaClient = amazonAthenaClient;

        public async Task Execute()
        {
            using (var client = _amazonAthenaClient)
            {
                var qContext = new QueryExecutionContext { Database = "dannys_cool_database" };
                var resConf = new ResultConfiguration { OutputLocation = "dannys_cool_bucket" };

                Console.WriteLine("Created Athena Client");
                await Run(client, qContext, resConf);
            }
        }

        private static async Task Run(IAmazonAthena client, QueryExecutionContext qContext, ResultConfiguration resConf)
        {
            /* Execute a simple query on a table */
            var qReq = new StartQueryExecutionRequest
            {
                QueryString = "SELECT * FROM cloudtrail_logs limit 10;",
                QueryExecutionContext = qContext,
                ResultConfiguration = resConf
            };

            try
            {
                /* Executes the query in an async manner */
                var qRes = await client.StartQueryExecutionAsync(qReq);
                /* Call internal method to parse the results and return a list of key/value dictionaries */
                var items = await GetQueryExecution(client, qRes.QueryExecutionId);
                foreach (var item in items)
                {
                    foreach (var (key, value) in item)
                    {
                        Console.WriteLine("Col: {0}", key);
                        Console.WriteLine("Val: {0}", value);
                    }
                }
            }
            catch (InvalidRequestException e)
            {
                Console.WriteLine("Run Error: {0}", e.Message);
            }
        }

        private static async Task<List<Dictionary<string, string>>> GetQueryExecution(IAmazonAthena client, string id)
        {
            var items = new List<Dictionary<string, string>>();
            QueryExecution q = null;
            /* Declare query execution request object */
            var qReq = new GetQueryExecutionRequest { QueryExecutionId = id };
            /* Poll API to determine when the query completed */
            do
            {
                try
                {
                    var results = await client.GetQueryExecutionAsync(qReq);
                    q = results.QueryExecution;
                    Console.WriteLine("Status: {0}... {1}", q.Status.State, q.Status.StateChangeReason);

                    await Task.Delay(5000); //Wait for 5sec before polling again
                }
                catch (InvalidRequestException e)
                {
                    Console.WriteLine("GetQueryExec Error: {0}", e.Message);
                }
            } while (q != null && (q.Status.State == "RUNNING" || q.Status.State == "QUEUED"));

            if (q != null)
                Console.WriteLine("Data Scanned for {0}: {1} Bytes", id, q.Statistics.DataScannedInBytes);

            /* Declare query results request object */
            var resReq = new GetQueryResultsRequest()
            {
                QueryExecutionId = id,
                MaxResults = 10
            };

            GetQueryResultsResponse resResp;
            /* Page through results and request additional pages if available */
            do
            {
                resResp = await client.GetQueryResultsAsync(resReq);
                /* Loop over result set and create a dictionary with column name for key and data for value */
                foreach (var row in resResp.ResultSet.Rows)
                {
                    var dict = new Dictionary<string, string>();
                    for (var i = 0; i < resResp.ResultSet.ResultSetMetadata.ColumnInfo.Count; i++)
                    {
                        dict.Add(resResp.ResultSet.ResultSetMetadata.ColumnInfo[i].Name, row.Data[i].VarCharValue);
                    }
                    items.Add(dict);
                }

                if (resResp.NextToken != null)
                    resReq.NextToken = resResp.NextToken;

            } while (resResp.NextToken != null);

            /* Return List of dictionary per row containing column name and value */
            return items;
        }
    }
}
