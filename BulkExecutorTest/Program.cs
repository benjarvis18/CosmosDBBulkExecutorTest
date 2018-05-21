using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkExecutorTest
{
    class Program
    {
        private static readonly string EndpointUrl = "<Your Endpoint URL>";
        private static readonly string AuthKey = "<Your Auth Key>";
        private static readonly string DatabaseName = "BulkExecutorTest";
        private static readonly string CollectionName = "Product";

        private static readonly string SqlConnectionString = "<Your SQL Connection String>";

        static void Main(string[] args)
        {
            BulkLoadDocumentsAsync().Wait();
        }

        static async Task BulkLoadDocumentsAsync()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            // Set up Cosmos DB bulk executor
            var connectionPolicy = new ConnectionPolicy()
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            var client = new DocumentClient(new Uri(EndpointUrl), AuthKey, connectionPolicy);

            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            var collection = await client.ReadDocumentCollectionAsync(UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName));

            var bulkExecutor = new BulkExecutor(client, collection);
            await bulkExecutor.InitializeAsync();

            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            // Get data from SQL and serialize to JSON
            var documents = new List<string>();

            Console.WriteLine("Reading documents from SQL.");

            using (var sqlConnection = new SqlConnection(SqlConnectionString))
            {
                using (SqlCommand command = new SqlCommand("SELECT * FROM dbo.Products", sqlConnection))
                {
                    await sqlConnection.OpenAsync();

                    using (var sqlDataReader = await command.ExecuteReaderAsync())
                    {
                        while (sqlDataReader.Read())
                        {
                            var item = new Dictionary<string, object>(sqlDataReader.FieldCount - 1);

                            for (var i = 0; i < sqlDataReader.FieldCount; i++)
                            {
                                var fieldName = sqlDataReader.GetName(i);

                                if (fieldName.Contains("."))
                                {
                                    var split = fieldName.Split('.');

                                    if (!item.ContainsKey(split[0]))
                                        item[split[0]] = new Dictionary<string, object>();

                                    (item[split[0]] as Dictionary<string, object>).Add(split[1], sqlDataReader.GetValue(i));
                                }
                                else
                                {
                                    item[fieldName] = sqlDataReader.GetValue(i);
                                }
                            }

                            var json = JsonConvert.SerializeObject(item, Formatting.Indented);
                            documents.Add(json);
                        }
                    }
                }
            }

            // Do the bulk import
            Console.WriteLine("Performing bulk import.");

            var result = await bulkExecutor.BulkImportAsync(documents);
            stopwatch.Stop();

            Console.WriteLine(JsonConvert.SerializeObject(result));
            Console.WriteLine($"Time Taken (s): {stopwatch.Elapsed.TotalSeconds}");
            Console.Read();
        }
    }
}
