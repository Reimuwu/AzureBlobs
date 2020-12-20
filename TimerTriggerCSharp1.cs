using System;
using System.IO;
using System.Text;
using System.Net.Http;
using System.Net.Http.Headers; 
using System.Threading.Tasks;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage.Blob;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;
using BlobCloudStorageAccount = Microsoft.Azure.Storage.CloudStorageAccount;
using TableCloudStorageAccount = Microsoft.Azure.Cosmos.Table.CloudStorageAccount;
using BlobCredentials = Microsoft.Azure.Storage.Auth.StorageCredentials;


namespace Company.Function
{
    public static class TimerTriggerCSharp1
    {
        private const string URL = "https://api.publicapis.org/random?auth=null";
        
        [FunctionName("TimerTriggerCSharp1")]
        public static async Task Run([TimerTrigger("*/60 * * * * *")]TimerInfo myTimer, ILogger log)
        {
            //connect to the Azure Table
            var storageConnection = "redacted";
            var tableName = "ConnectionAttempts";
            var accountName = "fetchpublicapi";
            var accountKey = "redacted";


            StorageCredentials tableCredentials = new StorageCredentials(accountName, accountKey);
            BlobCredentials blobCredentials = new BlobCredentials(accountName, accountKey);

            TableCloudStorageAccount tableStorageAccount = new TableCloudStorageAccount(tableCredentials, useHttps: true);
            BlobCloudStorageAccount blobStorageAccount = new BlobCloudStorageAccount(blobCredentials, useHttps: true);

            CloudTableClient tableClient = tableStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(tableName);

            // Create a BlobServiceClient object which will be used to create a container client
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnection);
            
            string containerName = "payloaddata";

            // Create the container and return a container client object
            BlobContainerClient containerClient = new BlobContainerClient(storageConnection, containerName);


            
            
            using (HttpClient client = new HttpClient()) //Disposable
            {
                client.BaseAddress = new Uri(URL);
                HttpResponseMessage response = client.GetAsync(URL).Result;
                Console.WriteLine("Status code: "+(int)response.StatusCode);


                //Time of the connection in Ticks
                long ticks = DateTime.UtcNow.Ticks;
                //String of the ticks for the field in Azure Table
                string convTicks = ticks.ToString();
                //Converted Ticks to a date format
                string date = ConvertTime(ticks).ToString();

                string statusCode = response.StatusCode.ToString();

                string blobName = convTicks+".txt";
                
                //Add an Accept header for JSON format.
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));


                

                if (response.IsSuccessStatusCode)
                {
                    var dataObjects = response.Content.ReadAsStringAsync().Result;
                    dynamic obj = JsonConvert.DeserializeObject<dynamic>(dataObjects);
                    CloudBlobClient blobClient = blobStorageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                    // Return a reference to a blob to be created in the container.
                    CloudBlockBlob blob = container.GetBlockBlobReference(blobName);

                    try
                    {
                        TableInfo customer = new TableInfo(statusCode, date, convTicks);
                        AddToTable(table, customer).Wait();
                        MemoryStream msWrite = new MemoryStream(Encoding.UTF8.GetBytes(dataObjects));
                        msWrite.Position = 0;
                        using (msWrite)
                        {
                            await blob.UploadFromStreamAsync(msWrite);
                        }

                        Console.WriteLine("Write operation succeeded for {0}", storageConnection);
                        Console.WriteLine();
                        
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
                else
                {             
                    try
                    {
                        TableInfo customer = new TableInfo(statusCode, date, convTicks);
                        AddToTable(table, customer).Wait();
                    }
                    catch(Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                }
            }
        }


        public static string ConvertTime(long ticks)
        {
            DateTime myDate = new DateTime(ticks);
            String test = myDate.ToString("dd MMMM yyyy HH:mm:ss");

            return test;
        }


        public static async Task AddToTable(CloudTable table, TableInfo customer)
        {
            //Makes an insert/merge operation/command or w/e you wanna call it
            TableOperation insertOrMergeOp = TableOperation.InsertOrMerge(customer);

            //execute the operation
            TableResult result = await table.ExecuteAsync(insertOrMergeOp);
            TableInfo insertedCustomer = result.Result as TableInfo;

            Console.WriteLine("Row added.");
        }

    }



    public class TableInfo : TableEntity //Data
    {
        public string TimerTicks{get;set;}
        public TableInfo(string StatusCode, string Time, string Ticks)
        {
            PartitionKey = Time;
            RowKey = StatusCode;
            TimerTicks = Ticks;
        }

    } 
}
