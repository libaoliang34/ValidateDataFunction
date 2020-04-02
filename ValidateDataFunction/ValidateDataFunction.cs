using System;
using System.Linq;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.WebJobs.Extensions.Storage;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System.Text;

namespace My.Functions
{
 public class Observation:TableEntity
    {
        public string TestCaseId { get; set; }
        public string TestCaseName { get; set; }
        public string Location { get; set; }
        public string ClientId { get; set; }
        public decimal Latency { get; set; }
        public string TestResult { get; set; }
        public string ProjectId { get; set; }
        public string ProjectName { get; set; }
        public string ProjectOwner { get; set; }
        public string ProjectAPIKey { get; set; }
        public DateTime TimeStamp { get; set; }
        public ResponseDetail ResponseDetail { get; set; }
    }

 

    public class ResponseDetail
    {
        public string ResourceType { get; set; }
        public string ResourceName { get; set; }
        public string Location { get; set; }

 

    }

    public static class HttpExample
    {
    [FunctionName("ValidateDataFunction")]
    public static async Task<IActionResult>  TableOutput(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req
        ,[Table("MyTable")] CloudTable cloudTable
        ,[EventHub("transmitdataeventhub",Connection="EventHubConnectionAppSetting")]IAsyncCollector<string> outputEvents,ILogger log)
    {
            string name = req.Query["name"];
            string requestBody = Encoding.UTF8.GetString(Encoding.Default.GetBytes(await new StreamReader(req.Body).ReadToEndAsync()));
            if(string.IsNullOrEmpty(name) && string.IsNullOrEmpty(requestBody))
            {
               return new BadRequestObjectResult("Please pass a name on the query string or in the request body");
            }
            if (string.IsNullOrEmpty(name))
            {
                if(!IsValidJson(requestBody)){
                    return new BadRequestObjectResult("Data verification failed, must be in JSON format");
                }
                Observation testCase = JsonConvert.DeserializeObject<Observation>(requestBody);
                if(string.IsNullOrEmpty(testCase.TestCaseId) ||string.IsNullOrEmpty(testCase.TestCaseName) ||string.IsNullOrEmpty(testCase.TestResult) || string.IsNullOrEmpty(testCase.ProjectId) || string.IsNullOrEmpty(testCase.ProjectAPIKey)
                ||string.IsNullOrEmpty(testCase.ProjectName)||string.IsNullOrEmpty(testCase.ProjectOwner )||string.IsNullOrEmpty(testCase.ClientId  )||string.IsNullOrEmpty(testCase.Latency+"")||string.IsNullOrEmpty(testCase.Location)
                ||string.IsNullOrEmpty(testCase.TimeStamp+"")||string.IsNullOrEmpty(testCase.ResponseDetail.Location)||string.IsNullOrEmpty(testCase.ResponseDetail.ResourceName)||string.IsNullOrEmpty(testCase.ResponseDetail.ResourceType))
                {
                  return new BadRequestObjectResult("All the data fields of body in the request cannot be empty");
                }
                // store the data of request body to storage Table 
                var TestCaseEntity = new Observations(){TestCaseId  =testCase.TestCaseId ,TestCaseName  =testCase.TestCaseName  ,Location  =testCase.Location  ,ClientId  =testCase.ClientId  ,Latency  =testCase.Latency  ,
                TestResult  =testCase.TestResult  ,ProjectId  =testCase.ProjectId  ,ProjectName  =testCase.ProjectName  ,ProjectOwner  =testCase.ProjectOwner  ,ProjectAPIKey  =testCase.ProjectAPIKey  ,TimeStamp  =testCase.TimeStamp  ,ResponseDetail =testCase.ResponseDetail };                 
                // Execute the operation. 将数据写入Table表中
                //await cloudTable.ExecuteAsync(TableOperation.InsertOrMerge(entity));  
                int flag=1;
                foreach (Observation item in cloudTable.ExecuteQuerySegmentedAsync(new TableQuery<Observation>(), null).Result)
                {
                    // Access table storage items here
                    if(TestCaseEntity.ProjectId .Equals(item.ProjectId )){
                        flag=2;
                        // Http请求达到后需要检索出其中的ProjectID and ProjectAPIKey字段，并在storage table中进行查询和比较，若匹配则将数据写入Event Hub。
                        if(TestCaseEntity.ProjectAPIKey .Equals(item.ProjectAPIKey )){
                                flag=0;
                                await outputEvents.AddAsync(JsonConvert.SerializeObject(TestCaseEntity));
                                break;
                        }
                    }
                }
                if(flag==1){
                    return new BadRequestObjectResult("The current projectid does not match in the storage table. Please check whether the projectd is correct");
                }else if(flag==2){
                    return new BadRequestObjectResult("The current apikey is not matched in the storage table. Please check whether apikey is correct");
                }
                return (ActionResult) new OkObjectResult(new {status = "success",Observation = TestCaseEntity}); 
            }
            else
            {
                return (ActionResult)new OkObjectResult($"Hello, {name}"); 
            }
    }


    /*json验证*/
    private static bool IsValidJson(string strInput)
{
    strInput = strInput.Trim();
    if ((strInput.StartsWith("{") && strInput.EndsWith("}")) || //For object
        (strInput.StartsWith("[") && strInput.EndsWith("]"))) //For array
    {
        try
        {
            var obj = JToken.Parse(strInput);
            return true;
        }
        catch (JsonReaderException jex)
        {
            //Exception in parsing json
            Console.WriteLine(jex.Message);
            return false;
        }
        catch (Exception ex) //some other exception
        {
            Console.WriteLine(ex.ToString());
            return false;
        }
    }
    else
    {
        return false;
    }
}public class Observations{
        public string TestCaseId { get; set; }
        public string TestCaseName { get; set; }
        public string Location { get; set; }
        public string ClientId { get; set; }
        public decimal Latency { get; set; }
        public string TestResult { get; set; }
        public string ProjectId { get; set; }
        public string ProjectName { get; set; }
        public string ProjectOwner { get; set; }
        public string ProjectAPIKey { get; set; }
        public DateTime TimeStamp { get; set; }
        public ResponseDetail ResponseDetail { get; set; }
    }
}

}