using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security;
using System.Threading.Tasks;
using System.Xml;
using spClient = Microsoft.SharePoint.Client;

namespace Practical365AzureBatch
{
    class Program
    {
        // Azure Storage connection
        private const string AzStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=practical365batchartstor;AccountKey=On629sEVFIpa9FwEh7cRfOMAoLZabcdAdtCkpaS4Ve8VrpImLmfNPH+U5V2Wqoi9aZSJKZHI5CjL2A4jmbmKoQ==;EndpointSuffix=core.windows.net";
        private const string AzAppContainerName = "applicacionbatch"; // Container names MUST be lower-case !!
        private const string AzInputContainerName = "inputbatch";
        private const string AzOutputContainerName = "outputbatch";

        // Azure Batch connection
        private const string AzBatchAccountName = "practical365batchartbatc";
        private const string AzBatchAccountKey = "oOnpv0DwKEE0OC85ybTq3TcKz+XPSAKPRcIBmltK70Jfvi0aRPgkNvoklz595ZEQx37I+d/VILXc8zR2HY+5/w==";
        private const string AzBatchAccountUrl = "https://practical365batchartbatc.westeurope.batch.azure.com";
        private const string AzPoolIdName = "practical365batchpool";
        private const string AzJobIdName = "practical365batchjob";

        // SharePoint connection
        private const string SpSiteUrl = "https://m365x679411.sharepoint.com/sites/Test_ModernSiteColl";
        private const string SpListName = "Practical265ListForBatch";
        private const string SpUser = "admin@M365x539511.onmicrosoft.com";
        private const string SpPw = "MySecretPw";

        static void Main(string[] args)
        {
            CreateXmlItemsInSharePoint();  // Create XML files from the List Items
            MainAsync().Wait();            // Process the data in Azure Batch and Azure Storage

            Console.WriteLine("Ready");
        }

        static void CreateXmlItemsInSharePoint()
        {
            spClient.ClientContext spCtx = LoginCsom();

            spClient.List myList = spCtx.Web.Lists.GetByTitle(SpListName);
            spClient.ListItemCollection allItems = myList.GetItems(spClient.CamlQuery.CreateAllItemsQuery());
            spClient.FieldCollection myFields = myList.Fields;
            spCtx.Load(myFields);
            spCtx.Load(allItems);
            spCtx.ExecuteQuery();

            Dictionary<string, string> fieldValues = new Dictionary<string, string>();
            foreach (Microsoft.SharePoint.Client.ListItem oneItem in allItems)
            {
                MemoryStream myStream = new MemoryStream();

                XmlWriterSettings mySettings = new XmlWriterSettings
                {
                    Indent = true,
                    IndentChars = ("    "),
                    CloseOutput = true,
                    OmitXmlDeclaration = true
                };

                using (XmlWriter myWriter = XmlWriter.Create(myStream, mySettings))
                {
                    myWriter.WriteStartDocument(true);
                    myWriter.WriteStartElement("Item");

                    foreach (Microsoft.SharePoint.Client.Field oneField in myFields)
                    {
                        if (oneField.Hidden == false)
                        {
                            try
                            {
                                fieldValues.Add(oneField.Title, oneItem[oneField.Title].ToString());
                            }
                            catch
                            {
                                // In case there is more than one field with the same name
                            }
                        }
                    }

                    foreach (string oneKey in fieldValues.Keys)
                    {
                        myWriter.WriteStartElement(oneKey.Replace(" ", "_"));
                        myWriter.WriteString(fieldValues[oneKey]);
                        myWriter.WriteEndElement();
                    }

                    fieldValues.Clear();
                    myWriter.WriteEndElement();
                    myWriter.WriteEndDocument();
                    myWriter.Flush();

                    try
                    {
                        spClient.AttachmentCreationInformation attInfo = new spClient.AttachmentCreationInformation();
                        attInfo.FileName = oneItem["Title"] + ".xml";
                        attInfo.ContentStream = new MemoryStream(myStream.ToArray());
                        oneItem.AttachmentFiles.Add(attInfo);
                        spCtx.ExecuteQuery();
                    }
                    catch
                    {
                        // In case the attachment already exists
                    }
                }
            }
        }

        static spClient.ClientContext LoginCsom()
        {
            spClient.ClientContext rtnContext = new spClient.ClientContext(SpSiteUrl);
            SecureString securePw = new SecureString();
            foreach (char oneChar in SpPw)
            { securePw.AppendChar(oneChar); }
            rtnContext.Credentials = new spClient.SharePointOnlineCredentials(SpUser, securePw);
            return rtnContext;
        }

        private static async Task MainAsync()
        {
            // Prepare the Azure Storage            
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(AzStorageConnectionString);
            CloudBlobClient blobClient = await ConfigureAzureStorage(storageAccount);

            // Upload the XML files and processor EXE to Azure Storage (Item1 = inputFiles, Item2 = applicationFiles)
            Tuple<List<ResourceFile>, List<ResourceFile>> tupleResourceFiles =
                        await UploadFilesToAzureStorage(blobClient);
            
            // The SAS Container for the output results
            string outputContainerSasUrl = GetContainerSasUrl(blobClient, AzOutputContainerName,
                SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.List);

            // Create the BatchClient and process the files
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(AzBatchAccountUrl,
                        AzBatchAccountName, AzBatchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                // Create the Pool containing the Virtual Machines that execute the tasks
                await CreatePoolAsync(batchClient, AzPoolIdName, tupleResourceFiles.Item2);

                // Create the Job to execute the tasks
                await CreateJobAsync(batchClient, AzJobIdName, AzPoolIdName);

                // Add the Tasks to the Jobs
                await AddTasksAsync(batchClient, AzJobIdName, tupleResourceFiles.Item1, outputContainerSasUrl);

                // Monitor the Tasks giving a max execution wait time
                await MonitorTasks(batchClient, AzJobIdName, TimeSpan.FromMinutes(30));

                // Download the result files
                await DownloadBlobsFromContainerAsync(blobClient, AzOutputContainerName);

                // Delete the used Azure resources
                await CleanUpResources(blobClient, batchClient);
            }
        }

        private static async Task<CloudBlobClient> ConfigureAzureStorage(CloudStorageAccount storageAccount)
        {
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            await CreateContainerIfNotExistAsync(blobClient, AzAppContainerName);
            await CreateContainerIfNotExistAsync(blobClient, AzInputContainerName);
            await CreateContainerIfNotExistAsync(blobClient, AzOutputContainerName);

            return blobClient;
        }

        private static async Task CreateContainerIfNotExistAsync(CloudBlobClient blobClient, string containerName)
        {
            // Create one container in the Storage Blob
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            try
            {
                if (await container.CreateIfNotExistsAsync())
                {
                    Console.WriteLine("Container " + containerName + " created");
                }
                else
                {
                    Console.WriteLine("Container " + containerName + " already exists");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private static async Task<Tuple<List<ResourceFile>, List<ResourceFile>>> UploadFilesToAzureStorage(CloudBlobClient blobClient)
        {
            // Upload the executor and XML files to the Azure Storage
            // First, the XML files from the SharePoint List
            List<Tuple<string, Stream>> inputFileStreams = GetXmlFilesFromList();  // XML files (item1=Name, item2=Data)

            // Upload the XML files to Azure
            List<ResourceFile> inputFiles = await UploadFilesToContainerAsync(blobClient, AzInputContainerName, inputFileStreams);

            // Second, the files for the executor that can be found in the VS project
            Stream streamCreatePdf = new FileStream(typeof(CreatePdf.Program).Assembly.Location, FileMode.Open, FileAccess.Read);
            string compileDirectory = Path.GetDirectoryName(typeof(Practical365AzureBatch.Program).Assembly.Location);
            Stream streamWindowsStorage = new FileStream(compileDirectory + @"\Microsoft.WindowsAzure.Storage.dll", FileMode.Open, FileAccess.Read);
            Stream streamITextKernel = new FileStream(compileDirectory + @"\iText.Kernel.dll", FileMode.Open, FileAccess.Read);
            Stream streamITextLayout = new FileStream(compileDirectory + @"\iText.Layout.dll", FileMode.Open, FileAccess.Read);

            // A List with the executor files
            List<Tuple<string, Stream>> applicationFileStreams = new List<Tuple<string, Stream>>
            {
                Tuple.Create("CreatePdf.exe", streamCreatePdf),
                Tuple.Create("Microsoft.WindowsAzure.Storage.dll", streamWindowsStorage),
                Tuple.Create("itext.kernel.dll", streamITextKernel),
                Tuple.Create("itext.layout.dll", streamITextLayout)
            };

            // Upload the files of the executor to Azure
            List<ResourceFile> applicationFiles = await UploadFilesToContainerAsync(blobClient, AzAppContainerName, applicationFileStreams);

            return Tuple.Create(inputFiles, applicationFiles);
        }

        private static List<Tuple<string, Stream>> GetXmlFilesFromList()
        {
            List<Tuple<string, Stream>> inputFileStreams = new List<Tuple<string, Stream>>();  // XML files (item1=Name, item2=Data)

            spClient.ClientContext spCtx = LoginCsom();

            spClient.Web myWeb = spCtx.Web;
            spClient.FolderCollection myFolders = myWeb.Folders;
            spClient.List myList = spCtx.Web.Lists.GetByTitle(SpListName);
            spClient.ListItemCollection allItems = myList.GetItems(spClient.CamlQuery.CreateAllItemsQuery());
            spCtx.Load(myWeb);
            spCtx.Load(myFolders);
            spCtx.Load(allItems);
            spCtx.ExecuteQuery();

            foreach (spClient.ListItem oneItem in allItems)
            {
                spClient.AttachmentCollection allAttachments = oneItem.AttachmentFiles;
                spCtx.Load(allAttachments);
                spCtx.ExecuteQuery();

                foreach (spClient.Attachment oneAttachment in allAttachments)
                {
                    spClient.File myXmlFile = myWeb.GetFileByServerRelativeUrl(oneAttachment.ServerRelativeUrl);
                    spClient.ClientResult<Stream> myXmlData = myXmlFile.OpenBinaryStream();
                    spCtx.Load(myXmlFile);
                    spCtx.ExecuteQuery();

                    using (MemoryStream mStream = new MemoryStream())
                    {
                        if (myXmlData != null)
                        {
                            myXmlData.Value.CopyTo(mStream);
                            byte[] myBinFile = mStream.ToArray();
                            MemoryStream xmlStream = new MemoryStream(myBinFile);
                            inputFileStreams.Add(Tuple.Create(myXmlFile.Name, (Stream)xmlStream));
                        }
                    }
                }
            }

            return inputFileStreams;
        }

        private static async Task<List<ResourceFile>> UploadFilesToContainerAsync(CloudBlobClient blobClient, string inputContainerName, List<Tuple<string, Stream>> fileStreams)
        {
            // Upload all the files to the Blob container
            List<ResourceFile> resourceFiles = new List<ResourceFile>();

            foreach (Tuple<string, Stream> fileStream in fileStreams)
            {
                resourceFiles.Add(await UploadFileToContainerAsync(blobClient, inputContainerName, fileStream));
            }

            return resourceFiles;
        }

        private static async Task<ResourceFile> UploadFileToContainerAsync(CloudBlobClient blobClient, string containerName, Tuple<string, Stream> fileStream)
        {
            // Upload one Blob to the container and get its SAS (the Blob SAS, not the Container SAS)
            Console.WriteLine("Uploading file " + fileStream.Item1 + " to the container " + containerName);

            string blobName = fileStream.Item1;

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(blobName);
            await blobData.UploadFromStreamAsync(fileStream.Item2);

            // Properties of the Shared Access Signature (SAS): no initial time, SAS is direct valid
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write | SharedAccessBlobPermissions.List
            };

            // Build the SAS URL for the blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return ResourceFile.FromStorageContainerUrl(blobSasUri);
        }

        private static string GetContainerSasUrl(CloudBlobClient blobClient,
                        string containerName, SharedAccessBlobPermissions permissions)
        {
            // Shared Access Signature (SAS) properties: SAS is direct valid
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = permissions
            };

            // Construir el URL del SAS para el blob
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            string sasContainerToken = container.GetSharedAccessSignature(sasConstraints);

            // Retorna el URL del contenedor, incluyendo el token SAS
            return String.Format("{0}{1}", container.Uri, sasContainerToken);
        }

        private static async Task CreatePoolAsync(BatchClient batchClient, string poolId,
                                IList<ResourceFile> resourceFiles)
        {
            // Create a Pool of Virtual Machines
            Console.WriteLine("Creating the Pool " + poolId);

            CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetDedicatedComputeNodes: 2,       // 2 Virtual Machines
                    virtualMachineSize: "standard_d1_v2", // - https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-sizes-specs
                    cloudServiceConfiguration: new CloudServiceConfiguration(osFamily: "6"));  // - https://docs.microsoft.com/en-us/azure/cloud-services/cloud-services-guestos-update-matrix

            pool.StartTask = new StartTask()
            {
                CommandLine = "cmd /c (robocopy %AZ_BATCH_TASK_WORKING_DIR% %AZ_BATCH_NODE_SHARED_DIR%) " +
                                                                "^& IF %ERRORLEVEL% LEQ 1 exit 0",
                ResourceFiles = resourceFiles,
                WaitForSuccess = true
            };

            await pool.CommitAsync();
        }

        private static async Task CreateJobAsync(BatchClient batchClient, string jobId, string poolId)
        {
            // Create one Job
            Console.WriteLine("Creating Job " + jobId);

            CloudJob job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            await job.CommitAsync();
        }

        private static async Task<List<CloudTask>> AddTasksAsync(BatchClient batchClient,
                string jobId, List<ResourceFile> inputFiles, string outputContainerSasUrl)
        {
            // Initilize the Jobs
            Console.WriteLine("Adding " + inputFiles.Count + " tasks to the Job " + jobId);

            List<CloudTask> tasks = new List<CloudTask>();

            // Create each Task. The application is in the shared directory en %AZ_BATCH_NODE_SHARED_DIR%
            foreach (ResourceFile inputFile in inputFiles)
            {
                // ATTENTION: The names of "inputFile.FilePath" CANOT have spaces !!
                string taskId = "topNtask" + inputFiles.IndexOf(inputFile);
                string taskCommandLine = String.Format(
                    "cmd /c %AZ_BATCH_NODE_SHARED_DIR%\\CreatePdf.exe \"{0}\" \"{1}\"",
                    inputFile.FilePath, outputContainerSasUrl);

                CloudTask task = new CloudTask(taskId, taskCommandLine)
                {
                    ResourceFiles = new List<ResourceFile> { inputFile }
                };
                tasks.Add(task);
            }

            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);

            return tasks;
        }

        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            // Monitor the Tasks
            bool allTasksSuccessful = true;
            const string successMessage = "All Tasks are finished";
            const string failureMessage = "Some Tasks are not finished in the given time";

            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id");
            List<CloudTask> tasks = await batchClient.JobOperations.ListTasks(jobId, detail).ToListAsync();

            Console.WriteLine("Waiting for Tasks finishing. Timeout in " + timeout.ToString());

            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();
            bool timedOut = await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);

            if (timedOut)
            {
                allTasksSuccessful = false;

                await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);

                Console.WriteLine(failureMessage);
            }
            else
            {
                try
                {
                    await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
                }
                catch (Exception ex)
                {
                    allTasksSuccessful = false;
                    await batchClient.JobOperations.TerminateJobAsync(jobId, failureMessage);
                    Console.WriteLine(failureMessage);
                }
            }

            if (allTasksSuccessful)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, successMessage);
                detail.SelectClause = "id, executionInfo";

                foreach (CloudTask task in tasks)
                {
                    await task.RefreshAsync(detail);

                    if (task.ExecutionInformation.FailureInformation != null)
                    {
                        allTasksSuccessful = false;

                        Console.WriteLine("Attention: Task [{0}] has an error: {1}", task.Id, task.ExecutionInformation.FailureInformation.Message);
                    }
                    else if (task.ExecutionInformation.ExitCode != 0)
                    {
                        allTasksSuccessful = false;

                        Console.WriteLine("Attention: Task [{0}] has probably an execution error", task.Id);
                    }
                }
            }

            if (allTasksSuccessful)
            {
                Console.WriteLine("All Tasks completed");
            }

            return allTasksSuccessful;
        }

        private static async Task DownloadBlobsFromContainerAsync(CloudBlobClient blobClient,
                                                string containerName)
        {
            // Upload the PDF files to Azure and SharePoint
            Console.WriteLine("Uploading files to the container " + containerName);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            foreach (IListBlobItem item in container.ListBlobs(prefix: null,
                                                            useFlatBlobListing: true))
            {
                CloudBlob blob = (CloudBlob)item;

                // Save the content as a byte array
                blob.FetchAttributes();
                long fileByteLength = blob.Properties.Length;
                Byte[] outputFile = new Byte[fileByteLength];
                await blob.DownloadToByteArrayAsync(outputFile, 0);

                UploadAttachmentToSharePoint(outputFile, blob.Name);
            }

            Console.WriteLine("All PDF files uploaded to SharePoint");
        }

        private static void UploadAttachmentToSharePoint(byte[] outputFile, string fileName)
        {
            spClient.ClientContext spCtx = LoginCsom();

            spClient.List myList = spCtx.Web.Lists.GetByTitle(SpListName);
            spClient.ListItemCollection allItems = myList.GetItems(spClient.CamlQuery.CreateAllItemsQuery());
            spCtx.Load(allItems);
            spCtx.ExecuteQuery();

            foreach (spClient.ListItem oneItem in allItems)
            {
                if (fileName.Contains(oneItem["Title"].ToString()))
                {
                    spClient.AttachmentCreationInformation attInfo = new spClient.AttachmentCreationInformation();
                    attInfo.FileName = fileName;
                    attInfo.ContentStream = new MemoryStream(outputFile.ToArray());
                    oneItem.AttachmentFiles.Add(attInfo);
                    spCtx.ExecuteQuery();
                }
            }
        }

        private static async Task CleanUpResources(CloudBlobClient blobClient, BatchClient batchClient)
        {
            // Delete the Resources from Azure Storage
            await DeleteContainerAsync(blobClient, AzAppContainerName);
            await DeleteContainerAsync(blobClient, AzInputContainerName);
            await DeleteContainerAsync(blobClient, AzOutputContainerName);

            // Delete the Resources from Batch
            await batchClient.JobOperations.DeleteJobAsync(AzJobIdName);
            Console.WriteLine("Job " + AzJobIdName + " deleted");
            await batchClient.PoolOperations.DeletePoolAsync(AzPoolIdName);
            Console.WriteLine("Pool " + AzPoolIdName + " deleted");
        }

        private static async Task DeleteContainerAsync(CloudBlobClient blobClient, string containerName)
        {
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            if (await container.DeleteIfExistsAsync())
            {
                Console.WriteLine("Container " + containerName + " deleted");
            }
            else
            {
                Console.WriteLine("Container " + containerName + " doesn't exist");
            }
        }
    }
}
