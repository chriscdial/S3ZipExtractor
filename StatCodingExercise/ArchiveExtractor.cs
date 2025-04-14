using System.IO.Compression;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Newtonsoft.Json;

namespace StatCodingExercise
{
    public class ArchiveExtractor
    {
        static string LocalAuthDataFile = "auth_data.json";
        static string MetadataStorageDir = "data/";
        static string PurchaseOrderDir = "by-po/";
        static string MetadataFileName = "ArchiveExtractorMetadata.json";

        private AuthData AUTH_DATA { get; set; }
        private AmazonS3Client S3Client { get; set; }

        public ArchiveExtractor() {
            EnsureAuthDataExists();
            AUTH_DATA = LoadAuthData();

            S3Client = new AmazonS3Client(AUTH_DATA.STORAGE.ACCESS_KEY, AUTH_DATA.STORAGE.SECRET, RegionEndpoint.USEast2);

            Console.WriteLine("ArchiveExtractor built");
        }

        public async Task Extract()
        {
            //await DeleteByPoFilesFromS3();
            //await DeleteMetadataFileFromS3();

            await EnsureMetadataDirExists();

            Console.WriteLine($"Querying objects in S3 Bucket: {AUTH_DATA.STORAGE.BUCKET}");

            ListObjectsResponse listObjectsResponse = S3Client.ListObjectsAsync(AUTH_DATA.STORAGE.BUCKET).Result;
            Console.WriteLine($"... {listObjectsResponse.S3Objects.Count} objects found in bucket");

            RuntimeMetadata runtimeMetadata = await LoadPreviousExecutionData(listObjectsResponse);

            List<S3Object> archives = listObjectsResponse.S3Objects.Where(obj => obj.Key.EndsWith(".zip")).ToList();
            Console.WriteLine($"... {archives.Count} objects of '.zip' extension");

            archives = archives.Where(archive => !runtimeMetadata.ArchiveInfo!.Any(a => a.Name == archive.Key)).ToList();
            Console.WriteLine($"{archives.Count} archives found which have not been processed");

            if (archives.Count > 0)
            {
                await EnsureByPoDirExists();

                Parallel.ForEach(archives, archive =>
                {
                    Console.WriteLine($"=== processing archive: {archive.Key} ===");
                    var request = new GetObjectRequest { BucketName = AUTH_DATA.STORAGE.BUCKET, Key = archive.Key };

                    var response = S3Client.GetObjectAsync(request).Result;
                    var zip = new ZipArchive(response.ResponseStream, ZipArchiveMode.Read);
                    Console.WriteLine($"... archive {archive.Key} contains {zip.Entries.Count} files to process");

                    runtimeMetadata.ArchiveInfo!.Add(new ArchiveInfo()
                    {
                        ExtractedOn = DateTime.UtcNow,
                        HashAtExtraction = archive.ETag,
                        Name = archive.Key,
                        FileInfo = zip.Entries.Select(file =>
                        {
                            return new FileInfo()
                            {
                                Name = file.Name,
                                ExtractedFromArchive = archive.Key
                            };
                        }).ToList()
                    });

                    var csv = zip.Entries.Where(doc => doc.Name.EndsWith(".csv")).Single();
                    Dictionary<string, List<string>> attachmentsByPoNumber = ParsePoNumberAttachmentRelationship(csv);                    

                    foreach (var attachment in zip.Entries.Where(doc => doc.Name.EndsWith(".pdf")))
                    {
                        Console.WriteLine($"... processing attachment {attachment.Name}");
                        foreach (var dictEntry in attachmentsByPoNumber.Where(dict => dict.Value.Contains(attachment.Name)))
                        {
                            Task task = UploadAttachmentToS3(dictEntry.Key, attachment);
                        }
                    }
                });

                await WriteMetadataToS3(runtimeMetadata);
            }
        }

        private static void EnsureAuthDataExists()
        {
            if (!File.Exists($"./{LocalAuthDataFile}"))
            {
                throw new FileNotFoundException($"{LocalAuthDataFile} file not found at project root. Refer to ArchiveExtractor.md for details");
            }
        }

        private static AuthData LoadAuthData()
        {
            using (StreamReader r = new StreamReader($"./{LocalAuthDataFile}"))
            {
                string json = r.ReadToEnd();
                return JsonConvert.DeserializeObject<AuthData>(json)!;
            }
        }

        private async Task UploadAttachmentToS3(string poNumDir, ZipArchiveEntry attachment )
        {
            var fileTransferUtility = new TransferUtility(S3Client);
            var attachmentKey = PurchaseOrderDir + poNumDir + "/" + attachment.Name;

            using (var fileToUpload = attachment.Open())
            {
                await fileTransferUtility.UploadAsync(fileToUpload, AUTH_DATA.STORAGE.BUCKET, attachmentKey);
            }
        }

        private Dictionary<string, List<string>> ParsePoNumberAttachmentRelationship(ZipArchiveEntry csvFile)
        {
            Dictionary<string, List<string>> attachmentsByPoNumber = new Dictionary<string, List<string>>();
            using (StreamReader reader = new StreamReader(csvFile.Open()))
            {
                string line = reader.ReadLine()!;
                var values = line.Split('~')!;

                int attachmentsColIndex = values.ToList().IndexOf("Attachment List");
                int poColIndex = values.ToList().IndexOf("PO Number");

                while (!reader.EndOfStream)
                {
                    line = reader.ReadLine()!;
                    values = line.Split('~')!;

                    var attachmentPaths = values[attachmentsColIndex].Split(',');
                    List<string> attachmentNames = attachmentPaths.Select(path => path.Split('/').Last()).ToList();

                    var poNumber = values[poColIndex].Trim() == string.Empty ? "Unfiled" : values[poColIndex];
                    if (attachmentsByPoNumber.ContainsKey(poNumber))
                    {
                        var poAttachments = attachmentsByPoNumber[poNumber];
                        poAttachments.AddRange(attachmentNames);
                    }
                    else
                    {
                        attachmentsByPoNumber.Add(poNumber, attachmentNames);
                    }
                }
            }

            return attachmentsByPoNumber;
        }

        private async Task<RuntimeMetadata> LoadPreviousExecutionData(ListObjectsResponse lor)
        {
            RuntimeMetadata runtimeMetadata;
            var metadataFile = lor.S3Objects.FirstOrDefault(obj => obj.Key == MetadataStorageDir + MetadataFileName);
            if (metadataFile == null)
            {
                runtimeMetadata = new RuntimeMetadata();
                runtimeMetadata.LastRuntime = DateTime.UtcNow;
                runtimeMetadata.ArchiveInfo = new List<ArchiveInfo>();
            }
            else
            {
                var request = new GetObjectRequest { BucketName = AUTH_DATA.STORAGE.BUCKET, Key = MetadataStorageDir + MetadataFileName };
                var response = await S3Client.GetObjectAsync(request);
                using (StreamReader reader = new StreamReader(response.ResponseStream))
                {
                    string json = reader.ReadToEnd();
                    runtimeMetadata = JsonConvert.DeserializeObject<RuntimeMetadata>(json)!;
                }
            }

            return runtimeMetadata;
        }

        private async Task WriteMetadataToS3(RuntimeMetadata metadata)
        {
            var objectRequest = new PutObjectRequest()
            {
                BucketName = AUTH_DATA.STORAGE.BUCKET,
                Key = MetadataStorageDir + MetadataFileName,
                ContentBody = JsonConvert.SerializeObject(metadata)
            };
            var reponse = await S3Client.PutObjectAsync(objectRequest);
        }

        private async Task EnsureMetadataDirExists()
        {
            await EnsureDirExists(MetadataStorageDir);
        }

        private async Task EnsureByPoDirExists()
        {
            await EnsureDirExists(PurchaseOrderDir);
        }

        private async Task EnsureDirExists(string dirPath)
        {
            var findDirRequest = new ListObjectsRequest();
            findDirRequest.BucketName = AUTH_DATA.STORAGE.BUCKET;
            findDirRequest.Prefix = dirPath;
            findDirRequest.MaxKeys = 1;

            ListObjectsResponse findDirResponse =
               await S3Client.ListObjectsAsync(findDirRequest);

            if (findDirResponse.S3Objects.Any())
                return;

            PutObjectRequest request = new PutObjectRequest()
            {
                BucketName = AUTH_DATA.STORAGE.BUCKET,
                StorageClass = S3StorageClass.Standard,
                ServerSideEncryptionMethod = ServerSideEncryptionMethod.None,
                Key = dirPath,
                ContentBody = string.Empty
            };

            PutObjectResponse response = await S3Client.PutObjectAsync(request);
        }

        private async Task DeleteByPoFilesFromS3()
        {
            var request = new ListObjectsV2Request { BucketName = AUTH_DATA.STORAGE.BUCKET };

            ListObjectsV2Response response;
            do
            {
                response = await S3Client.ListObjectsV2Async(request);

                //saftey-valve to ensure the .zips are never deleted
                var deleteObjs = response.S3Objects.Where(obj => !(obj.Key.EndsWith(".zip")));

                deleteObjs.Where(obj => obj.Key.StartsWith(PurchaseOrderDir)).ToList()
                    .ForEach(async obj => await S3Client.DeleteObjectAsync(AUTH_DATA.STORAGE.BUCKET, obj.Key));

                // If the response is truncated, set the request ContinuationToken
                // from the NextContinuationToken property of the response.
                request.ContinuationToken = response.NextContinuationToken;
            }
            while (response.IsTruncated);
        }

        private async Task DeleteMetadataFileFromS3()
        {
            await S3Client.DeleteObjectAsync(AUTH_DATA.STORAGE.BUCKET, MetadataStorageDir + MetadataFileName);
        }

        public class RuntimeMetadata
        {
            public DateTime LastRuntime { get; set; }
            public List<ArchiveInfo>? ArchiveInfo { get; set; }
        }

        public class ArchiveInfo
        {
            public DateTime ExtractedOn { get; set; }
            public string? HashAtExtraction { get; set; }
            public string? Name { get; set; }
            public List<FileInfo>? FileInfo { get; set; }
        }

        public class FileInfo
        {
            public string? Name { get; set; }
            public string? ExtractedFromArchive { get; set; }
        }        

        private class AuthData
        {
            public required STORAGE STORAGE { get; set; }
        }

        private class STORAGE
        {
            public required string REGION { get; set; }
            public required string BUCKET { get; set; }
            public required string ACCESS_KEY { get; set; }
            public required string SECRET { get; set; }
        }
    }
}
