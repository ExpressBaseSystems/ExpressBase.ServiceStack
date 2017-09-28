using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService : EbBaseService
    {
        public FileService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        [Authenticate]
        public string Post(UploadFileRequest request)
        {
            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest { FileName = request.FileName, ByteArray = request.ByteArray, TenantAccountId = request.TenantAccountId });
                    return "Successfully Uploaded to MQ";
                }
                catch (Exception e)
                {
                    return "Failed to Uplaod to MQ";
                }
            }
            else
            {
                request.MetaData = new MongoDB.Bson.BsonDocument();

                request.MetaData.Add(request.metaDataPair as IDictionary);

                return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.MetaData).ToString();
            }
        }

        [Authenticate]
        public byte[] Post(DownloadFileRequest request)
        {
            return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(request.ObjectId);
        }

        [Authenticate]
        public FindFilesByTagResponse Post(FindFilesByTagRequest request)
        {
            var filesList = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.FindFilesByTags(request.Filter);

            FindFilesByTagResponse Response = new FindFilesByTagResponse();

            Response.FileList = new List<FileInfo>();

            foreach (var element in filesList)
            {
                Response.FileList.Add(new FileInfo { ObjectId = element.Id.ToString(), MetaData = new BsonDocument(element.Metadata) });
            }

            return Response;
        }

        [Restrict(InternalOnly = true)]
        public class FileServiceInternal : EbBaseService
        {
            public string Post(UploadFileMqRequest request)
            {
                var id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.MetaData);
                return null;
            }
        }
    }
}
