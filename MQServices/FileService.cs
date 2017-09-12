using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService: EbBaseService
    {
        public FileService(IMessageQueueClient _mqc, IMessageProducer _mqp) : base(_mqc, _mqp) { }

        [Authenticate]
        public bool Post(UploadFileRequest request)
        {
            this.MessageProducer2.Publish(new UploadFileMqRequest { FileName=request.FileName, ByteArray = request.ByteArray, TenantAccountId= request.TenantAccountId });
            return true;
        }
    }

    [Restrict(InternalOnly = true)]
    public class FileServiceInternal : EbBaseService
    {
        public ObjectId Post(UploadFileMqRequest request)
        {
            return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.MetaData);
        }
    }
}
