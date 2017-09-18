using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService : EbBaseService
    {
        //public FileService(IMessageQueueClient _mqc, IMessageProducer _mqp) : base(_mqc, _mqp) { }
        public FileService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        [Authenticate]
        public bool Post(UploadFileRequest request)
        {
            try
            {
                this.MessageProducer3.Publish(new UploadFileMqRequest { FileName = request.FileName, ByteArray = request.ByteArray, TenantAccountId = request.TenantAccountId });
                return true;
            }
            catch (Exception e)
            {
                return false;
            }

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

        [Authenticate]
        public byte[] Post(DownloadFileRequest request)
        {
            return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(request.ObjectId);
        }
    }


}
