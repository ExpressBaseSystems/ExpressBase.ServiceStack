using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using ServiceStack.Text;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService : EbBaseService
    {
        public FileService(IMessageProducer _mqp, IMessageQueueClient _mqc, IServerEvents _se) : base(_mqp, _mqc, _se) { }

        //[Route("/event-stream/null")]
        //public void Post(UploadFileControllerResponse request)
        //{

        //}

        [Authenticate]
        public string Post(UploadFileRequest request)
        {
            if (request.MetaData == null && request.MetaDataPair != null)
            {

                request.MetaData = new MongoDB.Bson.BsonDocument();

                request.MetaData.Add(request.MetaDataPair as IDictionary);
            }


            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest { FileName = request.FileName, ByteArray = request.ByteArray, TenantAccountId = request.TenantAccountId, MetaData = new BsonDocument(request.MetaData) , UserId = request.UserId});
                    return "Successfully Uploaded to MQ";
                }
                catch (Exception e)
                {
                    return "Failed to Uplaod to MQ";
                }
            }
            else
            {
                string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.MetaData).ToString();

                this.MessageProducer3.Publish(new ImageResizeMqRequest { ObjectId = Id, FileName = request.FileName, ImageByte = request.ByteArray, MetaData = new BsonDocument(request.MetaData), TenantAccountId = request.TenantAccountId, UserId = request.UserId });

                return Id;
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

            Response.FileList = new List<FilesInfo>();

            foreach (var element in filesList)
            {
                Response.FileList.Add(new FilesInfo { ObjectId = element.Id.ToString(), MetaData = new BsonDocument(element.Metadata) });
            }

            return Response;
        }

        [Restrict(InternalOnly = true)]
        public class FileServiceInternal : EbBaseService
        {
            public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IServerEvents _se) : base(_mqp, _mqc, _se) { }

            readonly List<string> ImageSizes = new[] { "100x100", "360x360", "640x640" }.ToList();

            public string Post(UploadFileMqRequest request)
            {
                try
                {
                    var id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.MetaData);
                    
                    //this.ServerEvents.NotifyUserId(request.UserId.ToString(), "FileUpload", new UploadFileControllerResponse { objId = id.ToString(), Uploaded = "OK"});
                }
                catch (Exception e) {

                }
                
                return null;
            }

            public string Post(ImageResizeMqRequest request)
            {
                MemoryStream ms = new MemoryStream(request.ImageByte);

                ms.Position = 0;

                var fileName = request.ObjectId + ".png";
                try
                {
                    using (Image img = Image.FromStream(ms))
                    {
                        foreach (string size in ImageSizes)
                        {
                            UploadFileMqRequest uploadFileRequest = new UploadFileMqRequest();

                            var filename = request.FileName.Split('.');

                            uploadFileRequest.FileName = fileName[0] + size + ".png";
                            uploadFileRequest.MetaData = new BsonDocument(request.MetaData);
                            uploadFileRequest.TenantAccountId = request.TenantAccountId;
                            uploadFileRequest.UserId = request.UserId;

                            var parts = size?.Split('x');
                            int width = img.Width;
                            int height = img.Height;

                            if (parts != null && parts.Length > 0)
                                int.TryParse(parts[0], out width);

                            if (parts != null && parts.Length > 1)
                                int.TryParse(parts[1], out height);

                            var ImgStream = Resize(img, width, height);

                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadFileRequest.ByteArray = request.ImageByte;
                            this.MessageProducer3.Publish(uploadFileRequest);
                        }
                    }
                }
                catch (Exception e)
                {

                }

                return null;
            }

            public static Stream Resize(Image img, int newWidth, int newHeight)
            {
                if (newWidth != img.Width || newHeight != img.Height)
                {
                    var ratioX = (double)newWidth / img.Width;
                    var ratioY = (double)newHeight / img.Height;
                    var ratio = Math.Max(ratioX, ratioY);
                    var width = (int)(img.Width * ratio);
                    var height = (int)(img.Height * ratio);

                    var newImage = new Bitmap(width, height);
                    Graphics.FromImage(newImage).DrawImage(img, 0, 0, width, height);
                    img = newImage;
                }

                var ms = new MemoryStream();
                img.Save(ms, ImageFormat.Png);
                ms.Position = 0;
                return ms;
            }
        }
    }

}
