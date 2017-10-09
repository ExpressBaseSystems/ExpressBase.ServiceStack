using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;
using System.Linq;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService : EbBaseService
    {
        public FileService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        //[Route("/event-stream/null")]
        //public void Post(UploadFileControllerResponse request)
        //{

        //}

        [Authenticate]
        public string Post(UploadFileRequest request)
        {
            EbSolutionConnections SolutionConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

            request.IsAsync = false;

            if (request.MetaData == null && request.MetaDataPair != null)
            {
                request.MetaData = new BsonDocument();

                request.MetaData.AddRange(request.MetaDataPair as IDictionary);
            }

            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest { FileName = request.FileName, ByteArray = request.ByteArray, BucketName = request.BucketName, TenantAccountId = request.TenantAccountId, MetaData = new BsonDocument(request.MetaData), UserId = request.UserId });
                    return "Successfully Uploaded to MQ";
                }
                catch (Exception e)
                {
                    return "Failed to Uplaod to MQ";
                }
            }

            else if (!request.IsAsync)
            {
                string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.BucketName, request.MetaData).ToString();

                this.MessageProducer3.Publish(new ImageResizeMqRequest { ObjectId = Id, FileName = request.FileName, ImageByte = request.ByteArray, MetaData = new BsonDocument(request.MetaData), BucketName = request.BucketName, TenantAccountId = request.TenantAccountId, UserId = request.UserId });

                return Id;
            }

            return "Uploading Failed check the data";
        }

        [Authenticate]
        public byte[] Post(DownloadFileRequest request)
        {
            var FileNameParts = request.FileName.Substring(0, request.FileName.IndexOf('.'))?.Split('_');

            if (FileNameParts.Length == 1)
            {
                if (FileNameParts[0] != null)
                {
                    return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(new ObjectId(FileNameParts[0]), request.BucketName);
                }

                else { return (new byte[0]); }
            }

            else if (FileNameParts.Length == 2 || FileNameParts[0] != null)
            {
                return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(request.FileName, request.BucketName);
            }

            else
            {
                return (new byte[0]);
            }
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
            public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

            readonly List<string> ImageSizes = new[] { "small", "medium", "large" }.ToList();

            public string Post(UploadFileMqRequest request)
            {
                try
                {
                    string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(request.FileName, request.ByteArray, request.BucketName, request.MetaData).ToString();
                }
                catch (Exception e)
                {

                }
                return null;
            }

            public string Post(ImageResizeMqRequest request)
            {
                MemoryStream ms = new MemoryStream(request.ImageByte);

                ms.Position = 0;

                UploadFileMqRequest uploadFileRequest = new UploadFileMqRequest();

                uploadFileRequest.BucketName = request.BucketName;

                uploadFileRequest.MetaData = new BsonDocument();

                uploadFileRequest.MetaData = request.MetaData;

                uploadFileRequest.TenantAccountId = request.TenantAccountId;

                uploadFileRequest.UserId = request.UserId;

                try
                {
                    using (Image img = Image.FromStream(ms))
                    {
                        foreach (string size in ImageSizes)
                        {
                            int pixels;

                            if (size == "small")
                            { pixels = 50; }
                            else if (size == "medium")
                            { pixels = 150; }
                            else if (size == "large")
                            { pixels = 640; }
                            else break;

                            var ImgStream = Resize(img, pixels, pixels);

                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadFileRequest.ByteArray = request.ImageByte;
                            uploadFileRequest.FileName = request.ObjectId + "_" + size + ".jpg";

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
