using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using MongoDB.Driver.GridFS;
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

            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest
                    {
                        FileDetails = new FileMeta
                        {
                            FileName = request.FileDetails.FileName,
                            MetaDataDictionary = request.FileDetails.MetaDataDictionary
                        },
                        ByteArray = request.ByteArray,
                        BucketName = request.BucketName,
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId
                    });
                    return "Successfully Uploaded to MQ";
                }
                catch (Exception e)
                {
                    return "Failed to Uplaod to MQ";
                }
            }

            else if (!request.IsAsync)
            {

                string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
                    request.FileDetails.FileName,
                    request.FileDetails.MetaDataDictionary,
                    request.ByteArray,
                    request.BucketName
                    ).ToString();

                if (request.FileDetails.ContentType < 100)
                    this.MessageProducer3.Publish(new ImageResizeMqRequest
                    {
                        FileDetails = new FileMeta
                        {
                            ObjectId = Id,
                            FileName = request.FileDetails.FileName,
                            MetaDataDictionary = request.FileDetails.MetaDataDictionary
                        },
                        ImageByte = request.ByteArray,
                        BucketName = request.BucketName,
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId
                    });

                return Id;
            }

            return "Uploading Failed check the data";
        }

        [Authenticate]
        public byte[] Post(DownloadFileRequest request)
        {
            var FileNameParts = request.FileDetails.FileName.Substring(0, request.FileDetails.FileName.IndexOf('.'))?.Split('_');

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
                return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(request.FileDetails.FileName, request.BucketName);
            }

            else
            {
                return (new byte[0]);
            }
        }

        [Authenticate]
        public List<FileMeta> Post(FindFilesByTagRequest request)
        {
            List<FileMeta> FileList = new List<FileMeta>();

            List<GridFSFileInfo> filesList = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.FindFilesByTags(request.Filter, request.BucketName);

            foreach (GridFSFileInfo file in filesList)
            {
                string stringType = file.Filename.Split('.')[1];
                int intType =0;

                foreach (FileTypes type in Enum.GetValues(typeof(ImageTypes)))
                {
                    if (type.ToString() == stringType)
                    {
                        intType = (int)type;
                        break;
                    }
                }

                FileList.Add(
                    new FileMeta
                    {
                        ObjectId = file.Id.ToString(),
                        FileName = file.Filename,
                        ContentType = intType,
                        Length = file.Length,
                        UploadDateTime = file.UploadDateTime
                    });
            }
            return FileList;
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
                    string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).
                        FilesDB.UploadFile(
                        request.FileDetails.FileName,
                        request.FileDetails.MetaDataDictionary,
                        request.ByteArray,
                        request.BucketName
                        ).
                        ToString();
                }
                catch (Exception e)
                {

                }
                return null;
            }

            public string Post(ImageResizeMqRequest request)
            {
                UploadFileMqRequest uploadFileRequest = new UploadFileMqRequest();
                uploadFileRequest.BucketName = "thumbs";
                uploadFileRequest.FileDetails = new FileMeta();
                uploadFileRequest.FileDetails.MetaDataDictionary = request.FileDetails.MetaDataDictionary;
                uploadFileRequest.TenantAccountId = request.TenantAccountId;
                uploadFileRequest.UserId = request.UserId;

                MemoryStream ms = new MemoryStream(request.ImageByte);
                ms.Position = 0;

                try
                {
                    using (Image img = Image.FromStream(ms))
                    {
                        foreach (string size in ImageSizes)
                        {
                            int pixels;

                            if (size == "small"){pixels = 50;}
                            else if (size == "medium"){pixels = 150;}
                            else if (size == "large"){pixels = 640;}
                            else break;

                            var ImgStream = Resize(img, pixels, pixels);

                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);
                            
                            uploadFileRequest.ByteArray = request.ImageByte;
                            uploadFileRequest.FileDetails.FileName = request.FileDetails.ObjectId + "_" + size + ".png";

                            this.MessageProducer3.Publish(uploadFileRequest);
                        }
                    }
                }
                catch (Exception e)
                {}
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
