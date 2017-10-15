using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using MongoDB.Bson;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;

namespace ExpressBase.ServiceStack.MQServices
{
    public class FileService : EbBaseService
    {
        public FileService(ITenantDbFactory _tdb, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_tdb, _mqp, _mqc) { }

        [Authenticate]
        public string Post(UploadFileRequest request)
        {
            EbSolutionConnections SolutionConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

            string bucketName = "files";

            if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
            {
                bucketName = "images_original";
                if (request.FileDetails.FileName.StartsWith("dp"))
                {
                    bucketName = "dp_images";
                }
            }

            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest
                    {
                        FileDetails = new FileMeta
                        {
                            FileName = request.FileDetails.FileName,
                            MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                            Length = request.FileDetails.Length
                        },
                        FileByte = request.FileByte,
                        BucketName = bucketName,
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
                string Id = this.TenantDbFactory.FilesDB.UploadFile(
                    request.FileDetails.FileName,
                    (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                    request.FileByte,
                    bucketName)
                    .ToString();

                this.MessageProducer3.Publish(new FileMetaPersistMqRequest
                {
                    FileDetails = new FileMeta
                    {
                        ObjectId = Id,
                        FileName = request.FileDetails.FileName,
                        MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                        Length = request.FileByte.Length,
                        FileType = request.FileDetails.FileType
                    },
                    BucketName = bucketName,
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId
                });

                if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
                    this.MessageProducer3.Publish(new ImageResizeMqRequest
                    {
                        ImageInfo = new FileMeta
                        {
                            ObjectId = Id,
                            FileName = request.FileDetails.FileName,
                            MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { }
                        },
                        ImageByte = request.FileByte,
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
            string bucketName = string.Empty;
            ObjectId objectId;
            var FileNameParts = request.FileDetails.FileName.Substring(0, request.FileDetails.FileName.IndexOf('.'))?.Split('_');
            // 3 cases = > 1. ObjectId.(fileextension), 2. ObjectId_(size).(imageextionsion), 3. dp_(userid)_(size).(imageextension)

            if (FileNameParts.Length == 1)
            {
                if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
                    bucketName = "images_original";

                if (bucketName == string.Empty)
                    bucketName = "files";

                objectId = new ObjectId(FileNameParts[0]);

                return (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(new ObjectId(FileNameParts[0]), bucketName);
            }
            else if (FileNameParts.Length == 2)
            {
                if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
                {
                    if (FileNameParts[1] == "small")
                        bucketName = "images_small";
                    else if (FileNameParts[1] == "medium")
                        bucketName = "images_medium";
                    else if (FileNameParts[1] == "large")
                        bucketName = "images_large";
                }
                if (bucketName == string.Empty)
                {
                }
            }
            else if (request.FileDetails.FileName.StartsWith("dp"))
            {
                if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
                    bucketName = "dp_images";
            }

            if (bucketName != string.Empty)
            {
                return (this.TenantDbFactory.FilesDB.DownloadFile(request.FileDetails.FileName, bucketName));
            }
            else { return (new byte[0]); }
        }

        [Authenticate]
        public List<FileMeta> Post(FindFilesByTagRequest request)
        {
            List<FileMeta> FileList = new List<FileMeta>();
            using (var con = this.TenantDbFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
            {
                try
                {
                    con.Open();

                    string sql = @"SELECT 	id, userid, objid, length, tags, bucketname, filetype, uploaddatetime, eb_del FROM public.eb_files WHERE regexp_split_to_array(tags, ',') @> @tags AND eb_del = false;";
                    //string sql = @"SELECT objid, filetype, length, tags, uploaddatetime  FROM eb_files WHERE tags ~  @tags AND eb_del = false";
                    DataTable dt = new DataTable();
                    

                    var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
                    ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("tags", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value =  request.Tags});
                    ada.Fill(dt);

                    foreach (DataRow dr in dt.Rows)
                    {
                        FileList.Add(
                            new FileMeta()
                            {
                                ObjectId = dr["objid"].ToString(),
                                FileType = dr["filetype"].ToString(),
                                Length = (Int64)dr["length"],
                                UploadDateTime = (DateTime)dr["uploaddatetime"]
                            });
                    }
                    return FileList;
                }
                catch (Exception e)
                {
                    return null;
                }
            }
        }

        [Authenticate]
        public string Post(UploadImageRequest request)
        {
            string bucketName = "images_original";
            if (request.ImageInfo.FileName.StartsWith("dp"))
                bucketName = "dp_images";

            EbSolutionConnections SolutionConnections = this.Redis.Get<EbSolutionConnections>(string.Format("EbSolutionConnections_{0}", request.TenantAccountId));

            if (request.IsAsync)
            {
                try
                {
                    this.MessageProducer3.Publish(new UploadFileMqRequest
                    {
                        FileDetails = new FileMeta
                        {
                            FileName = request.ImageInfo.FileName,
                            MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                            request.ImageInfo.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                        },
                        FileByte = request.ImageByte,
                        BucketName = bucketName,
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
                    request.ImageInfo.FileName,
                    request.ImageInfo.MetaDataDictionary,
                    request.ImageByte,
                    bucketName
                    ).ToString();

                this.MessageProducer3.Publish(new FileMetaPersistMqRequest
                {
                    FileDetails = new FileMeta
                    {
                        ObjectId = Id,
                        FileName = request.ImageInfo.FileName,
                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                            request.ImageInfo.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                        Length = request.ImageByte.Length,
                        FileType = request.ImageInfo.FileType
                    },
                    BucketName = bucketName,
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId
                });
                this.MessageProducer3.Publish(new ImageResizeMqRequest
                {
                    ImageInfo = new FileMeta
                    {
                        ObjectId = Id,
                        FileName = request.ImageInfo.FileName,
                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                            request.ImageInfo.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { }
                    },
                    ImageByte = request.ImageByte,
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId
                });
                return Id;
            }

            return "Uploading Failed check the data";
        }

        [Restrict(InternalOnly = true)]
        public class FileServiceInternal : EbBaseService
        {
            public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

            public string Post(UploadFileMqRequest request)
            {
                try
                {
                    string Id = (new TenantDbFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
                        request.FileDetails.FileName,
                        (request.FileDetails.MetaDataDictionary.Count != 0) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                        request.FileByte,
                        request.BucketName
                        ).
                        ToString();

                    this.MessageProducer3.Publish(new FileMetaPersistMqRequest
                    {
                        FileDetails = new FileMeta
                        {
                            ObjectId = Id,
                            FileName = request.FileDetails.FileName,
                            MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { },
                            Length = request.FileByte.Length,
                            FileType = request.FileDetails.FileType
                        },
                        BucketName = request.BucketName,
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId
                    });
                    if (request.BucketName == "images_original" || (request.BucketName == "dp_images" && request.BucketName.Contains("actual")))
                        this.MessageProducer3.Publish(new ImageResizeMqRequest
                        {
                            ImageInfo = new FileMeta
                            {
                                ObjectId = Id,
                                FileName = request.FileDetails.FileName,
                                MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                                request.FileDetails.MetaDataDictionary :
                                new Dictionary<String, List<string>>() { }
                            },
                            ImageByte = request.FileByte,
                            TenantAccountId = request.TenantAccountId,
                            UserId = request.UserId
                        });
                    else return null;
                }
                catch (Exception e)
                {
                }
                return null;
            }

            public string Post(ImageResizeMqRequest request)
            {
                UploadFileMqRequest uploadFileRequest = new UploadFileMqRequest();
                uploadFileRequest.TenantAccountId = request.TenantAccountId;
                uploadFileRequest.UserId = request.UserId;

                MemoryStream ms = new MemoryStream(request.ImageByte);
                ms.Position = 0;

                try
                {
                    using (Image img = Image.FromStream(ms))
                    {
                        if (request.ImageInfo.FileName.StartsWith("dp"))
                        {
                            foreach (string size in Enum.GetNames(typeof(DPSizes)))
                            {
                                Stream ImgStream = Resize(img, (int)((DPSizes)Enum.Parse(typeof(DPSizes), size)), (int)((DPSizes)Enum.Parse(typeof(DPSizes), size)));
                                request.ImageByte = new byte[ImgStream.Length];
                                ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                                uploadFileRequest.FileByte = request.ImageByte;
                                uploadFileRequest.BucketName = "dp_images";
                                uploadFileRequest.FileDetails = new FileMeta()
                                {
                                    FileName = String.Format("dp_{0}_{1}.{2}", request.UserId, size, "jpg"),
                                    MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                                        request.ImageInfo.MetaDataDictionary :
                                        new Dictionary<String, List<string>>() { },
                                    FileType = "jpg"
                                };
                                this.MessageProducer3.Publish(uploadFileRequest);
                            }
                        }
                        else
                        {
                            foreach (string size in Enum.GetNames(typeof(ImageSizes)))
                            {
                                Stream ImgStream = Resize(img, (int)((ImageSizes)Enum.Parse(typeof(ImageSizes), size)), (int)((ImageSizes)Enum.Parse(typeof(ImageSizes), size)));

                                request.ImageByte = new byte[ImgStream.Length];
                                ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                                uploadFileRequest.FileDetails = new FileMeta()
                                {
                                    FileName = request.ImageInfo.ObjectId + "_" + size + ".png",
                                    MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                                        request.ImageInfo.MetaDataDictionary :
                                        new Dictionary<String, List<string>>() { },
                                    FileType = "png"

                                };
                                uploadFileRequest.FileByte = request.ImageByte;
                                uploadFileRequest.BucketName = string.Format("images_{0}", size);

                                this.MessageProducer3.Publish(uploadFileRequest);
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                }
                return null;
            }

            public string Post(FileMetaPersistMqRequest request)
            {
                string tag = string.Empty;
                if (request.FileDetails.MetaDataDictionary.Count != 0)
                    foreach (var items in request.FileDetails.MetaDataDictionary)
                    {
                        tag = string.Join(",", items.Value);
                    }
                TenantDbFactory tenantDbFactory = new TenantDbFactory(request.TenantAccountId, this.Redis);

                string sql = "INSERT INTO eb_files(userid, objid, length, filetype, tags, bucketname, uploaddatetime) VALUES(@userid, @objid, @length, @filetype,@tags,@bucketname, CURRENT_TIMESTAMP) RETURNING id";
                DbParameter[] parameters =
                    {
                        tenantDbFactory.ObjectsDB.GetNewParameter("userid", System.Data.DbType.Int32, request.UserId),
                        tenantDbFactory.ObjectsDB.GetNewParameter("objid",System.Data.DbType.String, request.FileDetails.ObjectId),
                        tenantDbFactory.ObjectsDB.GetNewParameter("length",System.Data.DbType.Int64, request.FileDetails.Length),
                        tenantDbFactory.ObjectsDB.GetNewParameter("filetype",System.Data.DbType.String, request.FileDetails.FileType),
                        tenantDbFactory.ObjectsDB.GetNewParameter("tags",System.Data.DbType.String, tag),
                        tenantDbFactory.ObjectsDB.GetNewParameter("bucketname",System.Data.DbType.String, request.BucketName)
                    };
                var iCount = tenantDbFactory.ObjectsDB.DoQuery(sql, parameters);

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