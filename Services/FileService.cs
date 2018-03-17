//using ExpressBase.Common;
//using ExpressBase.Common.Data;
//using ExpressBase.Common.EbServiceStack;
//using ExpressBase.Common.EbServiceStack.ReqNRes;
//using ExpressBase.Objects.ServiceStack_Artifacts;
//using MongoDB.Bson;
//using ServiceStack;
//using System;
//using System.Collections.Generic;
//using System.Data;

//namespace ExpressBase.ServiceStack.Services
//{
//    public class FileService : EbBaseService, IEbFileService
//    {
//        public FileService(IEbConnectionFactory _tdb) : base(_tdb) { }

//        //[Authenticate]
//        //public byte[] Post(DownloadFileRequest request)
//        //{
//        //    string bucketName = string.Empty;
//        //    ObjectId objectId;
//        //    var FileNameParts = request.FileDetails.FileName.Substring(0, request.FileDetails.FileName.IndexOf('.'))?.Split('_');
//        //    // 3 cases = > 1. ObjectId.(fileextension), 2. ObjectId_(size).(imageextionsion), 3. dp_(userid)_(size).(imageextension)
//        //    if (request.FileDetails.FileName.StartsWith("dp"))
//        //    {
//        //        if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
//        //            bucketName = "dp_images";
//        //    }
//        //    else if (FileNameParts.Length == 1)
//        //    {
//        //        if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
//        //            bucketName = "images_original";

//        //        if (bucketName == string.Empty)
//        //            bucketName = "files";

//        //        objectId = new ObjectId(FileNameParts[0]);

//        //        return (new EbConnectionFactory(request.TenantAccountId, this.Redis)).FilesDB.DownloadFile(new ObjectId(FileNameParts[0]), bucketName);
//        //    }
//        //    else if (FileNameParts.Length == 2)
//        //    {
//        //        if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
//        //        {
//        //            if (FileNameParts[1] == "small")
//        //                bucketName = "images_small";
//        //            else if (FileNameParts[1] == "medium")
//        //                bucketName = "images_medium";
//        //            else if (FileNameParts[1] == "large")
//        //                bucketName = "images_large";
//        //        }
//        //        if (bucketName == string.Empty)
//        //        {
//        //        }
//        //    }
            

//        //    if (bucketName != string.Empty)
//        //    {
//        //        return (this.EbConnectionFactory.FilesDB.DownloadFile(request.FileDetails.FileName, bucketName));
//        //    }
//        //    else { return (new byte[0]); }
//        //}

//        [Authenticate]
//        public List<FileMeta> Post(FindFilesByTagRequest request)
//        {
//            List<FileMeta> FileList = new List<FileMeta>();
//            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
//            {
//                try
//                {
//                    con.Open();
//                    string sql = @"SELECT id, userid, objid, length, tags, bucketname, filetype, uploaddatetime, eb_del FROM public.eb_files WHERE regexp_split_to_array(tags, ',') @> @tags AND COALESCE(eb_del, 'F')='F';";
//                    DataTable dt = new DataTable();
//                    var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
//                      ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("tags", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text) { Value =  request.Tags});
//                    ada.Fill(dt);

//                    foreach (DataRow dr in dt.Rows)
//                    {
//                        FileList.Add(
//                            new FileMeta()
//                            {
//                                ObjectId = dr["objid"].ToString(),
//                                FileType = dr["filetype"].ToString(),
//                                Length = (Int64)dr["length"],
//                                UploadDateTime = (DateTime)dr["uploaddatetime"]
//                            });
//                    }
//                    return FileList;
//                }
//                catch (Exception e)
//                {
//                    Log.Info("Exception:" + e.ToString());
//                    return null;
//                }
//            }
//        }
//    }
//}