using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Structures;
using System.Data.Common;
using ServiceStack;
using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Enums;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Messaging;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServiceStack.ReqNRes;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    public class EbFileDownloadService: EbBaseService
    {
        public string UserName { get; set; }
        public string Password { private get; set; }
        public string Host { get; set; }
        public List<string> Files { get; set; }

        public EbFileDownloadService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc)
        : base(_dbf, _sfc)
        {
        }

        public void ListFilesDirectory(string DirecStructure)
        {
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(FileDownloadConstants.HostName + DirecStructure + "DICOM/");
            request.Timeout = -1;
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            System.Net.ServicePointManager.Expect100Continue = false;
            request.Credentials = new NetworkCredential(UserName, Password);

            try
            {
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(responseStream);
                string[] files = reader.ReadToEnd().Split('\n');
                if (files.Length > 0)
                {
                    for (int i = 0; i < files.Length; i++)
                    {
                        if (!files[i].Trim().IsNullOrEmpty())
                        {
                            Console.WriteLine("File Name : " + files[i]);
                            Files.Add(files[i]);
                        }

                    }
                }
                Console.WriteLine($"Directory List Complete, status {response.StatusDescription}");
                reader.Close();
                response.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private List<string> ListDirectory(string DirecStructure)
        {
            List<string> DirectoryListing = new List<string>();
            FtpWebRequest request = (FtpWebRequest)WebRequest.Create(FileDownloadConstants.HostName + DirecStructure);
            request.Timeout = -1;
            request.Method = WebRequestMethods.Ftp.ListDirectory;
            System.Net.ServicePointManager.Expect100Continue = false;
            request.Credentials = new NetworkCredential(UserName, Password);
            try
            {
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                StreamReader reader = new StreamReader(responseStream);
                string[] Directories = reader.ReadToEnd().Split('\n');
                for (int i = 0; i < Directories.Length; i++)
                {
                    //if (!Directories[i].Equals(string.Empty) || !Directories[i].Equals('\r') || !Directories[i].Equals('\n'))
                    //{
                    //    DirectoryListing.Add(Directories[i]);
                    //    Console.WriteLine("Directory Listing: " + Directories[i]);
                    //}
                    //if (temp.Equals('\r'))
                    //    DirectoryListing.Add(temp.Substring(0, temp.Length - 1));
                    //else
                    //DirectoryListing.Add(temp);
                    if (!Directories[i].Equals(string.Empty))
                    {
                        if (Directories[i][Directories[i].Length - 1].Equals('\r'))
                            DirectoryListing.Add(Directories[i].Substring(0, Directories[i].Length - 1));
                        else
                            DirectoryListing.Add(Directories[i]);
                    }
                }
                Console.WriteLine($"Directory List Complete, status {response.StatusDescription}");
                reader.Close();
                response.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            return DirectoryListing;
        }

        //public void DownloadFile(string DirecStructure)//, List<string> fileNames)
        //{
        //    FtpWebRequest request;
        //    FtpWebResponse response;

        //    string path = Host + DirecStructure + "DICOM/";

        //    foreach (string name in Files)
        //    {
        //        if (!name.Equals(string.Empty))
        //        {
        //            request = (FtpWebRequest)WebRequest.Create(path + name);
        //            request.Method = WebRequestMethods.Ftp.DownloadFile;
        //            request.Credentials = new NetworkCredential(UserName, Password);
        //            response = (FtpWebResponse)request.GetResponse();
        //            //WriteFile(response, name);
        //            PushToQueue(response, name);
        //            response.Close();
        //        }
        //    }
        //}

        //[Authenticate]
        //private void Post(FileDownloadRequestObject req)
        //{
        //    string Uname = "ftpUser1";
        //    string pwd = "ftpPassword1";
        //    UserName = Uname;
        //    Password = pwd;
        //    string BasePath = "/files/Softfiles_L/";
        //    List<string> DirStructure = ListDirectory(BasePath);
        //    for (int i = 0; i < 10; i++)
        //    {
        //        string Path = BasePath + DirStructure[i] + "/";
        //        ListFilesDirectory(Path);
        //        DownloadFile(Path);
        //        Files.Clear();
        //    }
        //}

        //private void PushToQueue(FtpWebResponse response, string _fileName)
        //{
        //    Stream responseStream = response.GetResponseStream();
        //    byte[] FileContents = new byte[response.ContentLength];
        //    responseStream.ReadAsync(FileContents, 0, FileContents.Length);
        //    base.MessageProducer3.Publish(new UploadImageRequest()
        //    {
        //        Byte = FileContents,
        //        ImageInfo = new ImageMeta()
        //        {
        //            FileName = _fileName,

        //        }
        //    });
        //}

        [Authenticate]
        public void Post(FileDownloadRequestObject req)
        {
            string Uname = "";
            string pwd = "";
            UserName = Uname;
            Password = pwd;
            string BasePath = "/files/Softfiles_L/";
            Host = FileDownloadConstants.HostName;
            Files = new List<string>();
            List<string> DirStructure = ListDirectory(BasePath);
            for (int i = 0; i < 10; i++)
            {
                string Path = BasePath + DirStructure[i] + "/";
                ListFilesDirectory(Path);
                FtpWebRequest request;
                FtpWebResponse response;

                string fullpath = Host + Path + "DICOM/";
                if (Files.Count > 0)
                {
                    foreach (string name in Files)
                    {
                        if (!name.Equals(string.Empty))
                        {
                            request = (FtpWebRequest)WebRequest.Create(fullpath + name);
                            request.Method = WebRequestMethods.Ftp.DownloadFile;
                            request.Credentials = new NetworkCredential(UserName, Password);
                            response = (FtpWebResponse)request.GetResponse();
                            //WriteFile(response, name);
                            Stream responseStream = response.GetResponseStream();
                            byte[] FileContents = new byte[response.ContentLength];
                            responseStream.ReadAsync(FileContents, 0, FileContents.Length);
                            UploadImageAsyncRequest imgupreq = new UploadImageAsyncRequest();
                            imgupreq.ImageByte = FileContents;
                            imgupreq.ImageInfo = new ImageMeta();
                            imgupreq.ImageInfo.FileCategory = EbFileCategory.Images;
                            imgupreq.ImageInfo.FileName = name;
                            imgupreq.ImageInfo.FileType = name.Split('.').Last();
                            imgupreq.ImageInfo.ImageQuality = ImageQuality.original;
                            imgupreq.ImageInfo.Length = FileContents.Length;
                            imgupreq.ImageInfo.MetaDataDictionary = new Dictionary<string, List<string>>();
                            imgupreq.ImageInfo.MetaDataDictionary.Add("Customer",new List<string>() { name});

                            var x = FileClient.Post<UploadAsyncResponse>(imgupreq);
                            response.Close();
                        }
                    }
                    Files.Clear();
                }
                else
                {
                    Console.WriteLine("There were no files in that directory!\n\n");
                }
            }
        }
    }
}
