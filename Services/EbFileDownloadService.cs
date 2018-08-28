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

namespace ExpressBase.ServiceStack.Services
{
    public class EbFileDownloadService: EbBaseService
    {
        public List<string> _files = new List<string>();

        public string UserName { get; set; }
        public string Password { private get; set; }
        public string Host { get; set; }
        public List<string> Files
        {
            get { return _files; }
            set { _files = value; }
        }

        private EbMqClient QueueClient { get; set; }

        public EbFileDownloadService() { }

        public EbFileDownloadService(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbMqClient _mq)
        : base(_dbf, _mqp, _mqc, _mq)
        {
        }

        public EbFileDownloadService(string _userName, string _password, string _host)
        {
            UserName = _userName;
            Password = _password;
            Host = _host;

        }

        public void ListDirectory(string DirecStructure)
        {
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
                string[] files = reader.ReadToEnd().Split('\n');
                for (int i = 0; i < files.Length; i++)
                {
                    Console.WriteLine(files[i]);
                    if (files[i][files[i].Length - 1].Equals('\r'))
                        Files.Add(files[i].Substring(0, files[i].Length - 1));
                    else
                        Files.Add(files[i]);
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

        public void DownloadFile(string DirecStructure)//, List<string> fileNames)
        {
            FtpWebRequest request;
            FtpWebResponse response;

            string path = Host + DirecStructure;

            foreach (string name in Files)
            {
                if (!name.Equals(string.Empty))
                {
                    request = (FtpWebRequest)WebRequest.Create(path + name);
                    request.Method = WebRequestMethods.Ftp.DownloadFile;
                    request.Credentials = new NetworkCredential(UserName, Password);
                    response = (FtpWebResponse)request.GetResponse();
                    //WriteFile(response, name);
                    PushToQueue(response, name);
                    response.Close();
                }
            }
        }

        [Authenticate]
        private void Post(FileDownloadRequestObject req)
        {
            string Uname = "ftptest";
            string pwd = "ftppass";
            UserName = Uname;
            Password = pwd;
            DownloadFile("/ftp/directory");
        }

        private void PushToQueue(FtpWebResponse response, string _fileName)
        {
            Stream responseStream = response.GetResponseStream();
            byte[] FileContents = new byte[response.ContentLength];
            responseStream.ReadAsync(FileContents, 0, FileContents.Length);
            base.MessageProducer3.Publish(new UploadFileRequest()
            {
                Byte = FileContents,
                FileDetails = new FileMeta()
                {
                    FileName = _fileName
                }
            });
        }
    }
}
