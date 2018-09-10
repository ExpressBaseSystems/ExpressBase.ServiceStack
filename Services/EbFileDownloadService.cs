using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using ExpressBase.Common.Constants;
using ServiceStack;
using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Enums;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServiceStack.ReqNRes;
using System.Linq;
using System.Data.Common;
using ExpressBase.Common.Structures;
using ServiceStack.Messaging;

namespace ExpressBase.ServiceStack.Services
{
    public class EbFileDownloadService : EbBaseService
    {
        public string UserName { get; set; }
        public string Password { private get; set; }
        public string Host { get; set; }
        public List<KeyValuePair<int, string>> Files { get; set; }

        public EbFileDownloadService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IMessageProducer _mqp, IMessageQueueClient _mqc)
        : base(_dbf, _sfc, _mqp, _mqc) { }

        private void GetFileNamesFromDb()
        {
            int CustomerId = 0;
            string UploadPath = @"Softfiles_L/";
            string ImageTableQuery = @"
SELECT 
    customervendor.id, customervendor.accountcode, customervendor.imageid, vddicommentry.filename 
FROM 
    customervendor, vddicommentry
WHERE
	vddicommentry.patientid = (customervendor.prehead || customervendor.accountcode) 
ORDER BY
	vddicommentry.filename";
            string _imageId = string.Empty, _fileName = string.Empty, _accountCode = string.Empty;

            var table = this.EbConnectionFactory.ObjectsDB.DoQuery(ImageTableQuery);
            foreach (EbDataRow row in table.Rows)
            {
                CustomerId = (int)row[0];
                _accountCode = row[1].ToString();
                _imageId = row[2].ToString();
                _fileName = row[3].ToString();
                Files.Add(new KeyValuePair<int, string>(CustomerId, System.Web.HttpUtility.UrlPathEncode(UploadPath + _imageId + "/DICOM/" + _fileName)));
            }
        }
        private int MapFilesWithUser(int CustomerId, int FileRefId)
        {
            int res = 0;
            string MapQuery = @"INSERT into customer_files(customer_id, eb_files_ref_id) values(customer_id=@cust_id, eb_files_ref_id=@ref_id) returning id";
            DbParameter[] MapParams =
            {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("cust_id", EbDbTypes.Int32, CustomerId),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("ref_id", EbDbTypes.Int32, FileRefId)
            };
            var table = this.EbConnectionFactory.ObjectsDB.DoQuery(MapQuery);
            res = (int)table.Rows[0][0];
            return res;
        }

        [Authenticate]
        public void Post(FileDownloadRequestObject req)
        {

            string FilerefId = string.Empty;

            Files = new List<KeyValuePair<int, string>>();

            GetFileNamesFromDb();
            GetImageFtpRequest getImageFtp = new GetImageFtpRequest();

            getImageFtp.AddAuth(req.UserId, req.SolnId, this.FileClient.BearerToken, this.FileClient.RefreshToken);

            if (Files.Count > 0)
            {

                foreach (KeyValuePair<int, string> file in Files)
                {
                    if (!file.Value.Equals(string.Empty))
                    {
                        getImageFtp.FileUrl = file;
                        this.MessageProducer3.Publish(getImageFtp);
                    }
                }
            }
        }
    }
}
