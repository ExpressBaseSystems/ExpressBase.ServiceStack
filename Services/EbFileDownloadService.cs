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
            //            string ImageTableQuery_deprecated = @"
            //SELECT 
            //    customervendor.id, customervendor.accountcode, customervendor.imageid, vddicommentry.filename 
            //FROM 
            //    customervendor, vddicommentry
            //WHERE
            //	vddicommentry.patientid = (customervendor.prehead || customervendor.accountcode) 
            //ORDER BY
            //	vddicommentry.filename";
            string ImageTableQuery = @"
SELECT
    vddicommentry.customers_id, vddicommentry.imageid, vddicommentry.filename 
FROM 
    vddicommentry
ORDER BY
	vddicommentry.filename";
            string _imageId = string.Empty, _fileName = string.Empty;

            var table = this.EbConnectionFactory.DataDB.DoQuery(ImageTableQuery);
            foreach (EbDataRow row in table.Rows)
            {
                CustomerId = (int)row[0];
                _imageId = row[1].ToString();
                _fileName = row[2].ToString();
                Files.Add(new KeyValuePair<int, string>(CustomerId, UploadPath + _imageId + "/DICOM/" + _fileName));
            }
        }

        public bool AddEntry(string fname, int CustomerId)
        {
            int res = 0;

            try
            {
                string AddQuery = @"
INSERT INTO 
       eb_image_migration_counter 
      (filename, customer_id)
VALUES
      (@fname, @cid);";
                DbParameter[] MapParams =
                {
                                this.EbConnectionFactory.DataDB.GetNewParameter("cid", EbDbTypes.Int32, CustomerId),
                                this.EbConnectionFactory.DataDB.GetNewParameter("fname", EbDbTypes.String, fname),
                    };
                res = this.EbConnectionFactory.DataDB.DoNonQuery(AddQuery, MapParams);
            }
            catch (Exception e)
            {
                Log.Error("Counter: " + e.Message);
            }
            return res > 0;
        }

        [Authenticate]
        public void Post(FileDownloadRequestObject req)
        {

            string FilerefId = string.Empty;

            Files = new List<KeyValuePair<int, string>>();

            GetFileNamesFromDb();

            Console.WriteLine("Got data from Vddi Comentry");

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

                        AddEntry(fname: file.Value.SplitOnLast('/').Last(), CustomerId: file.Key);
                    }
                }
            }
        }
    }
}
