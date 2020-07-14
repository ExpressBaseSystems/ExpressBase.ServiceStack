using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class ExportToExcelService : EbBaseService
    {
        public ExportToExcelService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        [Authenticate]
        public void Post(ExportToExcelMqRequest request)
        {
            MessageProducer3.Publish(new ExportToExcelServiceRequest()
            {
                EbDataVisualization = request.EbDataVisualization,
                Ispaging = request.Ispaging,
                UserInfo = request.UserInfo,
                RefId = request.RefId,
                IsExcel = request.IsExcel,
                Params = request.Params,
                TFilters = request.TFilters,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId,
                eb_solution = request.eb_Solution,
                BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
                RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty,
                SubscriptionId = request.SubscriptionId
            });
        }
    }

    [Restrict(InternalOnly = true)]
    public class ExportToExcelInternalService : EbMqBaseService
    {
        public ExportToExcelInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec) { }

        public void Post(ExportToExcelServiceRequest request)
        {
            try
            {
                EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                var dataservice = base.ResolveService<DataVisService>();
                dataservice.EbConnectionFactory = ebConnectionFactory;
                TableDataRequest _req = new TableDataRequest();
                DataSourceDataResponse res = new DataSourceDataResponse();
                _req.EbDataVisualization = request.EbDataVisualization;
                _req.Ispaging = false;
                _req.UserInfo = request.UserInfo;
                _req.RefId = request.RefId;
                _req.IsExcel = true;
                _req.Params = request.Params;
                _req.TFilters = request.TFilters;
                _req.Token = request.BToken;
                _req.rToken = request.RToken;
                _req.eb_Solution = request.eb_solution;
                res = (DataSourceDataResponse)dataservice.Any(_req);
                byte[] compressedData = Compress(res.excel_file);
                this.Redis.Set("excel" + (request.EbDataVisualization.RefId + request.UserInfo.UserId), compressedData, DateTime.Now.AddHours(5));
                this.ServerEventClient.BearerToken = request.BToken;
                this.ServerEventClient.RefreshToken = request.RToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                this.ServerEventClient.Post<NotifyResponse>(new NotifySubscriptionRequest
                {
                    Msg = "../DV/GetExcel?refid=" + (request.EbDataVisualization.RefId + request.UserInfo.UserId) + "&filename=" + request.EbDataVisualization.DisplayName + ".xlsx",
                    Selector = StaticFileConstants.EXPORTTOEXCELSUCCESS,
                    ToSubscriptionId = request.SubscriptionId
                });
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception: " + e.ToString());
            }

        }

        public static byte[] Compress(byte[] data)
        {
            using (MemoryStream memory = new MemoryStream())
            {
                using (GZipStream gzip = new GZipStream(memory,
                    CompressionMode.Compress, true))
                {
                    gzip.Write(data, 0, data.Length);
                }
                return memory.ToArray();
            }
        }

    }
}