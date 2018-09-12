using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.EmailRelated;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class PdfToEmailService : EbBaseService
    {
        public PdfToEmailService(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_dbf, _mqp, _mqc, _sec) { }

        public void Post(PdfCreateServiceMqRequest request)
        {
            MessageProducer3.Publish(new PdfCreateServiceRequest()
            {
                Refid = request.Refid,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId
            });
        }
    }
    [Restrict(InternalOnly = true)]
    public class PdfToEmailInternalService : EbMqBaseService
    {
        public PdfToEmailInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc): base(_mqp, _mqc) { }

        public void Post(PdfCreateServiceRequest request)
        {
            var objservice = base.ResolveService<EbObjectService>();
            var dataservice = base.ResolveService<DataSourceService>();
            EbObjectParticularVersionResponse res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = request.Refid });
            EbEmailTemplate ebEmailTemplate = new EbEmailTemplate();
            foreach (var element in res.Data)
            {
                ebEmailTemplate = EbSerializers.Json_Deserialize(element.Json);
            }

            List<Param> _param = new List<Param> { new Param { Name = "id", Type = ((int)EbDbTypes.Int32).ToString(), Value = "1" } };

            DataSourceDataResponse dsresp = (DataSourceDataResponse)dataservice.Any(new DataSourceDataRequest { Params = _param, RefId = ebEmailTemplate.DataSourceRefId });
            var ds2 = dsresp.DataSet;
            EbObjectParticularVersionResponse myDsres =(EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = ebEmailTemplate.DataSourceRefId });
            EbDataSource ebDataSource = new EbDataSource();
            foreach (var element in myDsres.Data)
            {
                ebDataSource = EbSerializers.Json_Deserialize(element.Json);
            }
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, 1) }; //change 1 by request.id
            var ds = EbConnectionFactory.ObjectsDB.DoQueries(ebDataSource.Sql, parameters);
            //var pattern = @"\{{(.*?)\}}";
            //var matches = Regex.Matches(ebEmailTemplate.Body, pattern);
            //Dictionary<string, object> dict = new Dictionary<string, object>();
            foreach (var dscol in ebEmailTemplate.DsColumnsCollection)
            {
                string str = dscol.Title.Replace("{{", "").Replace("}}", "");

                foreach (var dt in ds.Tables)
                {
                    string colname = dt.Rows[0][str.Split('.')[1]].ToString();
                    ebEmailTemplate.Body = ebEmailTemplate.Body.Replace(dscol.Title, colname);
                }
            }

            ProtoBufServiceClient pclient = new ProtoBufServiceClient(ServiceStackClient);
            var RepRes = pclient.Get(new ReportRenderRequest { Refid = ebEmailTemplate.AttachmentReportRefID, Fullname = "MQ", Params = null });
            RepRes.StreamWrapper.Memorystream.Position = 0;

            MessageProducer3.Publish(new EmailServicesRequest()
            {
                From = "request.from",
                To = ebEmailTemplate.To,
                Cc = ebEmailTemplate.Cc,
                Bcc = ebEmailTemplate.Bcc,
                Message = ebEmailTemplate.Body,
                Subject = ebEmailTemplate.Subject,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId,
                AttachmentReport = RepRes.StreamWrapper,
                AttachmentName = RepRes.ReportName
            });
        }


    }
}
