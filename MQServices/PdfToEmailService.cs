using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    [Authenticate]
    public class EmailTemplateSendService : EbBaseService
    {
        public EmailTemplateSendService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public void Post(EmailTemplateWithAttachmentMqRequest request)
        {
            MessageProducer3.Publish(new EmailAttachmentRequest()
            {
                ObjId = request.ObjId,
                Params = request.Params,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId,
                RefId = request.RefId
            });
        }
    }

    [Authenticate]
    [Restrict(InternalOnly = true)]
    public class EmailTemplateSendInternalService : EbMqBaseService
    {
        public EmailTemplateSendInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public void Post(EmailAttachmentRequest request)
        {
            string mailTo = string.Empty;
            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            EbObjectService objservice = base.ResolveService<EbObjectService>();
            DataSourceService dataservice = base.ResolveService<DataSourceService>();
            ReportService reportservice = base.ResolveService<ReportService>();
            reportservice.EbConnectionFactory = objservice.EbConnectionFactory = dataservice.EbConnectionFactory = ebConnectionFactory;
            ReportRenderResponse RepRes = new ReportRenderResponse();
            EbEmailTemplate EmailTemplate = new EbEmailTemplate();

            if (request.ObjId > 0)
            {
                EbObjectFetchLiveVersionResponse template_res = (EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest() { Id = request.ObjId });
                if (template_res != null && template_res.Data.Count > 0)
                {
                    EmailTemplate = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }
            else if (request.RefId != string.Empty)
            {
                EbObjectParticularVersionResponse template_res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                if (template_res != null && template_res.Data.Count > 0)
                {
                    EmailTemplate = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }

            if (EmailTemplate != null)
            {
                if (EmailTemplate.DataSourceRefId != string.Empty && EmailTemplate.To != string.Empty)
                {
                    EbObjectParticularVersionResponse mailDs = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = EmailTemplate.DataSourceRefId });
                    if (mailDs.Data.Count > 0)
                    {
                        EbDataReader dr = EbSerializers.Json_Deserialize(mailDs.Data[0].Json);
                        IEnumerable<DbParameter> parameters = DataHelper.GetParams(ebConnectionFactory, false, request.Params, 0, 0);
                        EbDataSet ds = ebConnectionFactory.ObjectsDB.DoQueries(dr.Sql, parameters.ToArray());
                        string pattern = @"\{{(.*?)\}}";
                        IEnumerable<string> matches = Regex.Matches(EmailTemplate.Body, pattern).OfType<Match>()
                         .Select(m => m.Groups[0].Value)
                         .Distinct();
                        foreach (string _col in matches)
                        {
                            string str = _col.Replace("{{", "").Replace("}}", "");

                            foreach (EbDataTable dt in ds.Tables)
                            {
                                string colval = dt.Rows[0][str.Split('.')[1]].ToString();
                                EmailTemplate.Body = EmailTemplate.Body.Replace(_col, colval);
                            }
                        }
                        foreach (EbDataTable dt in ds.Tables)
                        {
                            mailTo = dt.Rows[0][EmailTemplate.To.Split('.')[1]].ToString();
                        }
                    }
                }
                if (mailTo != string.Empty)
                {
                    EmailServicesRequest request1 = new EmailServicesRequest()
                    {
                        To = mailTo,
                        Cc = EmailTemplate.Cc.Split(","),
                        Bcc = EmailTemplate.Bcc.Split(","),
                        Message = EmailTemplate.Body,
                        Subject = EmailTemplate.Subject,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        SolnId = request.SolnId,
                    };

                    //adding email attachment. type pdf
                    if (EmailTemplate.AttachmentReportRefID != string.Empty)
                    {
                        RepRes = reportservice.Get(new ReportRenderRequest
                        {
                            Refid = EmailTemplate.AttachmentReportRefID,
                            RenderingUser = new User { FullName = "Machine User" },
                            ReadingUser = new User { Preference = new Preferences { Locale = "en-US", TimeZone = "(UTC) Coordinated Universal Time" } },
                            Params = request.Params
                        });
                        if (RepRes != null && RepRes.StreamWrapper != null && RepRes.StreamWrapper.Memorystream != null)
                        {
                            RepRes.StreamWrapper.Memorystream.Position = 0;
                            request1.AttachmentReport = RepRes.ReportBytea;
                            request1.AttachmentName = RepRes.ReportName + ".pdf";
                        }
                    }

                    MessageProducer3.Publish(request1);
                }
            }
        }
    }

}

