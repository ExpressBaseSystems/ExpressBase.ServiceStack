using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using ExpressBase.ServiceStack.Services;
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
                RefId = request.RefId,
                BToken = request.BToken,
                RToken = request.RToken
            });
            Console.WriteLine("EmailTemplateWithAttachment publish complete");
        }
    }

    [Authenticate]
    [Restrict(InternalOnly = true)]
    public class EmailTemplateSendInternalService : EbMqBaseService
    {
        public EmailTemplateSendInternalService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        EbEmailTemplate Template = new EbEmailTemplate();

        EbDataSet DataSet;

        public void Post(EmailAttachmentRequest request)
        {
            Console.WriteLine("EmailTemplateWithAttachment mq internal started");

            EbConnectionFactory ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            EbObjectService objservice = base.ResolveService<EbObjectService>();
            ReportService reportservice = base.ResolveService<ReportService>();
            reportservice.EbConnectionFactory = objservice.EbConnectionFactory = ebConnectionFactory;

            if (request.ObjId > 0)
            {
                EbObjectFetchLiveVersionResponse template_res = (EbObjectFetchLiveVersionResponse)objservice.Get(new EbObjectFetchLiveVersionRequest() { Id = request.ObjId });
                if (template_res?.Data.Count > 0)
                {
                    this.Template = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }
            else if (request.RefId != string.Empty)
            {
                EbObjectParticularVersionResponse template_res = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
                if (template_res?.Data.Count > 0)
                {
                    this.Template = EbSerializers.Json_Deserialize(template_res.Data[0].Json);
                }
            }

            if (this.Template != null)
            {
                if (this.Template.DataSourceRefId != string.Empty && this.Template.To != string.Empty)
                {
                    EbObjectParticularVersionResponse mailDs = (EbObjectParticularVersionResponse)objservice.Get(new EbObjectParticularVersionRequest() { RefId = this.Template.DataSourceRefId });
                    if (mailDs.Data.Count > 0)
                    {
                        EbDataReader dr = EbSerializers.Json_Deserialize(mailDs.Data[0].Json);
                        IEnumerable<DbParameter> parameters = DataHelper.GetParams(ebConnectionFactory.ObjectsDB, false, request.Params, 0, 0);
                        DataSet = ebConnectionFactory.ObjectsDB.DoQueries(dr?.Sql, parameters.ToArray());
                        Fill();
                    }
                }
                if (this.Template.To != string.Empty)
                {
                    EmailServicesRequest request1 = new EmailServicesRequest()
                    {
                        To = this.Template.To,
                        Cc = this.Template.Cc.Split(","),
                        Bcc = this.Template.Bcc.Split(","),
                        Message = this.Template.Body,
                        Subject = this.Template.Subject,
                        UserId = request.UserId,
                        UserAuthId = request.UserAuthId,
                        SolnId = request.SolnId,
                        ReplyTo = this.Template.ReplyTo,
                        Params = request.Params,
                        RefId = this.Template.RefId                        
                    };

                    //adding email attachment. type pdf
                    if (this.Template.AttachmentReportRefID != string.Empty)
                    {
                        ReportRenderResponse RepRes = reportservice.Get(new ReportRenderRequest
                        {
                            Refid = this.Template.AttachmentReportRefID,
                            Params = request.Params,
                            SolnId = request.SolnId,
                            ReadingUserAuthId = request.UserAuthId,
                            RenderingUserAuthId = request.UserAuthId,
                            BToken = request.BToken,
                            RToken = request.RToken
                        });
                        if (RepRes?.StreamWrapper?.Memorystream != null)
                        {
                            RepRes.StreamWrapper.Memorystream.Position = 0;
                            request1.AttachmentReport = RepRes.ReportBytea;
                            request1.AttachmentName = RepRes.ReportName + ".pdf";
                            Console.WriteLine("EmailTemplateWithAttachment.Attachment Added");
                        }
                    }
                    MessageProducer3.Publish(request1);
                    Console.WriteLine("Published to Email send");
                }
                else
                {
                    throw new Exception("Email.To is empty " + this.Template.AttachmentReportRefID);
                }
            }
            else
            {
                throw new Exception("Template is empty :" + Template.RefId);
            }
        }

        public void Fill()
        {
            this.Template.Body = ReplacePlaceholders(this.Template.Body);
            this.Template.Subject = ReplacePlaceholders(this.Template.Subject);
            this.Template.To = ReplacePlaceholders(this.Template.To);
            this.Template.Cc = ReplacePlaceholders(this.Template.Cc);
            this.Template.Bcc = ReplacePlaceholders(this.Template.Bcc);
            this.Template.ReplyTo = ReplacePlaceholders(this.Template.ReplyTo);
        }

        public string ReplacePlaceholders(string text)
        {
            string pattern = @"\{{(.*?)\}}";
            IEnumerable<string> matches = Regex.Matches(text, pattern).OfType<Match>().Select(m => m.Groups[0].Value).Distinct();
            foreach (string _col in matches)
            {
                try
                {
                    string str = _col.Replace("{{", "").Replace("}}", "");
                    int tbl = Convert.ToInt32(str.Split('.')[0].Replace("Table", ""));
                    string colval = string.Empty;
                    if (DataSet.Tables[tbl - 1].Rows.Count > 0)
                        colval = DataSet.Tables[tbl - 1].Rows[0][str.Split('.')[1]].ToString();
                    text = text.Replace(_col, colval);
                }
                catch (Exception e)
                {
                    throw new Exception("EmailTemplateWithAttachment.matches fill Exception, col:" + _col);
                }
            }
            return text;
        }
    }
}

