using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Threading.Tasks;
using ServiceStack.Messaging;
using ExpressBase.Objects.EmailRelated;
using ExpressBase.Common;
using ExpressBase.Objects;
using ExpressBase.Common.Data;
using System.Text;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Data.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Structures;
using ExpressBase.Common.ServiceClients;
using System.Net.Mail;
using System.Net;
using ExpressBase.Objects.Services;
using System.IO;

namespace ExpressBase.ServiceStack
{
    [Authenticate]
    public class EmailService : EbBaseService
    {
        public EmailService(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_dbf, _mqp, _mqc, _sec) { }

        public EmailServicesResponse Post(EmailServicesMqRequest request)
        {
            EmailServicesResponse resp = new EmailServicesResponse();

            //EbObjectService myService = base.ResolveService<EbObjectService>();
            //var res = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.Refid });
            //EbEmailTemplate ebEmailTemplate = new EbEmailTemplate();
            //foreach (var element in res.Data)
            //{
            //    ebEmailTemplate = EbSerializers.Json_Deserialize(element.Json);
            //}

            //var myDs = base.ResolveService<EbObjectService>();
            //var myDsres = (EbObjectParticularVersionResponse)myDs.Get(new EbObjectParticularVersionRequest() { RefId = ebEmailTemplate.DataSourceRefId });
            //EbDataSource ebDataSource = new EbDataSource();
            //foreach (var element in myDsres.Data)
            //{
            //    ebDataSource = EbSerializers.Json_Deserialize(element.Json);
            //}
            //DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, 1) }; //change 1 by request.id
            //var ds = EbConnectionFactory.ObjectsDB.DoQueries(ebDataSource.Sql, parameters);
            ////var pattern = @"\{{(.*?)\}}";
            ////var matches = Regex.Matches(ebEmailTemplate.Body, pattern);
            ////Dictionary<string, object> dict = new Dictionary<string, object>();
            //foreach (var dscol in ebEmailTemplate.DsColumnsCollection)
            //{
            //    string str = dscol.Title.Replace("{{", "").Replace("}}", "");

            //    foreach (var dt in ds.Tables)
            //    {
            //        string colname = dt.Rows[0][str.Split('.')[1]].ToString();
            //        ebEmailTemplate.Body = ebEmailTemplate.Body.Replace(dscol.Title, colname);
            //    }
            //}

            //this.MessageProducer3.Publish(new EmailServicesRequest()
            //{
            //    From = request.From,
            //    To = ebEmailTemplate.To,
            //    Cc = ebEmailTemplate.Cc,
            //    Bcc = ebEmailTemplate.Bcc,
            //    Message = ebEmailTemplate.Body,
            //    Subject = ebEmailTemplate.Subject,
            //    UserId = request.UserId,
            //    UserAuthId = request.UserAuthId,
            //    TenantAccountId = request.TenantAccountId,
            //    AttachmentReport = ebEmailTemplate.AttachmentReport
            //});

            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    public class EmailInternalService : EbMqBaseService
    {
        public EmailInternalService() : base() { }

        public string Post(EmailServicesRequest request)
        {
            try
            {
                MailMessage mm = new MailMessage("expressbasesystems@gmail.com", request.To)
                {
                    Subject = request.Subject,
                    IsBodyHtml = true,
                    Body = request.Message,

                };
                mm.Attachments.Add(new Attachment(new MemoryStream(request.AttachmentReport)/*Memorystream*/, request.AttachmentName + ".pdf"));
                mm.CC.Add(request.Cc);
                mm.Bcc.Add(request.Bcc);
                System.Net.Mail.SmtpClient smtp = new System.Net.Mail.SmtpClient
                {
                    Host = "smtp.gmail.com",
                    Port = 587,
                    EnableSsl = true,
                    Credentials = new NetworkCredential { UserName = "expressbasesystems@gmail.com", Password = "ebsystems" }

                };
                smtp.Send(mm);
            }
            catch (Exception e)
            {
                return e.Message;
            }
            return null;
        }
    }
}
