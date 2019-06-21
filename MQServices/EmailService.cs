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

    public class EmailService : EbBaseService
    {
        public EmailService(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_dbf, _mqp, _mqc, _sec) { }

        //public EmailServicesResponse Post(EmailServicesMqRequest request)
        //{
        //    EmailServicesResponse resp = new EmailServicesResponse();
        //    return resp;
        //}

        //public ResetPasswordMqResponse Post(ResetPasswordMqRequest request)
        //{
        //    string q = "SELECT * FROM eb_reset_pw(:email)";
        //    DbParameter[] parameters = {
        //        this.EbConnectionFactory.DataDB.GetNewParameter("email",EbDbTypes.String,request.Email)
        //    };
        //    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(q, parameters);

        //    MessageProducer3.Publish(new EmailServicesRequest()
        //    {
        //        To = request.Email,
        //        Cc = null,
        //        Bcc = null,
        //        Message = "Your new password is" + dt.Rows[0][0],
        //        Subject = "Reset Password",
        //        //UserId = request.UserId,
        //        //UserAuthId = request.UserAuthId,
        //        SolnId = CoreConstants.EXPRESSBASE,
        //        //AttachmentReport = RepRes.ReportBytea,
        //        //AttachmentName = RepRes.ReportName
        //    });
        //    return new ResetPasswordMqResponse { };
        //}
    } 

    [Restrict(InternalOnly = true)]
    public class EmailInternalService : EbMqBaseService
    {
        public EmailInternalService() : base() { }

        public string Post(EmailServicesRequest request)
        {
            try
            {
                if (request.SolnId == CoreConstants.EXPRESSBASE)
                {
                    Console.WriteLine("Inside EmailService/EmailServiceInternal in SS \n Before Email \n To: " + request.To + "\nEmail Connections - infra: " + InfraConnectionFactory.EmailConnection.Capacity);
                    this.InfraConnectionFactory.EmailConnection.Send(request.To, request.Subject, request.Message, request.Cc, request.Bcc, request.AttachmentReport, request.AttachmentName);
                }
                else
                {
                    base.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                    Console.WriteLine("Inside EmailService/EmailServiceInternal in SS \n Before Email \n To: " + request.To + "\nEmail Connections - solution: " + EbConnectionFactory.EmailConnection.Capacity);
                    this.EbConnectionFactory.EmailConnection.Send(request.To, request.Subject, request.Message, request.Cc, request.Bcc, request.AttachmentReport, request.AttachmentName);
                }

                Console.WriteLine("Inside EmailService/EmailServiceInternal in SS \n After Email \nSend To:" + request.To);
            }
            catch (Exception e)
            {
                return e.Message;
            }
            return null;
        }
    }
}
