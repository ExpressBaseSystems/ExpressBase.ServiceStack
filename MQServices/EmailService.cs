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
            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    public class EmailInternalService : EbMqBaseService
    {
        public EmailInternalService() : base() { }

        public string Post(EmailServicesRequest request)
        {
            base.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {               
                this.EbConnectionFactory.Smtp.Send(request.To, request.Subject, request.Message, request.Cc, request.Bcc, request.AttachmentReport, request.AttachmentName);
            }
            catch (Exception e)
            {
                return e.Message;
            }
            return null;
        }
    }
}
