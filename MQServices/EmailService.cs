using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Threading.Tasks;
using ServiceStack.Messaging;

namespace ExpressBase.ServiceStack
{
    public class EmailService : EbBaseService
    {
        public EmailService(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public class EmailServiceInternal : EbBaseService
        {
            public EmailServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }
            public string Post(EmailServicesMqRequest request)
            {

                var emailMessage = new MimeMessage();

                emailMessage.From.Add(new MailboxAddress("EXPRESSbase", "info@expressbase.com"));
                emailMessage.To.Add(new MailboxAddress("", request.To));
                emailMessage.Subject = request.Subject;
                emailMessage.Body = new TextPart("plain") { Text = request.Message };
                try
                {
                    using (var client = new SmtpClient())
                    {
                        client.LocalDomain = "www.expressbase.com";
                        client.Connect("smtp.gmail.com", 465, true);
                        client.Authenticate(new System.Net.NetworkCredential() { UserName = "expressbasesystems@gmail.com", Password = "ebsystems" });
                        client.Send(emailMessage);
                        client.Disconnect(true);
                    }
                }
                catch (Exception e)
                {
                    return e.Message;
                }
                return null;
            }
        }
    }

   

}
